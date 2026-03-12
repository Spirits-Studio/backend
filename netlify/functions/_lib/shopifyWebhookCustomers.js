import {
  parseWebhookEnvelope,
  verifyWebhookHmac,
  hashWebhookPayload,
  beginWebhookIdempotency,
  completeWebhookIdempotency,
  createWebhookLogContext,
  createWebhookLogger,
  mapWebhookErrorMessage,
  sendWebhookJson,
} from "./shopifyWebhook.js";
import {
  normalizeCustomerWebhookPayload,
  upsertCanonicalCustomer,
  collectCustomerGuestCandidatesByEmail,
  mergeGuestCustomersIntoCanonical,
} from "./shopifyWebhookStudio.js";
import { normalizeRecordId } from "./studio.js";

const sumMergeRelinks = (mergeResult = {}) =>
  Number(mergeResult?.relinkedSavedConfigurations || 0) +
  Number(mergeResult?.relinkedLabels || 0) +
  Number(mergeResult?.relinkedOrders || 0);

export const createShopifyWebhookCustomersHandler = ({
  endpoint,
  expectedTopic,
}) =>
  async (req, res) => {
    const envelope = await parseWebhookEnvelope(req);
    const normalizedEvent = normalizeCustomerWebhookPayload(
      envelope.payload || {},
      envelope
    );
    const customer = normalizedEvent?.customer || {};

    const logger = createWebhookLogger(
      createWebhookLogContext({
        topic: envelope?.topic || expectedTopic || null,
        webhook_id: envelope?.webhook_id || null,
        shop_domain: envelope?.shop_domain || null,
        shopify_customer_id: customer?.shopify_id || null,
        email: customer?.email || null,
        status: "received",
      })
    );

    if (envelope.parseError) {
      logger.warn("invalid JSON payload", {
        status: "error",
        error: mapWebhookErrorMessage(envelope.parseError),
      });
      sendWebhookJson(res, 400, {
        ok: false,
        error: "invalid_json",
      });
      return;
    }

    if (!verifyWebhookHmac({ rawBody: envelope.rawBody, providedHmac: envelope.hmac })) {
      logger.warn("invalid hmac", {
        status: "error",
        error: "invalid_hmac",
      });
      res.status(401).end("invalid hmac");
      return;
    }

    const payloadHash = hashWebhookPayload(envelope.rawBody);
    const idempotency = await beginWebhookIdempotency({
      webhookId: envelope.webhook_id,
      topic: envelope.topic || expectedTopic || null,
      shopDomain: envelope.shop_domain || null,
      payloadHash,
      logger,
    });

    if (idempotency.blocked) {
      logger.error("idempotency storage unavailable", {
        status: "error",
        idempotent_skip: false,
        error: "idempotency_unavailable",
      });
      sendWebhookJson(res, 503, {
        ok: false,
        error: "idempotency_unavailable",
        message:
          "Webhook idempotency storage is unavailable. Refusing to process to avoid duplicate side effects.",
      });
      return;
    }

    if (idempotency.skip) {
      logger.info("duplicate webhook skipped", {
        status: "skipped",
        idempotent_skip: true,
      });
      await completeWebhookIdempotency({
        recordId: idempotency.recordId,
        status: "skipped",
        logger,
      });
      sendWebhookJson(res, 200, {
        ok: true,
        idempotent_skip: true,
        webhook_id: envelope.webhook_id,
      });
      return;
    }

    if (expectedTopic && envelope.topic && envelope.topic !== expectedTopic) {
      logger.info("topic mismatch skipped", {
        status: "skipped",
      });
      await completeWebhookIdempotency({
        recordId: idempotency.recordId,
        status: "skipped",
        logger,
      });
      sendWebhookJson(res, 202, {
        ok: true,
        skipped: true,
        reason: "topic_mismatch",
        expected_topic: expectedTopic,
        received_topic: envelope.topic,
      });
      return;
    }

    try {
      const canonicalCustomer = await upsertCanonicalCustomer({
        shopifyId: customer?.shopify_id,
        email: customer?.email,
        firstName: customer?.first_name,
        lastName: customer?.last_name,
        phone: customer?.phone,
        shopDomain: envelope?.shop_domain,
        creationSource: `${endpoint || "shopify-webhook-customers"} (${expectedTopic || envelope.topic || "customers"})`,
      });

      const canonicalCustomerRecordId = normalizeRecordId(
        canonicalCustomer?.customerRecordId
      );

      const guestCandidates = canonicalCustomerRecordId
        ? await collectCustomerGuestCandidatesByEmail({ email: customer?.email })
        : [];

      const mergeResult = canonicalCustomerRecordId
        ? await mergeGuestCustomersIntoCanonical({
            canonicalCustomerRecordId,
            guestCustomerRecordIds: guestCandidates,
          })
        : {
            mergedPairs: [],
            relinkedSavedConfigurations: 0,
            relinkedLabels: 0,
            relinkedOrders: 0,
          };

      logger.info("customer webhook processed", {
        canonical_customer_record_id: canonicalCustomerRecordId,
        merge_candidates_count: guestCandidates.length,
        relinked_saved_configs: mergeResult.relinkedSavedConfigurations,
        relinked_labels: mergeResult.relinkedLabels,
        relinked_orders: mergeResult.relinkedOrders,
        status: "processed",
      });

      await completeWebhookIdempotency({
        recordId: idempotency.recordId,
        status: "processed",
        logger,
      });

      sendWebhookJson(res, 200, {
        ok: true,
        topic: envelope.topic || expectedTopic || null,
        webhook_id: envelope.webhook_id || null,
        canonical_customer_record_id: canonicalCustomerRecordId,
        created_customer: Boolean(canonicalCustomer?.created),
        matched_by: canonicalCustomer?.matchedBy || null,
        merge_candidates_count: guestCandidates.length,
        merged_customer_pairs: mergeResult.mergedPairs,
        merged_records_updated: sumMergeRelinks(mergeResult),
        relinked_saved_configs: mergeResult.relinkedSavedConfigurations,
        relinked_labels: mergeResult.relinkedLabels,
        relinked_orders: mergeResult.relinkedOrders,
        idempotent_skip: false,
        status: "processed",
      });
    } catch (error) {
      const errorMessage = mapWebhookErrorMessage(error);

      logger.error("customer webhook processing failed", {
        status: "error",
        error: errorMessage,
      });

      await completeWebhookIdempotency({
        recordId: idempotency.recordId,
        status: "error",
        error,
        logger,
      });

      sendWebhookJson(res, error?.status || 500, {
        ok: false,
        error: "server_error",
        message: error?.message || String(error),
      });
    }
  };
