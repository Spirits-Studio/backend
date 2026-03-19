import {
  parseWebhookEnvelope,
  verifyWebhookHmac,
  hashWebhookPayload,
  beginWebhookIdempotency,
  completeWebhookIdempotency,
  createWebhookLogContext,
  createWebhookLogger,
  mapWebhookErrorMessage,
  shouldLogWebhookVerificationDebug,
  createWebhookVerificationDebugInfo,
  createWebhookPayloadDebugInfo,
  sendWebhookJson,
  sendWebhookText,
} from "./shopifyWebhook.js";
import {
  normalizeCustomerWebhookPayload,
  upsertCanonicalCustomer,
} from "./shopifyWebhookStudio.js";
import { normalizeRecordId } from "./studio.js";

export const createShopifyWebhookCustomersHandler = ({
  endpoint,
  expectedTopic,
}) =>
  async (req, res) => {
    const sendJson = (status, payload) => sendWebhookJson(res, status, payload, req);
    const sendText = (status, text) => sendWebhookText(res, status, text, req);
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
        ...(shouldLogWebhookVerificationDebug()
          ? createWebhookPayloadDebugInfo({ envelope })
          : {}),
      });
      return sendJson(400, {
        ok: false,
        error: "invalid_json",
      });
    }

    if (!verifyWebhookHmac({ rawBody: envelope.rawBody, providedHmac: envelope.hmac })) {
      logger.warn("invalid hmac", {
        status: "error",
        error: "invalid_hmac",
        ...(shouldLogWebhookVerificationDebug()
          ? createWebhookVerificationDebugInfo({
              rawBody: envelope.rawBody,
              headers: envelope.headers,
              providedHmac: envelope.hmac,
            })
          : {}),
      });
      return sendText(401, "invalid hmac");
    }

    if (shouldLogWebhookVerificationDebug()) {
      logger.info("webhook request payload", createWebhookPayloadDebugInfo({ envelope }));
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
      return sendJson(503, {
        ok: false,
        error: "idempotency_unavailable",
        message:
          "Webhook idempotency storage is unavailable. Refusing to process to avoid duplicate side effects.",
      });
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
      return sendJson(200, {
        ok: true,
        idempotent_skip: true,
        webhook_id: envelope.webhook_id,
      });
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
      return sendJson(202, {
        ok: true,
        skipped: true,
        reason: "topic_mismatch",
        expected_topic: expectedTopic,
        received_topic: envelope.topic,
      });
    }

    try {
      const canonicalCustomer = await upsertCanonicalCustomer({
        shopifyId: customer?.shopify_id,
        email: customer?.email,
        firstName: customer?.first_name,
        lastName: customer?.last_name,
        phone: customer?.phone,
        shopDomain: envelope?.shop_domain,
      });

      const canonicalCustomerRecordId = normalizeRecordId(
        canonicalCustomer?.customerRecordId
      );

      logger.info("customer webhook processed", {
        canonical_customer_record_id: canonicalCustomerRecordId,
        status: "processed",
      });

      await completeWebhookIdempotency({
        recordId: idempotency.recordId,
        status: "processed",
        logger,
      });

      return sendJson(200, {
        ok: true,
        topic: envelope.topic || expectedTopic || null,
        webhook_id: envelope.webhook_id || null,
        canonical_customer_record_id: canonicalCustomerRecordId,
        created_customer: Boolean(canonicalCustomer?.created),
        matched_by: canonicalCustomer?.matchedBy || null,
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

      return sendJson(error?.status || 500, {
        ok: false,
        error: "server_error",
        message: error?.message || String(error),
      });
    }
  };
