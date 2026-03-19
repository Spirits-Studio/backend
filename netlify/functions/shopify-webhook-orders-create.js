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
  sendWebhookJson,
  sendWebhookText,
} from "./_lib/shopifyWebhook.js";
import {
  normalizeOrderWebhookPayload,
  upsertCanonicalCustomer,
  collectOrderSignals,
  mergeGuestCustomersIntoCanonical,
  ensureSavedConfigurationOrderLinkage,
  createOrderRecordForSavedConfiguration,
} from "./_lib/shopifyWebhookStudio.js";
import {
  STUDIO_FIELDS,
  STUDIO_TABLES,
  getLinkedIds,
  getRecordOrNull,
  normalizeRecordId,
} from "./_lib/studio.js";

const toOrderIdString = (orderId) => {
  if (orderId == null) return null;
  const text = String(orderId).trim();
  return text || null;
};

const sumMergeRelinks = (mergeResult = {}) =>
  Number(mergeResult?.relinkedSavedConfigurations || 0) +
  Number(mergeResult?.relinkedLabels || 0) +
  Number(mergeResult?.relinkedOrders || 0);

export const createShopifyWebhookOrdersHandler = ({
  endpoint,
  expectedTopic,
  orderStatus,
}) =>
  async (req, res) => {
    const sendJson = (status, payload) => sendWebhookJson(res, status, payload, req);
    const sendText = (status, text) => sendWebhookText(res, status, text, req);
    const envelope = await parseWebhookEnvelope(req);
    const normalizedEvent = normalizeOrderWebhookPayload(envelope.payload || {}, envelope);
    const order = normalizedEvent?.order || {};

    const logger = createWebhookLogger(
      createWebhookLogContext({
        topic: envelope?.topic || expectedTopic || null,
        webhook_id: envelope?.webhook_id || null,
        shop_domain: envelope?.shop_domain || null,
        order_id: toOrderIdString(order?.id),
        shopify_customer_id: order?.customer?.shopify_id || null,
        email: order?.customer?.email || order?.email || null,
        status: "received",
      })
    );

    if (envelope.parseError) {
      logger.warn("invalid JSON payload", {
        status: "error",
        error: mapWebhookErrorMessage(envelope.parseError),
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
        idempotent_skip: false,
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
        shopifyId: order?.customer?.shopify_id,
        email: order?.customer?.email || order?.email,
        firstName: order?.customer?.first_name,
        lastName: order?.customer?.last_name,
        phone: order?.customer?.phone,
        shopDomain: envelope?.shop_domain,
        creationSource: `${endpoint || "shopify-webhook-orders"} (${expectedTopic || envelope.topic || "orders"})`,
      });
      const canonicalCustomerRecordId = normalizeRecordId(
        canonicalCustomer?.customerRecordId
      );

      const signals = await collectOrderSignals(envelope.payload || {});
      const mergeCandidates = Array.isArray(signals?.candidateCustomerRecordIds)
        ? signals.candidateCustomerRecordIds
        : [];
      const mergeResult = canonicalCustomerRecordId
        ? await mergeGuestCustomersIntoCanonical({
            canonicalCustomerRecordId,
            guestCustomerRecordIds: mergeCandidates,
          })
        : {
            mergedPairs: [],
            relinkedSavedConfigurations: 0,
            relinkedLabels: 0,
            relinkedOrders: 0,
          };

      const savedConfigurationSignals = Array.isArray(signals?.savedConfigurationSignals)
        ? signals.savedConfigurationSignals
        : [];

      const touchedConfigIds = [];
      const createdOrderRecordIds = [];

      for (const signal of savedConfigurationSignals) {
        const savedConfigurationRecordId = normalizeRecordId(
          signal?.saved_configuration_id
        );
        if (!savedConfigurationRecordId) continue;

        const savedConfigRecord = await getRecordOrNull(
          STUDIO_TABLES.savedConfigurations,
          savedConfigurationRecordId
        );
        if (!savedConfigRecord) continue;

        const linkedCustomerIds = canonicalCustomerRecordId
          ? [canonicalCustomerRecordId]
          : getLinkedIds(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.customer);

        const orderRecord = await createOrderRecordForSavedConfiguration({
          orderId: toOrderIdString(order?.id),
          orderStatus,
          savedConfigurationRecordId,
          customerRecordIds: linkedCustomerIds,
        });

        const preferredFrontVersionId = normalizeRecordId(
          signal?.front_version_ids?.[0]
        );
        const preferredBackVersionId = normalizeRecordId(
          signal?.back_version_ids?.[0]
        );

        await ensureSavedConfigurationOrderLinkage({
          savedConfigurationRecordId,
          canonicalCustomerRecordId,
          orderRecordId: orderRecord?.id || null,
          orderStatus,
          preferredFrontVersionId,
          preferredBackVersionId,
        });

        touchedConfigIds.push(savedConfigurationRecordId);
        if (orderRecord?.id) createdOrderRecordIds.push(orderRecord.id);
      }

      const responsePayload = {
        ok: true,
        topic: envelope.topic || expectedTopic || null,
        webhook_id: envelope.webhook_id || null,
        order_id: toOrderIdString(order?.id),
        canonical_customer_record_id: canonicalCustomerRecordId,
        merge_candidates_count: mergeCandidates.length,
        merged_customer_pairs: mergeResult.mergedPairs,
        merged_records_updated: sumMergeRelinks(mergeResult),
        relinked_saved_configs: mergeResult.relinkedSavedConfigurations,
        relinked_labels: mergeResult.relinkedLabels,
        relinked_orders: mergeResult.relinkedOrders,
        updated_saved_configurations: Array.from(new Set(touchedConfigIds)),
        created_order_records: Array.from(new Set(createdOrderRecordIds)),
        idempotent_skip: false,
        status: "processed",
      };

      logger.info("webhook processed", {
        canonical_customer_record_id: canonicalCustomerRecordId,
        merge_candidates_count: mergeCandidates.length,
        relinked_saved_configs: mergeResult.relinkedSavedConfigurations,
        relinked_labels: mergeResult.relinkedLabels,
        relinked_orders: mergeResult.relinkedOrders,
        idempotent_skip: false,
        status: "processed",
      });

      await completeWebhookIdempotency({
        recordId: idempotency.recordId,
        status: "processed",
        logger,
      });

      return sendJson(200, responsePayload);
    } catch (error) {
      const errorMessage = mapWebhookErrorMessage(error);

      logger.error("webhook processing failed", {
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

const shopifyWebhookOrdersCreate = createShopifyWebhookOrdersHandler({
  endpoint: "shopify-webhook-orders-create",
  expectedTopic: "orders/create",
  orderStatus: "Order Received",
});

export default shopifyWebhookOrdersCreate;
