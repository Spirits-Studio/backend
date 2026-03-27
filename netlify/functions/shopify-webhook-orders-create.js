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
} from "./_lib/shopifyWebhook.js";
import {
  normalizeOrderWebhookPayload,
  upsertCanonicalCustomer,
  collectOrderSignals,
  ensureSavedConfigurationOrderLinkage,
  createOrderRecordForSavedConfiguration,
  buildShopifyOrderDetailsItem,
  buildShopifyOrderDetailsMetafieldPayload,
  upsertBillingAddressForOrder,
} from "./_lib/shopifyWebhookStudio.js";
import {
  setShopifyOrderComplianceMetafield,
  setShopifyOrderDetailsMetafield,
} from "./_lib/shopifyAdmin.js";
import {
  buildOrderComplianceMetafieldPayload,
  parseComplianceNoteAttributes,
} from "./_lib/compliance.js";
import {
  STUDIO_FIELDS,
  STUDIO_TABLES,
  createResilient,
  getLinkedIds,
  getRecordOrNull,
  listAllRecords,
  normalizeRecordId,
  resolveCustomerCreationSource,
  updateResilient,
} from "./_lib/studio.js";

const toOrderIdString = (orderId) => {
  if (orderId == null) return null;
  const text = String(orderId).trim();
  return text || null;
};

const escapeFormulaValue = (value) =>
  String(value || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");

const upsertComplianceOrderRecords = async ({
  orderId,
  orderStatus,
  customerRecordId = null,
  forResale,
}) => {
  const normalizedOrderId = toOrderIdString(orderId);
  if (!normalizedOrderId || typeof forResale !== "boolean") return [];

  const existingOrderRecords = await listAllRecords(STUDIO_TABLES.orders, {
    filterByFormula: `{${STUDIO_FIELDS.orders.orderId}}='${escapeFormulaValue(
      normalizedOrderId
    )}'`,
    maxRecords: 25,
  });

  const fields = {
    [STUDIO_FIELDS.orders.orderId]: normalizedOrderId,
    [STUDIO_FIELDS.orders.orderStatus]: orderStatus || "Ordered",
    [STUDIO_FIELDS.orders.forResale]: forResale,
    ...(customerRecordId
      ? {
          [STUDIO_FIELDS.orders.customer]: [customerRecordId],
        }
      : {}),
  };

  if (Array.isArray(existingOrderRecords) && existingOrderRecords.length > 0) {
    const updatedIds = [];
    for (const record of existingOrderRecords) {
      const updated = await updateResilient(STUDIO_TABLES.orders, record.id, {}, fields);
      if (updated?.id) updatedIds.push(updated.id);
    }
    return updatedIds;
  }

  const created = await createResilient(STUDIO_TABLES.orders, {}, fields);
  return created?.id ? [created.id] : [];
};

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
      const complianceState = parseComplianceNoteAttributes(envelope.payload || {});
      const signals = await collectOrderSignals(envelope.payload || {});
      const hasAuthenticatedShopifyCustomer = Boolean(order?.customer?.shopify_id);
      const preferredCustomerRecordIds = Array.isArray(signals?.customerRecordIds)
        ? signals.customerRecordIds
        : [];
      const complianceCustomerRecordId = normalizeRecordId(
        complianceState.customer_airtable_id
      );
      if (
        complianceCustomerRecordId &&
        !hasAuthenticatedShopifyCustomer &&
        !preferredCustomerRecordIds.includes(complianceCustomerRecordId)
      ) {
        preferredCustomerRecordIds.unshift(complianceCustomerRecordId);
      }

      const canonicalCustomer = await upsertCanonicalCustomer({
        shopifyId: order?.customer?.shopify_id,
        email: order?.customer?.email || order?.email,
        firstName: order?.customer?.first_name,
        lastName: order?.customer?.last_name,
        phone: order?.customer?.phone,
        shopDomain: envelope?.shop_domain,
        creationSource: resolveCustomerCreationSource(endpoint),
        preferredCustomerRecordIds,
      });
      const canonicalCustomerRecordId = normalizeRecordId(
        canonicalCustomer?.customerRecordId
      );

      const savedConfigurationSignals = Array.isArray(signals?.savedConfigurationSignals)
        ? signals.savedConfigurationSignals
        : [];

      const touchedConfigIds = [];
      const createdOrderRecordIds = [];
      const linkedAddressRecordIds = [];
      const shopifyOrderDetailsItems = [];

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
          savedConfigurationRecord: savedConfigRecord,
          customerRecordIds: linkedCustomerIds,
          quantity: signal?.quantity,
          lineItemMetadata: signal,
        });

        const billingAddressRecord = await upsertBillingAddressForOrder({
          order,
          customerRecordIds: linkedCustomerIds,
          orderRecordId: orderRecord?.id || null,
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
        if (billingAddressRecord?.id) linkedAddressRecordIds.push(billingAddressRecord.id);

        const shopifyOrderDetailsItem = buildShopifyOrderDetailsItem({
          order,
          savedConfigurationRecordId,
          savedConfigurationRecord: savedConfigRecord,
          orderRecordId: orderRecord?.id || null,
          customerRecordId: linkedCustomerIds[0] || canonicalCustomerRecordId,
          signal,
        });
        if (shopifyOrderDetailsItem) {
          shopifyOrderDetailsItems.push(shopifyOrderDetailsItem);
        }
      }

      const resolvedComplianceCustomerRecordId = hasAuthenticatedShopifyCustomer
        ? canonicalCustomerRecordId || null
        : complianceCustomerRecordId || canonicalCustomerRecordId || null;
      const complianceOrderRecordIds =
        complianceState.profile?.for_resale == null
          ? []
          : await upsertComplianceOrderRecords({
              orderId: toOrderIdString(order?.id),
              orderStatus,
              customerRecordId: resolvedComplianceCustomerRecordId,
              forResale: complianceState.profile.for_resale,
            });

      const shopifyOrderMetafieldPayload = buildShopifyOrderDetailsMetafieldPayload({
        order,
        orderStatus,
        items: shopifyOrderDetailsItems,
      });
      const shopifyOrderMetafieldSync =
        shopifyOrderDetailsItems.length > 0
          ? await setShopifyOrderDetailsMetafield({
              shopDomain: envelope?.shop_domain,
              orderId: toOrderIdString(order?.id),
              payload: shopifyOrderMetafieldPayload,
            })
          : {
              ok: false,
              skipped: true,
              reason: "no_supported_order_items",
            };

      const shopifyOrderComplianceMetafieldSync =
        complianceState.profile?.for_resale == null
          ? {
              ok: false,
              skipped: true,
              reason: "no_compliance_attributes",
            }
          : await setShopifyOrderComplianceMetafield({
              shopDomain: envelope?.shop_domain,
              orderId: toOrderIdString(order?.id),
              payload: buildOrderComplianceMetafieldPayload({
                order,
                customerRecordId: resolvedComplianceCustomerRecordId,
                sessionId: complianceState.session_id,
                profile: complianceState.profile,
              }),
            });

      const responsePayload = {
        ok: true,
        topic: envelope.topic || expectedTopic || null,
        webhook_id: envelope.webhook_id || null,
        order_id: toOrderIdString(order?.id),
        canonical_customer_record_id: canonicalCustomerRecordId,
        updated_saved_configurations: Array.from(new Set(touchedConfigIds)),
        created_order_records: Array.from(new Set(createdOrderRecordIds)),
        compliance_order_records: Array.from(new Set(complianceOrderRecordIds)),
        linked_address_records: Array.from(new Set(linkedAddressRecordIds)),
        shopify_order_metafield_sync: shopifyOrderMetafieldSync,
        shopify_order_compliance_metafield_sync: shopifyOrderComplianceMetafieldSync,
        idempotent_skip: false,
        status: "processed",
      };

      logger.info("webhook processed", {
        canonical_customer_record_id: canonicalCustomerRecordId,
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
  orderStatus: "Ordered",
});

export default shopifyWebhookOrdersCreate;
