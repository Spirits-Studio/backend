import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeRecordId,
  sanitizeName,
  getRecordOrNull,
  getLinkedIds,
  updateResilient,
  mapErrorResponse,
} from "./_lib/studio.js";

export default withShopifyProxy(
  async (arg, { qs, isV2, method }) => {
    try {
      const body = (await parseBody(arg, method, isV2)) || {};

      const customerRecordId = normalizeRecordId(
        firstNonEmpty(
          body.customer_record_id,
          body.customer_id,
          body.ss_customer_airtable_id,
          qs.customer_record_id,
          qs.customer_id
        )
      );
      const labelRecordId = normalizeRecordId(
        firstNonEmpty(body.label_record_id, body.labelRecordId, qs.label_record_id)
      );
      const displayName = sanitizeName(
        firstNonEmpty(body.display_name, body.displayName, body.name)
      );

      if (!customerRecordId || !labelRecordId || !displayName) {
        return sendJson(400, {
          ok: false,
          error: "missing_required_fields",
          message: "customer_record_id, label_record_id and display_name are required.",
        });
      }

      const label = await getRecordOrNull(STUDIO_TABLES.labels, labelRecordId);
      if (!label) {
        return sendJson(404, {
          ok: false,
          error: "label_not_found",
          message: "Label not found.",
        });
      }

      const linkedCustomers = getLinkedIds(label, STUDIO_FIELDS.labels.customers);
      if (!linkedCustomers.includes(customerRecordId)) {
        return sendJson(403, {
          ok: false,
          error: "forbidden",
          message: "This label does not belong to the current customer.",
        });
      }

      const updated = await updateResilient(
        STUDIO_TABLES.labels,
        labelRecordId,
        {},
        { [STUDIO_FIELDS.labels.displayName]: displayName }
      );

      return sendJson(200, {
        ok: true,
        label_record_id: updated.id,
        display_name: updated.fields?.[STUDIO_FIELDS.labels.displayName] || displayName,
      });
    } catch (error) {
      return sendJson(error?.status || 500, mapErrorResponse(error));
    }
  },
  {
    methods: ["PATCH"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
