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
      const savedConfigurationRecordId = normalizeRecordId(
        firstNonEmpty(
          body.saved_configuration_record_id,
          body.savedConfigurationRecordId,
          body.saved_configuration_id,
          qs.saved_configuration_record_id
        )
      );
      const displayName = sanitizeName(
        firstNonEmpty(body.display_name, body.displayName, body.name)
      );

      if (!customerRecordId || !savedConfigurationRecordId || !displayName) {
        return sendJson(400, {
          ok: false,
          error: "missing_required_fields",
          message:
            "customer_record_id, saved_configuration_record_id and display_name are required.",
        });
      }

      const record = await getRecordOrNull(
        STUDIO_TABLES.savedConfigurations,
        savedConfigurationRecordId
      );
      if (!record) {
        return sendJson(404, {
          ok: false,
          error: "saved_configuration_not_found",
          message: "Saved configuration not found.",
        });
      }

      const linkedCustomers = getLinkedIds(
        record,
        STUDIO_FIELDS.savedConfigurations.customer
      );
      if (!linkedCustomers.includes(customerRecordId)) {
        return sendJson(403, {
          ok: false,
          error: "forbidden",
          message: "This saved configuration does not belong to the current customer.",
        });
      }

      const updated = await updateResilient(
        STUDIO_TABLES.savedConfigurations,
        savedConfigurationRecordId,
        {},
        { [STUDIO_FIELDS.savedConfigurations.displayName]: displayName }
      );

      return sendJson(200, {
        ok: true,
        saved_configuration_record_id: updated.id,
        display_name:
          updated.fields?.[STUDIO_FIELDS.savedConfigurations.displayName] || displayName,
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
