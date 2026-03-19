import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeRecordId,
  listRecordsByLinkedRecordIds,
  mapErrorResponse,
} from "./_lib/studio.js";

export default withShopifyProxy(
  async (arg, { qs, isV2, method }) => {
    try {
      const body = (await parseBody(arg, method, isV2)) || {};
      const customerRecordId = normalizeRecordId(
        firstNonEmpty(
          qs.customer_record_id,
          qs.customer_id,
          body.customer_record_id,
          body.customer_id,
          body.ss_customer_airtable_id
        )
      );
      if (!customerRecordId) {
        return sendJson(200, { ok: true, has_content: false });
      }

      const [savedConfigs, labels] = await Promise.all([
        listRecordsByLinkedRecordIds(STUDIO_TABLES.savedConfigurations, {
          fieldName: STUDIO_FIELDS.savedConfigurations.customer,
          linkedRecordIds: customerRecordId,
          maxMatches: 1,
        }),
        listRecordsByLinkedRecordIds(STUDIO_TABLES.labels, {
          fieldName: STUDIO_FIELDS.labels.customers,
          linkedRecordIds: customerRecordId,
          maxMatches: 1,
        }),
      ]);

      const hasContent = (savedConfigs?.length || 0) > 0 || (labels?.length || 0) > 0;
      return sendJson(200, {
        ok: true,
        has_content: hasContent,
        customer_record_id: customerRecordId,
      });
    } catch (error) {
      return sendJson(error?.status || 500, mapErrorResponse(error));
    }
  },
  {
    methods: ["GET"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
