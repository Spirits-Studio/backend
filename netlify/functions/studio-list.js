import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  STUDIO_FIELD_FALLBACKS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeRecordId,
  getRecordOrNull,
  getFieldValue,
  getLinkedIds,
  listRecordsByLinkedRecordIds,
  sanitizeText,
  mapErrorResponse,
} from "./_lib/studio.js";

const toText = (value) => (value == null ? "" : String(value).trim());

const parseJsonField = (value) => {
  if (!value || typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

const resolveSessionId = (...values) => {
  for (const value of values) {
    const sessionId = sanitizeText(value, 255);
    if (sessionId) return sessionId;
  }
  return null;
};

const matchesTextFilter = (value, expected) => {
  if (!expected) return true;
  return toText(value).toLowerCase() === expected;
};

const summarizeIds = (values = [], limit = 10) => {
  const ids = Array.isArray(values) ? values.filter(Boolean) : [];
  return {
    count: ids.length,
    ids: ids.slice(0, limit),
    truncated: ids.length > limit,
  };
};

const summarizeCustomerRecord = (record) => {
  const fields = record?.fields || {};
  return {
    found: Boolean(record?.id),
    id: record?.id || null,
    shopify_id: fields["Shopify ID"] || null,
    source: fields.Source || null,
    email_present: Boolean(fields.Email),
    phone_present: Boolean(fields.Phone),
  };
};

const summarizeLabelRecord = (record) => ({
  id: record?.id || null,
  session_id: resolveSessionId(record?.fields?.[STUDIO_FIELDS.labels.sessionId]),
  customer_ids: getLinkedIds(record, STUDIO_FIELDS.labels.customers),
  direct_version_ids: getLinkedIds(record, STUDIO_FIELDS.labels.labelVersions),
  current_front_version_id:
    getLinkedIds(
      record,
      STUDIO_FIELDS.labels.currentFrontLabelVersion,
      STUDIO_FIELD_FALLBACKS.labels.currentFrontLabelVersion
    )[0] || null,
  current_back_version_id:
    getLinkedIds(
      record,
      STUDIO_FIELDS.labels.currentBackLabelVersion,
      STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion
    )[0] || null,
});

const summarizeVersionRecord = (record) => ({
  id: record?.id || null,
  label_ids: getLinkedIds(
    record,
    STUDIO_FIELDS.labelVersions.labels,
    LABEL_VERSION_LABEL_FIELD_FALLBACKS
  ),
  design_side: record?.fields?.[STUDIO_FIELDS.labelVersions.designSide] || null,
  version_kind: record?.fields?.[STUDIO_FIELDS.labelVersions.versionKind] || null,
  version_number: record?.fields?.[STUDIO_FIELDS.labelVersions.versionNumber] || null,
  session_id: resolveSessionId(record?.fields?.[STUDIO_FIELDS.labelVersions.sessionId]),
});

const PREVIOUS_LABEL_VERSION_FIELD_FALLBACKS = ["Previous Label Versions"];
const LABEL_VERSION_LABEL_FIELD_FALLBACKS = [
  "Labels: Current Front Label Versions",
  "Labels: Current Back Label Versions",
];

export default withShopifyProxy(
  async (arg, { qs, isV2, method }) => {
    let customerRecordId = null;
    try {
      const body = (await parseBody(arg, method, isV2)) || {};
      const rawCustomerRecordId = firstNonEmpty(
        qs.customer_record_id,
        qs.customer_id,
        body.customer_record_id,
        body.customer_id,
        body.ss_customer_airtable_id
      );
      customerRecordId = normalizeRecordId(
        firstNonEmpty(
          qs.customer_record_id,
          qs.customer_id,
          body.customer_record_id,
          body.customer_id,
          body.ss_customer_airtable_id
        )
      );
      const bottleFilter = toText(firstNonEmpty(qs.bottle, body.bottle)).toLowerCase();
      const statusFilter = toText(firstNonEmpty(qs.status, body.status)).toLowerCase();

      console.info("[studio-list] request", {
        method,
        is_v2: Boolean(isV2),
        query_keys: Object.keys(qs || {}).sort(),
        body_keys: Object.keys(body || {}).sort(),
        raw_customer_record_id: rawCustomerRecordId || null,
        normalized_customer_record_id: customerRecordId,
        raw_customer_id: firstNonEmpty(qs.customer_id, body.customer_id) || null,
        raw_ss_customer_airtable_id: body.ss_customer_airtable_id || null,
        bottle_filter: bottleFilter || null,
        status_filter: statusFilter || null,
      });

      if (!customerRecordId) {
        console.warn("[studio-list] missing_customer_record_id", {
          raw_customer_record_id: rawCustomerRecordId || null,
          raw_customer_id: firstNonEmpty(qs.customer_id, body.customer_id) || null,
          raw_ss_customer_airtable_id: body.ss_customer_airtable_id || null,
        });
        return sendJson(400, {
          ok: false,
          error: "missing_customer_record_id",
          message: "A valid Airtable customer record id is required.",
        });
      }

      const customerRecord = await getRecordOrNull(
        STUDIO_TABLES.customers,
        customerRecordId
      );

      console.info("[studio-list] customer-status", {
        customer_record_id: customerRecordId,
        customer: summarizeCustomerRecord(customerRecord),
      });

      if (!customerRecord?.id) {
        console.warn("[studio-list] customer-record-missing", {
          customer_record_id: customerRecordId,
        });
      }

      const rawSavedConfigRecords = await listRecordsByLinkedRecordIds(
        STUDIO_TABLES.savedConfigurations,
        {
          fieldName: STUDIO_FIELDS.savedConfigurations.customer,
          linkedRecordIds: customerRecordId,
        }
      );
      const savedConfigRecords = rawSavedConfigRecords.filter((record) => {
        const fields = record?.fields || {};
        return (
          matchesTextFilter(
            fields[STUDIO_FIELDS.savedConfigurations.bottleSelection],
            bottleFilter
          ) &&
          matchesTextFilter(fields[STUDIO_FIELDS.savedConfigurations.status], statusFilter)
        );
      });

      const labelRecords = await listRecordsByLinkedRecordIds(STUDIO_TABLES.labels, {
        fieldName: STUDIO_FIELDS.labels.customers,
        linkedRecordIds: customerRecordId,
      });

      const labelRecordIds = labelRecords.map((record) => record.id);
      const versionRecords = await listRecordsByLinkedRecordIds(
        STUDIO_TABLES.labelVersions,
        {
          fieldName: STUDIO_FIELDS.labelVersions.labels,
          linkedRecordIds: labelRecordIds,
          fallbackFieldNames: LABEL_VERSION_LABEL_FIELD_FALLBACKS,
        }
      );

      const versionsById = new Map(versionRecords.map((record) => [record.id, record]));
      const explicitVersionIds = Array.from(
        new Set(
          labelRecords.flatMap((record) => [
            ...getLinkedIds(record, STUDIO_FIELDS.labels.labelVersions),
            ...getLinkedIds(
              record,
              STUDIO_FIELDS.labels.currentFrontLabelVersion,
              STUDIO_FIELD_FALLBACKS.labels.currentFrontLabelVersion
            ),
            ...getLinkedIds(
              record,
              STUDIO_FIELDS.labels.currentBackLabelVersion,
              STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion
            ),
          ])
        )
      ).filter((id) => !versionsById.has(id));
      if (explicitVersionIds.length) {
        const directVersionRecords = await Promise.all(
          explicitVersionIds.map((id) => getRecordOrNull(STUDIO_TABLES.labelVersions, id))
        );
        directVersionRecords.filter(Boolean).forEach((record) => {
          if (versionsById.has(record.id)) return;
          versionsById.set(record.id, record);
          versionRecords.push(record);
        });
      }

      console.info("[studio-list] linked-record-status", {
        customer_record_id: customerRecordId,
        saved_configurations_raw: summarizeIds(
          rawSavedConfigRecords.map((record) => record.id)
        ),
        saved_configurations_filtered: summarizeIds(
          savedConfigRecords.map((record) => record.id)
        ),
        labels: summarizeIds(labelRecords.map((record) => record.id)),
        label_versions: summarizeIds(versionRecords.map((record) => record.id)),
        explicit_version_ids: summarizeIds(explicitVersionIds),
        label_summaries: labelRecords.map(summarizeLabelRecord),
        version_summaries: versionRecords.map(summarizeVersionRecord),
      });

      const versionsByLabelId = new Map();
      versionRecords.forEach((record) => {
        const linkedLabels = getLinkedIds(
          record,
          STUDIO_FIELDS.labelVersions.labels,
          LABEL_VERSION_LABEL_FIELD_FALLBACKS
        );
        linkedLabels.forEach((labelId) => {
          const current = versionsByLabelId.get(labelId) || [];
          current.push(record);
          versionsByLabelId.set(labelId, current);
        });
      });
      const labelSessionById = new Map(
        labelRecords.map((record) => [
          record.id,
          resolveSessionId(record.fields?.[STUDIO_FIELDS.labels.sessionId]),
        ])
      );

      const savedConfigurations = savedConfigRecords
        .map((record) => {
          const fields = record.fields || {};
          const labelIds = getLinkedIds(record, STUDIO_FIELDS.savedConfigurations.labels);
          return {
            id: record.id,
            created_at: record.createdTime || null,
            configuration_id: fields[STUDIO_FIELDS.savedConfigurations.configurationId] || null,
            display_name: fields[STUDIO_FIELDS.savedConfigurations.displayName] || null,
            bottle_selection: fields[STUDIO_FIELDS.savedConfigurations.bottleSelection] || null,
            liquid_selection: fields[STUDIO_FIELDS.savedConfigurations.liquidSelection] || null,
            closure_selection: fields[STUDIO_FIELDS.savedConfigurations.closureSelection] || null,
            wax_selection: fields[STUDIO_FIELDS.savedConfigurations.waxSelection] || null,
            alcohol_selection: fields[STUDIO_FIELDS.savedConfigurations.alcoholSelection] || null,
            preview_url: fields[STUDIO_FIELDS.savedConfigurations.previewImageUrl] || null,
            internal_sku: fields[STUDIO_FIELDS.savedConfigurations.internalSku] || null,
            shopify_variant_id:
              fields[STUDIO_FIELDS.savedConfigurations.shopifyVariantId] || null,
            status: fields[STUDIO_FIELDS.savedConfigurations.status] || null,
            session_id: resolveSessionId(
              fields[STUDIO_FIELDS.savedConfigurations.sessionId],
              ...labelIds.map((labelId) => labelSessionById.get(labelId))
            ),
            label_ids: labelIds,
            label_front_version_id:
              getLinkedIds(
                record,
                STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion,
                STUDIO_FIELD_FALLBACKS.savedConfigurations.currentFrontLabelVersion
              )[0] || null,
            label_back_version_id:
              getLinkedIds(
                record,
                STUDIO_FIELDS.savedConfigurations.currentBackLabelVersion,
                STUDIO_FIELD_FALLBACKS.savedConfigurations.currentBackLabelVersion
              )[0] || null,
            configuration: parseJsonField(
              fields[STUDIO_FIELDS.savedConfigurations.configJson]
            ),
          };
        })
        .sort((a, b) => (a.created_at < b.created_at ? 1 : -1));

      const labels = labelRecords
        .map((record) => {
          const fields = record.fields || {};
          const linkedIds = Array.from(
            new Set([
              ...getLinkedIds(record, STUDIO_FIELDS.labels.labelVersions),
              ...getLinkedIds(
                record,
                STUDIO_FIELDS.labels.currentFrontLabelVersion,
                STUDIO_FIELD_FALLBACKS.labels.currentFrontLabelVersion
              ),
              ...getLinkedIds(
                record,
                STUDIO_FIELDS.labels.currentBackLabelVersion,
                STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion
              ),
            ])
          );
          const linkedVersionRecords = linkedIds
            .map((id) => versionsById.get(id))
            .filter(Boolean);
          const fallbackVersionRecords = versionsByLabelId.get(record.id) || [];

          const uniqueVersions = Array.from(
            new Map(
              [...linkedVersionRecords, ...fallbackVersionRecords].map((row) => [row.id, row])
            ).values()
          ).sort((a, b) => {
            const av = Number(a.fields?.[STUDIO_FIELDS.labelVersions.versionNumber] || 0);
            const bv = Number(b.fields?.[STUDIO_FIELDS.labelVersions.versionNumber] || 0);
            if (av !== bv) return av - bv;
            return String(a.createdTime || "").localeCompare(String(b.createdTime || ""));
          });

          return {
            id: record.id,
            created_at: record.createdTime || null,
            display_name: fields[STUDIO_FIELDS.labels.displayName] || null,
            session_id: resolveSessionId(fields[STUDIO_FIELDS.labels.sessionId]),
            saved_configuration_ids: getLinkedIds(
              record,
              STUDIO_FIELDS.labels.savedConfigurations
            ),
            current_front_version_id:
              getLinkedIds(
                record,
                STUDIO_FIELDS.labels.currentFrontLabelVersion,
                STUDIO_FIELD_FALLBACKS.labels.currentFrontLabelVersion
              )[0] || null,
            current_back_version_id:
              getLinkedIds(
                record,
                STUDIO_FIELDS.labels.currentBackLabelVersion,
                STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion
              )[0] || null,
            versions: uniqueVersions.map((versionRecord) => {
              const versionFields = versionRecord.fields || {};
              const linkedVersionLabelIds = getLinkedIds(
                versionRecord,
                STUDIO_FIELDS.labelVersions.labels,
                LABEL_VERSION_LABEL_FIELD_FALLBACKS
              );
              const sessionSourceLabelIds = linkedVersionLabelIds.length
                ? linkedVersionLabelIds
                : [record.id];
              return {
                id: versionRecord.id,
                created_at:
                  versionRecord.createdTime ||
                  getFieldValue(
                    versionRecord,
                    STUDIO_FIELDS.labelVersions.createdAt,
                    STUDIO_FIELD_FALLBACKS.labelVersions.createdAt
                  ) ||
                  null,
                name: versionFields[STUDIO_FIELDS.labelVersions.name] || null,
                accepted: Boolean(versionFields[STUDIO_FIELDS.labelVersions.accepted]),
                design_side: versionFields[STUDIO_FIELDS.labelVersions.designSide] || null,
                version_kind: versionFields[STUDIO_FIELDS.labelVersions.versionKind] || null,
                version_number:
                  versionFields[STUDIO_FIELDS.labelVersions.versionNumber] || null,
                prompt_text: versionFields[STUDIO_FIELDS.labelVersions.promptText] || null,
                edit_prompt_text:
                  versionFields[STUDIO_FIELDS.labelVersions.editPromptText] || null,
                model_name: versionFields[STUDIO_FIELDS.labelVersions.modelName] || null,
                input_reference_url:
                  versionFields[STUDIO_FIELDS.labelVersions.inputReferenceUrl] || null,
                output_image_url:
                  versionFields[STUDIO_FIELDS.labelVersions.outputImageUrl] || null,
                output_pdf_url:
                  versionFields[STUDIO_FIELDS.labelVersions.outputPdfUrl] || null,
                output_s3_key: versionFields[STUDIO_FIELDS.labelVersions.outputS3Key] || null,
                output_s3_url: versionFields[STUDIO_FIELDS.labelVersions.outputS3Url] || null,
                output_zakeke_url:
                  versionFields[STUDIO_FIELDS.labelVersions.outputZakekeUrl] || null,
                session_id: resolveSessionId(
                  versionFields[STUDIO_FIELDS.labelVersions.sessionId],
                  ...sessionSourceLabelIds.map((labelId) => labelSessionById.get(labelId))
                ),
                previous_label_version_id:
                  getLinkedIds(
                    versionRecord,
                    STUDIO_FIELDS.labelVersions.previousLabelVersion,
                    PREVIOUS_LABEL_VERSION_FIELD_FALLBACKS
                  )[0] || null,
              };
            }),
          };
        })
        .sort((a, b) => (a.created_at < b.created_at ? 1 : -1));

      console.info("[studio-list] response-summary", {
        customer_record_id: customerRecordId,
        saved_configurations: summarizeIds(savedConfigurations.map((row) => row.id)),
        labels: summarizeIds(labels.map((row) => row.id)),
        counts: {
          saved_configurations: savedConfigurations.length,
          labels: labels.length,
          label_versions: versionRecords.length,
        },
      });

      return sendJson(200, {
        ok: true,
        customer_record_id: customerRecordId,
        filters: {
          bottle: bottleFilter || null,
          status: statusFilter || null,
        },
        saved_configurations: savedConfigurations,
        labels,
        counts: {
          saved_configurations: savedConfigurations.length,
          labels: labels.length,
          label_versions: versionRecords.length,
        },
      });
    } catch (error) {
      console.error("[studio-list] error", {
        customer_record_id: customerRecordId,
        status: error?.status || 500,
        message: error?.message || String(error),
        url: error?.url || null,
        method: error?.method || null,
        responseText: error?.responseText || null,
      });
      return sendJson(error?.status || 500, mapErrorResponse(error));
    }
  },
  {
    methods: ["GET"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
