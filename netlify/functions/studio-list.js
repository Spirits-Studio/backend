import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { escapeFormulaValue } from "../../src/lib/airtable.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  STUDIO_FIELD_FALLBACKS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeRecordId,
  listAllRecords,
  buildLinkedCustomerFormula,
  getFieldValue,
  getLinkedIds,
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

const buildFormula = ({ customerRecordId, bottleFilter, statusFilter }) => {
  const clauses = [
    buildLinkedCustomerFormula(
      STUDIO_FIELDS.savedConfigurations.customer,
      customerRecordId
    ),
  ];

  if (bottleFilter) {
    const safe = escapeFormulaValue(bottleFilter.toLowerCase());
    clauses.push(
      `LOWER({${STUDIO_FIELDS.savedConfigurations.bottleSelection}})='${safe}'`
    );
  }

  if (statusFilter) {
    const safe = escapeFormulaValue(statusFilter.toLowerCase());
    clauses.push(`LOWER({${STUDIO_FIELDS.savedConfigurations.status}})='${safe}'`);
  }

  if (clauses.length === 1) return clauses[0];
  return `AND(${clauses.join(",")})`;
};

const buildVersionsFormula = (labelRecordIds = []) => {
  const clauses = labelRecordIds
    .map((id) => normalizeRecordId(id))
    .filter(Boolean)
    .map((id) => `FIND('${escapeFormulaValue(id)}', ARRAYJOIN({${STUDIO_FIELDS.labelVersions.labels}}))`);
  if (!clauses.length) return null;
  if (clauses.length === 1) return clauses[0];
  return `OR(${clauses.join(",")})`;
};

const PREVIOUS_LABEL_VERSION_FIELD_FALLBACKS = ["Previous Label Versions"];

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
        return sendJson(400, {
          ok: false,
          error: "missing_customer_record_id",
          message: "A valid Airtable customer record id is required.",
        });
      }

      const bottleFilter = toText(firstNonEmpty(qs.bottle, body.bottle)).toLowerCase();
      const statusFilter = toText(firstNonEmpty(qs.status, body.status)).toLowerCase();

      const savedConfigFormula = buildFormula({
        customerRecordId,
        bottleFilter,
        statusFilter,
      });
      const savedConfigRecords = await listAllRecords(STUDIO_TABLES.savedConfigurations, {
        filterByFormula: savedConfigFormula,
      });

      const labelsFormula = buildLinkedCustomerFormula(
        STUDIO_FIELDS.labels.customers,
        customerRecordId
      );
      const labelRecords = await listAllRecords(STUDIO_TABLES.labels, {
        filterByFormula: labelsFormula,
      });

      const labelRecordIds = labelRecords.map((record) => record.id);
      const versionRecords = [];
      for (let idx = 0; idx < labelRecordIds.length; idx += 15) {
        const chunk = labelRecordIds.slice(idx, idx + 15);
        const formula = buildVersionsFormula(chunk);
        if (!formula) continue;
        const chunkRows = await listAllRecords(STUDIO_TABLES.labelVersions, {
          filterByFormula: formula,
        });
        versionRecords.push(...chunkRows);
      }

      const versionsById = new Map(versionRecords.map((record) => [record.id, record]));
      const versionsByLabelId = new Map();
      versionRecords.forEach((record) => {
        const linkedLabels = getLinkedIds(record, STUDIO_FIELDS.labelVersions.labels);
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
          const linkedIds = getLinkedIds(record, STUDIO_FIELDS.labels.labelVersions);
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
                STUDIO_FIELDS.labelVersions.labels
              );
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
                input_character_url:
                  versionFields[STUDIO_FIELDS.labelVersions.inputCharacterUrl] || null,
                input_logo_url:
                  versionFields[STUDIO_FIELDS.labelVersions.inputLogoUrl] || null,
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
                  ...linkedVersionLabelIds.map((labelId) => labelSessionById.get(labelId))
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
      return sendJson(error?.status || 500, mapErrorResponse(error));
    }
  },
  {
    methods: ["GET"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
