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
  sanitizeText,
  mapErrorResponse,
} from "./_lib/studio.js";

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

const mapVersion = (record, labelSessionById = new Map()) => {
  const fields = record?.fields || {};
  if (!record) return null;
  const linkedLabelIds = getLinkedIds(record, STUDIO_FIELDS.labelVersions.labels);
  return {
    id: record.id,
    created_at:
      record.createdTime ||
      getFieldValue(
        record,
        STUDIO_FIELDS.labelVersions.createdAt,
        STUDIO_FIELD_FALLBACKS.labelVersions.createdAt
      ) ||
      null,
    name: fields[STUDIO_FIELDS.labelVersions.name] || null,
    design_side: fields[STUDIO_FIELDS.labelVersions.designSide] || null,
    version_kind: fields[STUDIO_FIELDS.labelVersions.versionKind] || null,
    version_number: fields[STUDIO_FIELDS.labelVersions.versionNumber] || null,
    prompt_text: fields[STUDIO_FIELDS.labelVersions.promptText] || null,
    edit_prompt_text: fields[STUDIO_FIELDS.labelVersions.editPromptText] || null,
    model_name: fields[STUDIO_FIELDS.labelVersions.modelName] || null,
    output_image_url: fields[STUDIO_FIELDS.labelVersions.outputImageUrl] || null,
    output_pdf_url: fields[STUDIO_FIELDS.labelVersions.outputPdfUrl] || null,
    output_s3_url: fields[STUDIO_FIELDS.labelVersions.outputS3Url] || null,
    output_zakeke_url: fields[STUDIO_FIELDS.labelVersions.outputZakekeUrl] || null,
    session_id: resolveSessionId(
      fields[STUDIO_FIELDS.labelVersions.sessionId],
      ...linkedLabelIds.map((labelId) => labelSessionById.get(labelId))
    ),
  };
};

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
      const savedConfigurationRecordId = normalizeRecordId(
        firstNonEmpty(
          qs.saved_configuration_record_id,
          qs.savedConfigurationRecordId,
          body.saved_configuration_record_id,
          body.savedConfigurationRecordId,
          qs.id
        )
      );

      if (!customerRecordId || !savedConfigurationRecordId) {
        return sendJson(400, {
          ok: false,
          error: "missing_required_fields",
          message:
            "customer_record_id and saved_configuration_record_id are required.",
        });
      }

      const savedConfig = await getRecordOrNull(
        STUDIO_TABLES.savedConfigurations,
        savedConfigurationRecordId
      );
      if (!savedConfig) {
        return sendJson(404, {
          ok: false,
          error: "saved_configuration_not_found",
          message: "Saved configuration not found.",
        });
      }

      const linkedCustomers = getLinkedIds(
        savedConfig,
        STUDIO_FIELDS.savedConfigurations.customer
      );
      if (!linkedCustomers.includes(customerRecordId)) {
        return sendJson(403, {
          ok: false,
          error: "forbidden",
          message: "This saved configuration does not belong to the current customer.",
        });
      }

      const fields = savedConfig.fields || {};
      const labelIds = getLinkedIds(savedConfig, STUDIO_FIELDS.savedConfigurations.labels);
      const frontVersionId =
        getLinkedIds(
          savedConfig,
          STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion,
          STUDIO_FIELD_FALLBACKS.savedConfigurations.currentFrontLabelVersion
        )[0] || null;
      const backVersionId =
        getLinkedIds(
          savedConfig,
          STUDIO_FIELDS.savedConfigurations.currentBackLabelVersion,
          STUDIO_FIELD_FALLBACKS.savedConfigurations.currentBackLabelVersion
        )[0] || null;

      const [labels, frontVersion, backVersion] = await Promise.all([
        Promise.all(
          labelIds.map(async (id) => {
            const label = await getRecordOrNull(STUDIO_TABLES.labels, id);
            if (!label) return null;
            const lf = label.fields || {};
            return {
              id: label.id,
              display_name: lf[STUDIO_FIELDS.labels.displayName] || null,
              session_id: resolveSessionId(lf[STUDIO_FIELDS.labels.sessionId]),
              current_front_version_id:
                getLinkedIds(
                  label,
                  STUDIO_FIELDS.labels.currentFrontLabelVersion,
                  STUDIO_FIELD_FALLBACKS.labels.currentFrontLabelVersion
                )[0] || null,
              current_back_version_id:
                getLinkedIds(
                  label,
                  STUDIO_FIELDS.labels.currentBackLabelVersion,
                  STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion
                )[0] || null,
            };
          })
        ),
        frontVersionId
          ? getRecordOrNull(STUDIO_TABLES.labelVersions, frontVersionId)
          : Promise.resolve(null),
        backVersionId
          ? getRecordOrNull(STUDIO_TABLES.labelVersions, backVersionId)
          : Promise.resolve(null),
      ]);
      const activeLabels = labels.filter(Boolean);
      const labelSessionById = new Map(
        activeLabels.map((label) => [label.id, resolveSessionId(label.session_id)])
      );
      const configurationSessionId = resolveSessionId(
        fields[STUDIO_FIELDS.savedConfigurations.sessionId],
        ...activeLabels.map((label) => label.session_id)
      );

      return sendJson(200, {
        ok: true,
        configuration: {
          id: savedConfig.id,
          created_at: savedConfig.createdTime || null,
          configuration_id: fields[STUDIO_FIELDS.savedConfigurations.configurationId] || null,
          display_name: fields[STUDIO_FIELDS.savedConfigurations.displayName] || null,
          bottle_selection: fields[STUDIO_FIELDS.savedConfigurations.bottleSelection] || null,
          liquid_selection: fields[STUDIO_FIELDS.savedConfigurations.liquidSelection] || null,
          closure_selection: fields[STUDIO_FIELDS.savedConfigurations.closureSelection] || null,
          wax_selection: fields[STUDIO_FIELDS.savedConfigurations.waxSelection] || null,
          alcohol_selection: fields[STUDIO_FIELDS.savedConfigurations.alcoholSelection] || null,
          internal_sku: fields[STUDIO_FIELDS.savedConfigurations.internalSku] || null,
          shopify_variant_id:
            fields[STUDIO_FIELDS.savedConfigurations.shopifyVariantId] || null,
          status: fields[STUDIO_FIELDS.savedConfigurations.status] || null,
          session_id: configurationSessionId,
          preview_url: fields[STUDIO_FIELDS.savedConfigurations.previewImageUrl] || null,
          snapshot: parseJsonField(fields[STUDIO_FIELDS.savedConfigurations.configJson]),
          label_front_version_id: frontVersionId,
          label_back_version_id: backVersionId,
          labels: activeLabels,
        },
        label_versions: {
          front: mapVersion(frontVersion, labelSessionById),
          back: mapVersion(backVersion, labelSessionById),
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
