import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  STUDIO_FIELD_FALLBACKS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeSide,
  normalizeVersionKind,
  normalizeRecordId,
  sanitizeName,
  sanitizeText,
  sanitizeUrl,
  assertPayloadSize,
  coerceBoolean,
  toLinkedRecordArray,
  getLinkedIds,
  buildLinkedPatch,
  createResilient,
  updateResilient,
  getRecordOrNull,
  listRecordsByLinkedRecordIds,
  resolveCustomerRecordIdOrCreate,
  mapErrorResponse,
} from "./_lib/studio.js";

const sideToTitle = (side) => (side === "back" ? "Back" : "Front");
const LABEL_VERSION_LABEL_FIELD_FALLBACKS = [
  "Labels: Current Front Label Versions",
  "Labels: Current Back Label Versions",
];

const toVersionNumber = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.max(1, Math.floor(num));
};

const deriveS3KeyFromUrl = (value) => {
  const text = String(value || "").trim();
  if (!text) return null;
  try {
    const parsed = new URL(text);
    const pathname = String(parsed.pathname || "").replace(/^\/+/, "");
    if (!pathname) return null;
    return decodeURIComponent(pathname);
  } catch {
    return null;
  }
};

const parsePayloadInputs = (body = {}) => {
  const aiInput =
    body.ai_input && typeof body.ai_input === "object" ? body.ai_input : {};
  const outputs =
    body.outputs && typeof body.outputs === "object" ? body.outputs : {};
  const order =
    body.order && typeof body.order === "object" ? body.order : {};
  const bottle = sanitizeText(
    firstNonEmpty(
      body.bottle,
      body.bottle_name,
      body.bottleName,
      order?.bottle?.name,
      order?.bottleName,
      order?.bottle,
      outputs.bottle
    ),
    120
  );
  const outputImageUrl = sanitizeUrl(
    firstNonEmpty(
      body.output_image_url,
      body.outputImageUrl,
      body.output_s3_url,
      body.outputS3Url,
      outputs.outputImageUrl,
      outputs.frontImage,
      outputs.url,
      outputs.s3url
    )
  );
  const outputS3Url = sanitizeUrl(
    firstNonEmpty(
      body.output_s3_url,
      body.outputS3Url,
      outputs.s3url,
      outputs.url,
      outputImageUrl
    )
  );
  const outputPdfUrl = sanitizeUrl(
    firstNonEmpty(body.output_pdf_url, body.outputPdfUrl, outputs.outputPdfUrl)
  );
  const outputZakekeUrl = sanitizeUrl(
    firstNonEmpty(
      body.output_zakeke_url,
      body.outputZakekeUrl,
      outputs.outputZakekeUrl,
      outputs.zakekeUrl,
      outputImageUrl,
      outputS3Url
    )
  );
  const derivedS3Key = deriveS3KeyFromUrl(outputS3Url);

  return {
    bottle,
    promptText: sanitizeText(
      firstNonEmpty(
        body.prompt_text,
        body.promptText,
        body.prompt,
        aiInput.prompt
      )
    ),
    editPromptText: sanitizeText(
      firstNonEmpty(
        body.edit_prompt_text,
        body.editPromptText,
        body.critique,
        body.revision_notes,
        body.revisionNotes
      )
    ),
    modelName: sanitizeText(
      firstNonEmpty(body.model_name, body.modelName, outputs.modelName),
      255
    ),
    inputReferenceUrl: sanitizeUrl(
      firstNonEmpty(
        body.input_reference_url,
        body.inputReferenceUrl,
        body.previousImage,
        outputs.inputReferenceUrl
      )
    ),
    outputImageUrl,
    outputPdfUrl,
    outputS3Key: sanitizeText(
      firstNonEmpty(
        body.output_s3_key,
        body.outputS3Key,
        outputs.outputS3Key,
        derivedS3Key
      ),
      255
    ),
    outputS3Url,
    outputZakekeUrl,
  };
};

async function computeNextVersionNumber(labelRecordId) {
  const rows = await listRecordsByLinkedRecordIds(STUDIO_TABLES.labelVersions, {
    fieldName: STUDIO_FIELDS.labelVersions.labels,
    linkedRecordIds: labelRecordId,
    fallbackFieldNames: LABEL_VERSION_LABEL_FIELD_FALLBACKS,
  });
  const current = rows.reduce((maxVersion, record) => {
    const versionNumber = toVersionNumber(
      record?.fields?.[STUDIO_FIELDS.labelVersions.versionNumber]
    );
    return versionNumber && versionNumber > maxVersion ? versionNumber : maxVersion;
  }, 0);
  return current + 1;
}

export const createStudioSaveLabelVersionHandler = ({
  parseBodyImpl = parseBody,
  assertPayloadSizeImpl = assertPayloadSize,
  sendJsonImpl = sendJson,
  getRecordOrNullImpl = getRecordOrNull,
  createResilientImpl = createResilient,
  updateResilientImpl = updateResilient,
  computeNextVersionNumberImpl = computeNextVersionNumber,
  resolveCustomerRecordIdOrCreateImpl = resolveCustomerRecordIdOrCreate,
  mapErrorResponseImpl = mapErrorResponse,
} = {}) =>
  async (arg, { qs = {}, isV2, method }) => {
    try {
      const body = (await parseBodyImpl(arg, method, isV2)) || {};
      assertPayloadSizeImpl(body);

      const providedCustomerRecordId = normalizeRecordId(
        firstNonEmpty(
          body.customer_record_id,
          body.customerRecordId,
          body.customer_id,
          body.customerId,
          body.ss_customer_airtable_id,
          qs.customer_record_id,
          qs.customer_id
        )
      );
      if (!providedCustomerRecordId) {
        return sendJsonImpl(409, {
          ok: false,
          error: "customer_not_resolved",
          message:
            "A valid Airtable customer record id is required before saving. Resolve identity via create-airtable-customer first.",
        });
      }

      const side = normalizeSide(
        firstNonEmpty(body.design_side, body.designSide, body.side, qs.side)
      );
      if (!side) {
        return sendJsonImpl(400, {
          ok: false,
          error: "invalid_side",
          message: "design_side must be either front or back.",
        });
      }

      const versionKind = normalizeVersionKind(
        firstNonEmpty(
          body.version_kind,
          body.versionKind,
          body.kind,
          body.requestKind,
          qs.version_kind
        )
      );
      if (!versionKind) {
        return sendJsonImpl(400, {
          ok: false,
          error: "invalid_version_kind",
          message: "version_kind must be Initial, Edit, or Upload.",
        });
      }

      const accepted = coerceBoolean(body.accepted, true);
      if (!accepted) {
        return sendJsonImpl(400, {
          ok: false,
          error: "not_accepted",
          message: "Only accepted label versions can be persisted.",
        });
      }

      const forceNewLabelHead = coerceBoolean(
        body.force_new_label_head ?? body.forceNewLabelHead,
        false
      );
      const customerResolution = await resolveCustomerRecordIdOrCreateImpl({
        providedCustomerRecordId,
        body,
        qs,
        allowCreate: true,
        endpoint: "studio-save-label-version",
      });
      const customerRecordId = normalizeRecordId(customerResolution?.customerRecordId);
      if (!customerRecordId) {
        return sendJsonImpl(409, {
          ok: false,
          error: "customer_not_resolved",
          message:
            "Could not resolve Airtable customer record id for this request. Resolve identity via create-airtable-customer first.",
          provided_customer_record_id: providedCustomerRecordId,
        });
      }

      const requestedLabelRecordId = normalizeRecordId(
        firstNonEmpty(body.label_record_id, body.labelRecordId, body.current_label_record_id)
      );
      let previousLabelVersionRecordId = normalizeRecordId(
        firstNonEmpty(
          body.previous_label_version_record_id,
          body.previousLabelVersionRecordId,
          body.previous_version_record_id,
          body.previousVersionRecordId
        )
      );

      const sessionId = sanitizeText(
        firstNonEmpty(body.session_id, body.sessionId, qs.session_id),
        255
      );

      let labelRecord = null;
      if (requestedLabelRecordId && !forceNewLabelHead) {
        labelRecord = await getRecordOrNullImpl(STUDIO_TABLES.labels, requestedLabelRecordId);
        if (!labelRecord) {
          return sendJsonImpl(404, {
            ok: false,
            error: "label_not_found",
            message: "The supplied label_record_id was not found.",
          });
        }
        const linkedCustomers = getLinkedIds(labelRecord, STUDIO_FIELDS.labels.customers);
        if (!linkedCustomers.includes(customerRecordId)) {
          return sendJsonImpl(403, {
            ok: false,
            error: "label_customer_mismatch",
            message: "The supplied label does not belong to this customer.",
          });
        }
      }

      let previousVersionRecord = null;
      if (previousLabelVersionRecordId) {
        previousVersionRecord = await getRecordOrNullImpl(
          STUDIO_TABLES.labelVersions,
          previousLabelVersionRecordId
        );
        if (!previousVersionRecord) {
          return sendJsonImpl(404, {
            ok: false,
            error: "previous_version_not_found",
            message: "The supplied previous label version record does not exist.",
          });
        }

        const previousSide = normalizeSide(
          previousVersionRecord.fields?.[STUDIO_FIELDS.labelVersions.designSide]
        );
        if (previousSide && previousSide !== side) {
          return sendJsonImpl(400, {
            ok: false,
            error: "side_mismatch",
            message: "Previous label version side does not match the requested side.",
          });
        }

        const previousLabels = getLinkedIds(
          previousVersionRecord,
          STUDIO_FIELDS.labelVersions.labels
        );
        if (labelRecord && previousLabels.length && !previousLabels.includes(labelRecord.id)) {
          return sendJsonImpl(400, {
            ok: false,
            error: "previous_version_label_mismatch",
            message:
              "previous_label_version_record_id must reference the same label head as label_record_id.",
          });
        }

        if (!labelRecord && !forceNewLabelHead) {
          if (previousLabels[0]) {
            const inferredLabelRecord = await getRecordOrNullImpl(
              STUDIO_TABLES.labels,
              previousLabels[0]
            );
            if (inferredLabelRecord) {
              const linkedCustomers = getLinkedIds(
                inferredLabelRecord,
                STUDIO_FIELDS.labels.customers
              );
              if (!linkedCustomers.includes(customerRecordId)) {
                return sendJsonImpl(403, {
                  ok: false,
                  error: "label_customer_mismatch",
                  message:
                    "The inferred label does not belong to this customer.",
                });
              }
            }
            labelRecord = inferredLabelRecord;
          }
        }
      }

      const labelDisplayName = sanitizeName(
        firstNonEmpty(
          body.label_display_name,
          body.labelDisplayName,
          body.display_name,
          body.displayName,
          body.title
        )
      );

      if (!labelRecord) {
        labelRecord = await createResilientImpl(
          STUDIO_TABLES.labels,
          {},
          {
            [STUDIO_FIELDS.labels.customers]: toLinkedRecordArray(customerRecordId),
            [STUDIO_FIELDS.labels.displayName]:
              labelDisplayName || `${sideToTitle(side)} Label`,
            [STUDIO_FIELDS.labels.sessionId]: sessionId || undefined,
          }
        );
      }

      const labelSessionId = sanitizeText(
        labelRecord?.fields?.[STUDIO_FIELDS.labels.sessionId],
        255
      );
      const previousVersionSessionId = sanitizeText(
        previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.sessionId],
        255
      );
      const resolvedSessionId = sanitizeText(
        firstNonEmpty(labelSessionId, previousVersionSessionId, sessionId),
        255
      );

      const payloadInputs = parsePayloadInputs(body);
      const previousOutputImageUrl = sanitizeUrl(
        previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.outputImageUrl]
      );
      const previousOutputS3Url = sanitizeUrl(
        previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.outputS3Url]
      );
      const previousOutputS3Key = sanitizeText(
        previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.outputS3Key],
        255
      );
      console.info("[trace:s3:backend:studio-save-label-version:resolved-input]", {
        sessionId: resolvedSessionId || sessionId || null,
        side,
        versionKind,
        forceNewLabelHead,
        labelRecordId: labelRecord?.id || null,
        previousLabelVersionRecordId: previousLabelVersionRecordId || null,
        previousOutputImageUrl: previousOutputImageUrl || null,
        previousOutputS3Url: previousOutputS3Url || null,
        previousOutputS3Key: previousOutputS3Key || null,
        incomingOutputImageUrl: payloadInputs.outputImageUrl || null,
        incomingOutputS3Url: payloadInputs.outputS3Url || null,
        incomingOutputS3Key: payloadInputs.outputS3Key || null,
        incomingOutputPdfUrl: payloadInputs.outputPdfUrl || null,
      });
      const hasOutputRef = Boolean(
        payloadInputs.outputImageUrl ||
          payloadInputs.outputS3Url ||
          payloadInputs.outputZakekeUrl ||
          payloadInputs.outputPdfUrl
      );
      if (!hasOutputRef) {
        return sendJsonImpl(400, {
          ok: false,
          error: "missing_output_refs",
          message:
            "At least one output reference URL is required (output_image_url, output_s3_url, output_zakeke_url, or output_pdf_url).",
        });
      }
      const previousVersionNumber = toVersionNumber(
        previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.versionNumber]
      );
      const inferredNextFromPrevious = previousVersionNumber
        ? previousVersionNumber + 1
        : null;
      const computedNextVersionNumber = await computeNextVersionNumberImpl(labelRecord.id);
      const versionNumber = Math.max(
        inferredNextFromPrevious || 1,
        toVersionNumber(computedNextVersionNumber) || 1
      );
      const versionName =
        sanitizeName(firstNonEmpty(body.version_name, body.versionName)) ||
        `${sideToTitle(side)} ${versionKind} v${versionNumber}`;

      const versionRecord = await createResilientImpl(
        STUDIO_TABLES.labelVersions,
        {
          [STUDIO_FIELDS.labelVersions.labels]: toLinkedRecordArray(labelRecord.id),
          [STUDIO_FIELDS.labelVersions.designSide]: sideToTitle(side),
          [STUDIO_FIELDS.labelVersions.versionKind]: versionKind,
          [STUDIO_FIELDS.labelVersions.versionNumber]: versionNumber,
          [STUDIO_FIELDS.labelVersions.accepted]: true,
        },
        {
          [STUDIO_FIELDS.labelVersions.bottle]: payloadInputs.bottle,
          [STUDIO_FIELDS.labelVersions.name]: versionName,
          [STUDIO_FIELDS.labelVersions.promptText]: payloadInputs.promptText,
          [STUDIO_FIELDS.labelVersions.editPromptText]: payloadInputs.editPromptText,
          [STUDIO_FIELDS.labelVersions.modelName]: payloadInputs.modelName,
          [STUDIO_FIELDS.labelVersions.inputReferenceUrl]:
            payloadInputs.inputReferenceUrl,
          [STUDIO_FIELDS.labelVersions.outputImageUrl]: payloadInputs.outputImageUrl,
          [STUDIO_FIELDS.labelVersions.outputPdfUrl]: payloadInputs.outputPdfUrl,
          [STUDIO_FIELDS.labelVersions.outputS3Key]: payloadInputs.outputS3Key,
          [STUDIO_FIELDS.labelVersions.outputS3Url]: payloadInputs.outputS3Url,
          [STUDIO_FIELDS.labelVersions.outputZakekeUrl]:
            payloadInputs.outputZakekeUrl,
          [STUDIO_FIELDS.labelVersions.outputImage]:
            payloadInputs.outputImageUrl || payloadInputs.outputS3Url
              ? [
                  {
                    url: payloadInputs.outputImageUrl || payloadInputs.outputS3Url,
                  },
                ]
            : undefined,
          [STUDIO_FIELDS.labelVersions.sessionId]:
            resolvedSessionId || undefined,
          [STUDIO_FIELDS.labelVersions.previousLabelVersion]:
            toLinkedRecordArray(previousLabelVersionRecordId),
        }
      );

      const existingVersionIds = getLinkedIds(
        labelRecord,
        STUDIO_FIELDS.labels.labelVersions
      );
      const mergedVersionIds = Array.from(
        new Set([...existingVersionIds, versionRecord.id])
      );

      await updateResilientImpl(
        STUDIO_TABLES.labels,
        labelRecord.id,
        {},
        {
          [STUDIO_FIELDS.labels.customers]: toLinkedRecordArray(customerRecordId),
          [STUDIO_FIELDS.labels.displayName]: labelDisplayName || undefined,
          [STUDIO_FIELDS.labels.labelVersions]:
            mergedVersionIds.length ? mergedVersionIds : undefined,
          [STUDIO_FIELDS.labels.sessionId]: resolvedSessionId || undefined,
          ...(side === "front"
            ? buildLinkedPatch(
                labelRecord,
                STUDIO_FIELDS.labels.currentFrontLabelVersion,
                versionRecord.id,
                {
                  fallbackFieldNames:
                    STUDIO_FIELD_FALLBACKS.labels.currentFrontLabelVersion,
                }
              )
            : {}),
          ...(side === "back"
            ? buildLinkedPatch(
                labelRecord,
                STUDIO_FIELDS.labels.currentBackLabelVersion,
                versionRecord.id,
                {
                  fallbackFieldNames:
                    STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion,
                }
              )
            : {}),
        }
      );

      console.info("[trace:s3:backend:studio-save-label-version:created]", {
        sessionId: resolvedSessionId || null,
        side,
        versionKind,
        labelRecordId: labelRecord.id,
        labelVersionRecordId: versionRecord.id,
        versionNumber,
        previousLabelVersionRecordId: previousLabelVersionRecordId || null,
        previousOutputImageUrl: previousOutputImageUrl || null,
        previousOutputS3Url: previousOutputS3Url || null,
        previousOutputS3Key: previousOutputS3Key || null,
        persistedOutputImageUrl: payloadInputs.outputImageUrl || null,
        persistedOutputS3Url: payloadInputs.outputS3Url || null,
        persistedOutputS3Key: payloadInputs.outputS3Key || null,
      });

      return sendJsonImpl(200, {
        ok: true,
        customer_record_id: customerRecordId,
        customer_record_recovered: Boolean(customerResolution?.recovered),
        session_id: resolvedSessionId || null,
        side,
        version_kind: versionKind,
        label_record_id: labelRecord.id,
        label_version_record_id: versionRecord.id,
        version_number: versionNumber,
        label_created_at: labelRecord.createdTime || null,
        label_version_created_at: versionRecord.createdTime || null,
      });
    } catch (error) {
      return sendJsonImpl(error?.status || 500, mapErrorResponseImpl(error));
    }
  };

const studioSaveLabelVersionHandler = createStudioSaveLabelVersionHandler();

export default withShopifyProxy(
  studioSaveLabelVersionHandler,
  {
    methods: ["POST"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
