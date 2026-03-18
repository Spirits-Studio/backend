import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { escapeFormulaValue, listRecords } from "../../src/lib/airtable.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeSide,
  normalizeRecordId,
  sanitizeText,
  toLinkedRecordArray,
  getFieldValue,
  getLinkedIds,
  getRecordOrNull,
  updateResilient,
  mapErrorResponse,
} from "./_lib/studio.js";

const SESSION_FIELD_CANDIDATES = [
  STUDIO_FIELDS.savedConfigurations.sessionId,
  "session_id",
  "sessionId",
  "SessionId",
];

const LABEL_VERSION_FIELD_FALLBACKS = {
  sessionId: ["session_id", "sessionId", "SessionId"],
  designSide: ["design_side", "designSide", "Side"],
  versionNumber: ["version_number", "versionNumber"],
  versionKind: ["version_kind", "versionKind"],
  outputImageUrl: ["output_image_url", "outputImageUrl"],
  outputPdfUrl: ["output_pdf_url", "outputPdfUrl"],
};

const SAVED_CONFIGURATION_FIELD_FALLBACKS = {
  customer: ["Customers", "customer", "customer_id", "customerId"],
};

const SAVED_CONFIGURATION_SELECTION_FIELDS = {
  front: {
    selectedId: [
      "selectedFrontLabelVersionRecordId",
      "Selected Front Label Version Record ID",
      "selected_front_label_version_record_id",
    ],
    selectedAt: [
      "selectedFrontLabelVersionAt",
      "Selected Front Label Version At",
      "selected_front_label_version_at",
    ],
    selectedBy: [
      "selectedFrontLabelVersionBy",
      "Selected Front Label Version By",
      "selected_front_label_version_by",
    ],
  },
  back: {
    selectedId: [
      "selectedBackLabelVersionRecordId",
      "Selected Back Label Version Record ID",
      "selected_back_label_version_record_id",
    ],
    selectedAt: [
      "selectedBackLabelVersionAt",
      "Selected Back Label Version At",
      "selected_back_label_version_at",
    ],
    selectedBy: [
      "selectedBackLabelVersionBy",
      "Selected Back Label Version By",
      "selected_back_label_version_by",
    ],
  },
};

const getRecordIdFromFieldValue = (value) => {
  if (Array.isArray(value)) {
    for (const entry of value) {
      const recordId = normalizeRecordId(entry);
      if (recordId) return recordId;
    }
    return null;
  }
  return normalizeRecordId(value);
};

const pickKnownFieldName = (record, candidates = []) => {
  for (const fieldName of candidates) {
    if (Object.prototype.hasOwnProperty.call(record?.fields || {}, fieldName)) {
      return fieldName;
    }
  }
  return candidates[0] || null;
};

const toCreatedTimeMs = (record) => {
  const createdTime = sanitizeText(record?.createdTime, 64);
  if (!createdTime) return null;
  const parsed = Date.parse(createdTime);
  return Number.isFinite(parsed) ? parsed : null;
};

const compareRecordsByLatest = (a, b) => {
  const aCreatedTime = toCreatedTimeMs(a);
  const bCreatedTime = toCreatedTimeMs(b);

  if (aCreatedTime != null || bCreatedTime != null) {
    if (aCreatedTime == null) return 1;
    if (bCreatedTime == null) return -1;
    if (aCreatedTime !== bCreatedTime) return bCreatedTime - aCreatedTime;
  }

  const aId = sanitizeText(a?.id, 64) || "";
  const bId = sanitizeText(b?.id, 64) || "";
  return bId.localeCompare(aId);
};

const findSavedConfigurationBySessionId = async (sessionId) => {
  const safeSessionId = escapeFormulaValue(sessionId);

  for (const fieldName of SESSION_FIELD_CANDIDATES) {
    const formula = `{${fieldName}}='${safeSessionId}'`;
    try {
      const page = await listRecords(STUDIO_TABLES.savedConfigurations, {
        filterByFormula: formula,
        maxRecords: 20,
        pageSize: 20,
      });
      const candidates = (Array.isArray(page?.records) ? page.records : []).filter(
        (record) => record?.id
      );
      if (!candidates.length) continue;

      const sortedCandidates = [...candidates].sort(compareRecordsByLatest);
      const selected = sortedCandidates[0] || null;
      if (sortedCandidates.length > 1 && selected?.id) {
        console.warn(
          "[trace:s3:backend:select-label-version:session-collision]",
          {
            sessionId,
            matchedFieldName: fieldName,
            candidateRecordIds: sortedCandidates.map((record) => record.id),
            selectedRecordId: selected.id,
          }
        );
      }
      if (selected?.id) return selected;
    } catch (error) {
      if (Number(error?.status || 0) === 422) continue;
      throw error;
    }
  }

  return null;
};

const buildSelectedLabelVersion = ({
  labelVersionRecord,
  selectedAt,
  source,
  designSide,
}) => ({
  recordId: labelVersionRecord?.id || null,
  versionNumber:
    Number(
      getFieldValue(
        labelVersionRecord,
        STUDIO_FIELDS.labelVersions.versionNumber,
        LABEL_VERSION_FIELD_FALLBACKS.versionNumber
      )
    ) || null,
  versionKind:
    sanitizeText(
      getFieldValue(
        labelVersionRecord,
        STUDIO_FIELDS.labelVersions.versionKind,
        LABEL_VERSION_FIELD_FALLBACKS.versionKind
      ),
      40
    ) || null,
  outputImageUrl:
    sanitizeText(
      getFieldValue(
        labelVersionRecord,
        STUDIO_FIELDS.labelVersions.outputImageUrl,
        LABEL_VERSION_FIELD_FALLBACKS.outputImageUrl
      ),
      2048
    ) || null,
  outputPdfUrl:
    sanitizeText(
      getFieldValue(
        labelVersionRecord,
        STUDIO_FIELDS.labelVersions.outputPdfUrl,
        LABEL_VERSION_FIELD_FALLBACKS.outputPdfUrl
      ),
      2048
    ) || null,
  selectedAt,
  source,
  designSide,
});

const getLinkedCustomerIds = (savedConfigurationRecord) => {
  const linkedIds = getLinkedIds(
    savedConfigurationRecord,
    STUDIO_FIELDS.savedConfigurations.customer,
    SAVED_CONFIGURATION_FIELD_FALLBACKS.customer
  );
  if (linkedIds.length) return linkedIds;

  const singleCustomerId = getRecordIdFromFieldValue(
    getFieldValue(
      savedConfigurationRecord,
      STUDIO_FIELDS.savedConfigurations.customer,
      SAVED_CONFIGURATION_FIELD_FALLBACKS.customer
    )
  );
  return singleCustomerId ? [singleCustomerId] : [];
};

const createStudioSelectLabelVersionHandler = ({
  parseBodyImpl = parseBody,
  sendJsonImpl = sendJson,
  getRecordOrNullImpl = getRecordOrNull,
  updateResilientImpl = updateResilient,
  findSavedConfigurationBySessionIdImpl = findSavedConfigurationBySessionId,
  mapErrorResponseImpl = mapErrorResponse,
} = {}) =>
  async (arg, { isV2, method }) => {
    try {
      const body = (await parseBodyImpl(arg, method, isV2)) || {};
      const sessionId = sanitizeText(
        firstNonEmpty(body.sessionId, body.session_id),
        255
      );
      const designSide = normalizeSide(
        firstNonEmpty(body.designSide, body.design_side)
      );
      const labelVersionRecordId = normalizeRecordId(
        firstNonEmpty(body.labelVersionRecordId, body.label_version_record_id)
      );
      const customerRecordId = normalizeRecordId(
        firstNonEmpty(body.customerRecordId, body.customer_record_id)
      );
      const savedConfigurationRecordId = normalizeRecordId(
        firstNonEmpty(
          body.savedConfigurationRecordId,
          body.saved_configuration_record_id
        )
      );
      const source = sanitizeText(firstNonEmpty(body.source), 80);
      const selectedAt =
        sanitizeText(firstNonEmpty(body.selectedAt, body.selected_at), 64) ||
        new Date().toISOString();

      console.info("[trace:s3:backend:select-label-version:request]", {
        sessionId: sessionId || null,
        designSide: designSide || null,
        labelVersionRecordId: labelVersionRecordId || null,
        customerRecordId: customerRecordId || null,
        savedConfigurationRecordId: savedConfigurationRecordId || null,
        source: source || null,
        selectedAt,
      });

      if (!sessionId) {
        return sendJsonImpl(400, { ok: false, error: "missing_session_id" });
      }
      if (!designSide) {
        return sendJsonImpl(400, { ok: false, error: "invalid_design_side" });
      }
      if (!labelVersionRecordId) {
        return sendJsonImpl(400, {
          ok: false,
          error: "missing_label_version_record_id",
        });
      }
      if (!source) {
        return sendJsonImpl(400, { ok: false, error: "missing_source" });
      }

      const labelVersionRecord = await getRecordOrNullImpl(
        STUDIO_TABLES.labelVersions,
        labelVersionRecordId
      );
      if (!labelVersionRecord) {
        return sendJsonImpl(404, { ok: false, error: "label_version_not_found" });
      }

      const labelVersionSessionId = sanitizeText(
        getFieldValue(
          labelVersionRecord,
          STUDIO_FIELDS.labelVersions.sessionId,
          LABEL_VERSION_FIELD_FALLBACKS.sessionId
        ),
        255
      );
      const labelVersionSide = normalizeSide(
        getFieldValue(
          labelVersionRecord,
          STUDIO_FIELDS.labelVersions.designSide,
          LABEL_VERSION_FIELD_FALLBACKS.designSide
        )
      );

      if (labelVersionSessionId && labelVersionSessionId !== sessionId) {
        return sendJsonImpl(409, {
          ok: false,
          error: "session_mismatch",
          expectedSessionId: labelVersionSessionId,
        });
      }
      if (labelVersionSide && labelVersionSide !== designSide) {
        return sendJsonImpl(409, {
          ok: false,
          error: "design_side_mismatch",
          expectedDesignSide: labelVersionSide,
        });
      }

      const selectedLabelVersion = buildSelectedLabelVersion({
        labelVersionRecord,
        selectedAt,
        source,
        designSide,
      });

      let savedConfigurationRecord = null;
      let savedConfigurationResolution = "session";
      if (savedConfigurationRecordId) {
        savedConfigurationResolution = "explicit";
        savedConfigurationRecord = await getRecordOrNullImpl(
          STUDIO_TABLES.savedConfigurations,
          savedConfigurationRecordId
        );
        if (!savedConfigurationRecord) {
          return sendJsonImpl(404, {
            ok: false,
            error: "saved_configuration_not_found",
          });
        }
      } else {
        savedConfigurationRecord =
          await findSavedConfigurationBySessionIdImpl(sessionId);
      }

      if (!savedConfigurationRecord) {
        console.info("[trace:s3:backend:select-label-version:deferred]", {
          sessionId,
          designSide,
          labelVersionRecordId,
          reason: "saved_configuration_not_found",
        });
        return sendJsonImpl(200, {
          ok: true,
          idempotent: false,
          deferred: true,
          sessionId,
          designSide,
          selectedLabelVersion,
          saved_configuration_record_id: null,
          savedConfigurationRecordId: null,
        });
      }

      const linkedCustomerIds = getLinkedCustomerIds(savedConfigurationRecord);
      if (
        customerRecordId &&
        linkedCustomerIds.length &&
        !linkedCustomerIds.includes(customerRecordId)
      ) {
        return sendJsonImpl(403, {
          ok: false,
          error: "forbidden",
          message: "This saved configuration does not belong to the current customer.",
        });
      }

      const savedConfigurationSessionId = sanitizeText(
        getFieldValue(
          savedConfigurationRecord,
          STUDIO_FIELDS.savedConfigurations.sessionId,
          SESSION_FIELD_CANDIDATES.slice(1)
        ),
        255
      );
      if (
        savedConfigurationSessionId &&
        savedConfigurationSessionId !== sessionId
      ) {
        return sendJsonImpl(409, {
          ok: false,
          error: "saved_configuration_session_mismatch",
          expectedSessionId: savedConfigurationSessionId,
        });
      }

      const sideFields = SAVED_CONFIGURATION_SELECTION_FIELDS[designSide];
      const selectedIdField = pickKnownFieldName(
        savedConfigurationRecord,
        sideFields.selectedId
      );
      const selectedAtField = pickKnownFieldName(
        savedConfigurationRecord,
        sideFields.selectedAt
      );
      const selectedByField = pickKnownFieldName(
        savedConfigurationRecord,
        sideFields.selectedBy
      );
      const currentSelectedId = getRecordIdFromFieldValue(
        getFieldValue(
          savedConfigurationRecord,
          selectedIdField,
          sideFields.selectedId
        )
      );

      console.info("[trace:s3:backend:select-label-version:resolved]", {
        sessionId,
        designSide,
        labelVersionRecordId,
        savedConfigurationRecordId: savedConfigurationRecord.id,
        savedConfigurationResolution,
        customerRecordId: customerRecordId || null,
        currentSelectedId: currentSelectedId || null,
        candidateOutputImageUrl: selectedLabelVersion.outputImageUrl || null,
        candidateOutputPdfUrl: selectedLabelVersion.outputPdfUrl || null,
      });

      if (currentSelectedId && currentSelectedId === labelVersionRecordId) {
        console.info("[trace:s3:backend:select-label-version:idempotent]", {
          sessionId,
          designSide,
          labelVersionRecordId,
          currentSelectedId,
          outputImageUrl: selectedLabelVersion.outputImageUrl || null,
        });
        return sendJsonImpl(200, {
          ok: true,
          idempotent: true,
          sessionId,
          designSide,
          selectedLabelVersion,
          saved_configuration_record_id: savedConfigurationRecord.id,
          savedConfigurationRecordId: savedConfigurationRecord.id,
        });
      }

      const updateFields = {
        [selectedIdField]: labelVersionRecordId,
        [selectedAtField]: selectedAt,
        [selectedByField]: source,
      };

      try {
        await updateResilientImpl(
          STUDIO_TABLES.savedConfigurations,
          savedConfigurationRecord.id,
          {},
          updateFields
        );
      } catch (error) {
        if (Number(error?.status || 0) !== 422) throw error;
        await updateResilientImpl(
          STUDIO_TABLES.savedConfigurations,
          savedConfigurationRecord.id,
          {},
          {
            ...updateFields,
            [selectedIdField]: toLinkedRecordArray(labelVersionRecordId),
          }
        );
      }

      console.info("[trace:s3:backend:select-label-version:updated]", {
        sessionId,
        designSide,
        previousSelectedId: currentSelectedId || null,
        nextSelectedId: labelVersionRecordId,
        outputImageUrl: selectedLabelVersion.outputImageUrl || null,
        outputPdfUrl: selectedLabelVersion.outputPdfUrl || null,
        savedConfigurationRecordId: savedConfigurationRecord.id,
        savedConfigurationResolution,
      });

      return sendJsonImpl(200, {
        ok: true,
        idempotent: false,
        sessionId,
        designSide,
        selectedLabelVersion,
        saved_configuration_record_id: savedConfigurationRecord.id,
        savedConfigurationRecordId: savedConfigurationRecord.id,
      });
    } catch (error) {
      return sendJsonImpl(error?.status || 500, mapErrorResponseImpl(error));
    }
  };

const studioSelectLabelVersionHandler = createStudioSelectLabelVersionHandler();

export { createStudioSelectLabelVersionHandler, findSavedConfigurationBySessionId };

export default withShopifyProxy(studioSelectLabelVersionHandler, {
  methods: ["POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});
