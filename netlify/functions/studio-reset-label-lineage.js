import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { escapeFormulaValue } from "../../src/lib/airtable.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeSide,
  normalizeRecordId,
  sanitizeName,
  sanitizeText,
  assertPayloadSize,
  toLinkedRecordArray,
  getLinkedIds,
  createResilient,
  getRecordOrNull,
  deleteRecordOrNull,
  listAllRecords,
  resolveCustomerRecordIdOrCreate,
  mapErrorResponse,
} from "./_lib/studio.js";

const RESET_IDEMPOTENCY_TTL_MS = 30 * 60 * 1000;
const resetLabelLineageCache = new Map();

const sideToTitle = (side) => (side === "back" ? "Back" : "Front");

const getIdempotencyCacheKey = ({ customerRecordId, side, idempotencyKey }) =>
  `${customerRecordId}:${side}:${idempotencyKey}`;

const readIdempotencyCache = ({
  cache,
  key,
  nowMs,
  ttlMs = RESET_IDEMPOTENCY_TTL_MS,
}) => {
  const entry = cache.get(key);
  if (!entry) return null;
  if (nowMs - entry.createdAtMs > ttlMs) {
    cache.delete(key);
    return null;
  }
  return entry.payload || null;
};

const writeIdempotencyCache = ({ cache, key, nowMs, payload }) => {
  if (!payload) return;
  cache.set(key, {
    createdAtMs: nowMs,
    payload,
  });
};

const normalizeIdempotencyKey = (value) =>
  sanitizeText(String(value || "").trim(), 255) || null;

const dedupeRecordIds = (values = []) => {
  const out = [];
  values.forEach((value) => {
    const recordId = normalizeRecordId(value);
    if (recordId && !out.includes(recordId)) out.push(recordId);
  });
  return out;
};

const buildLabelVersionFilterFormula = ({ labelRecordId, side }) => {
  const safeLabelRecordId = escapeFormulaValue(labelRecordId);
  const safeSide = escapeFormulaValue(sideToTitle(side));
  return `AND(FIND('${safeLabelRecordId}', ARRAYJOIN({${STUDIO_FIELDS.labelVersions.labels}})), {${STUDIO_FIELDS.labelVersions.designSide}}='${safeSide}')`;
};

export const __clearResetLabelLineageCacheForTests = () => {
  resetLabelLineageCache.clear();
};

export const createStudioResetLabelLineageHandler = ({
  parseBodyImpl = parseBody,
  assertPayloadSizeImpl = assertPayloadSize,
  sendJsonImpl = sendJson,
  getRecordOrNullImpl = getRecordOrNull,
  deleteRecordOrNullImpl = deleteRecordOrNull,
  listAllRecordsImpl = listAllRecords,
  createResilientImpl = createResilient,
  resolveCustomerRecordIdOrCreateImpl = resolveCustomerRecordIdOrCreate,
  mapErrorResponseImpl = mapErrorResponse,
  idempotencyCache = resetLabelLineageCache,
  nowImpl = () => Date.now(),
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
            "A valid Airtable customer record id is required before reset. Resolve identity via create-airtable-customer first.",
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

      const customerResolution = await resolveCustomerRecordIdOrCreateImpl({
        providedCustomerRecordId,
        body,
        qs,
        allowCreate: false,
        endpoint: "studio-reset-label-lineage",
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

      const idempotencyKey = normalizeIdempotencyKey(
        firstNonEmpty(body.idempotency_key, body.idempotencyKey, qs.idempotency_key)
      );
      const nowMs = Number(nowImpl()) || Date.now();
      if (idempotencyKey) {
        const cacheKey = getIdempotencyCacheKey({
          customerRecordId,
          side,
          idempotencyKey,
        });
        const cached = readIdempotencyCache({
          cache: idempotencyCache,
          key: cacheKey,
          nowMs,
        });
        if (cached) {
          return sendJsonImpl(200, {
            ...cached,
            ok: true,
            idempotent: true,
            idempotency_key: idempotencyKey,
          });
        }
      }

      const requestedLabelRecordId = normalizeRecordId(
        firstNonEmpty(body.label_record_id, body.labelRecordId, body.current_label_record_id)
      );
      const previousLabelVersionRecordId = normalizeRecordId(
        firstNonEmpty(
          body.previous_label_version_record_id,
          body.previousLabelVersionRecordId,
          body.previous_version_record_id,
          body.previousVersionRecordId
        )
      );

      let oldLabelRecord = null;
      if (requestedLabelRecordId) {
        oldLabelRecord = await getRecordOrNullImpl(
          STUDIO_TABLES.labels,
          requestedLabelRecordId
        );
        if (!oldLabelRecord) {
          return sendJsonImpl(404, {
            ok: false,
            error: "label_not_found",
            message: "The supplied label_record_id was not found.",
          });
        }

        const linkedCustomers = getLinkedIds(
          oldLabelRecord,
          STUDIO_FIELDS.labels.customers
        );
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
          previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.designSide]
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
        if (
          oldLabelRecord &&
          previousLabels.length &&
          !previousLabels.includes(oldLabelRecord.id)
        ) {
          return sendJsonImpl(400, {
            ok: false,
            error: "previous_version_label_mismatch",
            message:
              "previous_label_version_record_id must reference the same label head as label_record_id.",
          });
        }

        if (!oldLabelRecord && previousLabels[0]) {
          oldLabelRecord = await getRecordOrNullImpl(
            STUDIO_TABLES.labels,
            previousLabels[0]
          );
          if (!oldLabelRecord) {
            return sendJsonImpl(404, {
              ok: false,
              error: "label_not_found",
              message: "The label inferred from previous_label_version_record_id was not found.",
            });
          }

          const inferredLinkedCustomers = getLinkedIds(
            oldLabelRecord,
            STUDIO_FIELDS.labels.customers
          );
          if (!inferredLinkedCustomers.includes(customerRecordId)) {
            return sendJsonImpl(403, {
              ok: false,
              error: "label_customer_mismatch",
              message: "The inferred label does not belong to this customer.",
            });
          }
        }
      }

      const versionIdsToDelete = [];
      if (oldLabelRecord?.id) {
        const labelFormula = buildLabelVersionFilterFormula({
          labelRecordId: oldLabelRecord.id,
          side,
        });
        const linkedVersions = await listAllRecordsImpl(STUDIO_TABLES.labelVersions, {
          filterByFormula: labelFormula,
        });

        linkedVersions.forEach((versionRecord) => {
          const versionRecordId = normalizeRecordId(versionRecord?.id);
          if (!versionRecordId) return;
          const versionSide = normalizeSide(
            versionRecord?.fields?.[STUDIO_FIELDS.labelVersions.designSide]
          );
          if (versionSide && versionSide !== side) return;
          versionIdsToDelete.push(versionRecordId);
        });
      }

      if (previousVersionRecord?.id) {
        const previousVersionSide = normalizeSide(
          previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.designSide]
        );
        if (!previousVersionSide || previousVersionSide === side) {
          versionIdsToDelete.push(previousVersionRecord.id);
        }
      }

      const dedupedVersionIds = dedupeRecordIds(versionIdsToDelete);
      const deletedVersionRecordIds = [];
      for (const versionRecordId of dedupedVersionIds) {
        const deleted = await deleteRecordOrNullImpl(
          STUDIO_TABLES.labelVersions,
          versionRecordId
        );
        if (deleted?.id) {
          deletedVersionRecordIds.push(deleted.id);
        }
      }

      let deletedLabelRecordId = null;
      if (oldLabelRecord?.id) {
        const deleted = await deleteRecordOrNullImpl(
          STUDIO_TABLES.labels,
          oldLabelRecord.id
        );
        deletedLabelRecordId = normalizeRecordId(deleted?.id) || oldLabelRecord.id;
      }

      const resolvedDisplayName =
        sanitizeName(
          firstNonEmpty(
            body.label_display_name,
            body.labelDisplayName,
            body.display_name,
            body.displayName,
            oldLabelRecord?.fields?.[STUDIO_FIELDS.labels.displayName]
          )
        ) || `${sideToTitle(side)} Label`;
      const resolvedSessionId = sanitizeText(
        firstNonEmpty(
          body.session_id,
          body.sessionId,
          oldLabelRecord?.fields?.[STUDIO_FIELDS.labels.sessionId],
          previousVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.sessionId],
          qs.session_id
        ),
        255
      );

      const newLabelRecord = await createResilientImpl(
        STUDIO_TABLES.labels,
        {},
        {
          [STUDIO_FIELDS.labels.customers]: toLinkedRecordArray(customerRecordId),
          [STUDIO_FIELDS.labels.displayName]: resolvedDisplayName,
          [STUDIO_FIELDS.labels.sessionId]: resolvedSessionId || undefined,
        }
      );

      const responseBody = {
        ok: true,
        customer_record_id: customerRecordId,
        customer_record_recovered: Boolean(customerResolution?.recovered),
        side,
        old_label_record_id: oldLabelRecord?.id || null,
        old_label_version_record_id: previousVersionRecord?.id || null,
        deleted_label_record_id: deletedLabelRecordId,
        deleted_label_version_record_ids: deletedVersionRecordIds,
        new_label_record_id: normalizeRecordId(newLabelRecord?.id),
        new_label_created_at: newLabelRecord?.createdTime || null,
        session_id: resolvedSessionId || null,
        idempotency_key: idempotencyKey,
      };

      if (idempotencyKey) {
        const cacheKey = getIdempotencyCacheKey({
          customerRecordId,
          side,
          idempotencyKey,
        });
        writeIdempotencyCache({
          cache: idempotencyCache,
          key: cacheKey,
          nowMs,
          payload: responseBody,
        });
      }

      return sendJsonImpl(200, responseBody);
    } catch (error) {
      return sendJsonImpl(error?.status || 500, mapErrorResponseImpl(error));
    }
  };

const studioResetLabelLineageHandler = createStudioResetLabelLineageHandler();

export default withShopifyProxy(studioResetLabelLineageHandler, {
  methods: ["POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});
