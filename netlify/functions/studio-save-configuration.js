import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  STUDIO_FIELD_FALLBACKS,
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeRecordId,
  normalizeSide,
  normalizeStatus,
  sanitizeName,
  sanitizeText,
  sanitizeUrl,
  assertPayloadSize,
  toLinkedRecordArray,
  getLinkedIds,
  buildLinkedPatch,
  createResilient,
  updateResilient,
  getRecordOrNull,
  mapErrorResponse,
} from "./_lib/studio.js";

const defaultConfigurationId = () => {
  const now = Date.now().toString(36).toUpperCase();
  const suffix = Math.floor(Math.random() * 10000)
    .toString()
    .padStart(4, "0");
  return `CFG-${now}-${suffix}`;
};

const safeObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value) ? value : {};

const parseSnapshot = (body = {}) => {
  const configuration = safeObject(body.configuration);
  const inline = safeObject(body.snapshot);
  return Object.keys(configuration).length ? configuration : inline;
};

const parseVersionSide = (record) =>
  normalizeSide(record?.fields?.[STUDIO_FIELDS.labelVersions.designSide]);

const getVersionSavedConfigFallback = (versionRecord) => {
  const side = parseVersionSide(versionRecord);
  if (side === "front") {
    return STUDIO_FIELD_FALLBACKS.labelVersions.savedConfigurationsFront;
  }
  if (side === "back") {
    return STUDIO_FIELD_FALLBACKS.labelVersions.savedConfigurationsBack;
  }
  return [];
};

const resolveSessionId = (...values) => {
  for (const value of values) {
    const sessionId = sanitizeText(value, 255);
    if (sessionId) return sessionId;
  }
  return null;
};

const normalizeWaxSelection = (value) => {
  const raw = sanitizeText(value, 120);
  if (!raw) return undefined;
  // Shopify/Zakeke often sends "Wax Sealed in <Color>", but Airtable select options
  // are stored as just the color label.
  return raw.replace(/^wax\s*sealed\s*in\s+/i, "").trim() || undefined;
};

export default withShopifyProxy(
  async (arg, { qs, isV2, method }) => {
    try {
      const body = (await parseBody(arg, method, isV2)) || {};
      assertPayloadSize(body);

      const customerRecordId = normalizeRecordId(
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
      if (!customerRecordId) {
        return sendJson(400, {
          ok: false,
          error: "missing_customer_record_id",
          message: "A valid Airtable customer record id is required.",
        });
      }

      const snapshot = parseSnapshot(body);
      const requestedSessionId = sanitizeText(
        firstNonEmpty(body.session_id, body.sessionId, snapshot.sessionId, qs.session_id),
        255
      );

      const existingRecordId = normalizeRecordId(
        firstNonEmpty(
          body.saved_configuration_record_id,
          body.savedConfigurationRecordId,
          body.saved_configuration_id,
          body.savedConfigurationId
        )
      );
      let existingRecord = null;
      if (existingRecordId) {
        existingRecord = await getRecordOrNull(
          STUDIO_TABLES.savedConfigurations,
          existingRecordId
        );
        if (!existingRecord) {
          return sendJson(404, {
            ok: false,
            error: "saved_configuration_not_found",
            message: "The supplied saved configuration record was not found.",
          });
        }
        const linkedCustomers = getLinkedIds(
          existingRecord,
          STUDIO_FIELDS.savedConfigurations.customer
        );
        if (!linkedCustomers.includes(customerRecordId)) {
          return sendJson(403, {
            ok: false,
            error: "saved_configuration_customer_mismatch",
            message: "The supplied saved configuration does not belong to this customer.",
          });
        }
      }

      const frontVersionId = normalizeRecordId(
        firstNonEmpty(
          body.label_front_version_id,
          body.labelFrontVersionId,
          body.front_label_version_record_id,
          body.frontLabelVersionRecordId
        )
      );
      const backVersionId = normalizeRecordId(
        firstNonEmpty(
          body.label_back_version_id,
          body.labelBackVersionId,
          body.back_label_version_record_id,
          body.backLabelVersionRecordId
        )
      );

      let frontVersionRecord = null;
      let backVersionRecord = null;
      if (frontVersionId) {
        frontVersionRecord = await getRecordOrNull(STUDIO_TABLES.labelVersions, frontVersionId);
        if (!frontVersionRecord) {
          return sendJson(404, {
            ok: false,
            error: "front_version_not_found",
            message: "Front label version was not found.",
          });
        }
        if (parseVersionSide(frontVersionRecord) !== "front") {
          return sendJson(400, {
            ok: false,
            error: "front_version_side_mismatch",
            message: "Front label version id does not reference a front-side label.",
          });
        }
      }
      if (backVersionId) {
        backVersionRecord = await getRecordOrNull(STUDIO_TABLES.labelVersions, backVersionId);
        if (!backVersionRecord) {
          return sendJson(404, {
            ok: false,
            error: "back_version_not_found",
            message: "Back label version was not found.",
          });
        }
        if (parseVersionSide(backVersionRecord) !== "back") {
          return sendJson(400, {
            ok: false,
            error: "back_version_side_mismatch",
            message: "Back label version id does not reference a back-side label.",
          });
        }
      }

      const frontLabelFromVersion = frontVersionRecord
        ? getLinkedIds(frontVersionRecord, STUDIO_FIELDS.labelVersions.labels)[0] || null
        : null;
      const backLabelFromVersion = backVersionRecord
        ? getLinkedIds(backVersionRecord, STUDIO_FIELDS.labelVersions.labels)[0] || null
        : null;

      const providedFrontLabelId = normalizeRecordId(
        firstNonEmpty(
          body.label_front_record_id,
          body.labelFrontRecordId,
          body.front_label_record_id
        )
      );
      const providedBackLabelId = normalizeRecordId(
        firstNonEmpty(
          body.label_back_record_id,
          body.labelBackRecordId,
          body.back_label_record_id
        )
      );

      const frontLabelId = providedFrontLabelId || frontLabelFromVersion;
      const backLabelId = providedBackLabelId || backLabelFromVersion;

      if (
        frontLabelId &&
        frontLabelFromVersion &&
        frontLabelId !== frontLabelFromVersion
      ) {
        return sendJson(400, {
          ok: false,
          error: "front_label_version_mismatch",
          message: "Front label/version linkage is invalid.",
        });
      }
      if (backLabelId && backLabelFromVersion && backLabelId !== backLabelFromVersion) {
        return sendJson(400, {
          ok: false,
          error: "back_label_version_mismatch",
          message: "Back label/version linkage is invalid.",
        });
      }

      const ensureLabelOwnedByCustomer = async (labelId, side) => {
        if (!labelId) return;
        const labelRecord = await getRecordOrNull(STUDIO_TABLES.labels, labelId);
        if (!labelRecord) {
          return sendJson(404, {
            ok: false,
            error: `${side}_label_not_found`,
            message: `${side === "front" ? "Front" : "Back"} label record was not found.`,
          });
        }
        const linkedCustomers = getLinkedIds(labelRecord, STUDIO_FIELDS.labels.customers);
        if (!linkedCustomers.includes(customerRecordId)) {
          return sendJson(403, {
            ok: false,
            error: `${side}_label_customer_mismatch`,
            message: `${side === "front" ? "Front" : "Back"} label does not belong to this customer.`,
          });
        }
        return null;
      };
      const frontLabelErr = await ensureLabelOwnedByCustomer(frontLabelId, "front");
      if (frontLabelErr) return frontLabelErr;
      const backLabelErr = await ensureLabelOwnedByCustomer(backLabelId, "back");
      if (backLabelErr) return backLabelErr;

      const frontLabelRecord = frontLabelId
        ? await getRecordOrNull(STUDIO_TABLES.labels, frontLabelId)
        : null;
      const backLabelRecord = backLabelId
        ? await getRecordOrNull(STUDIO_TABLES.labels, backLabelId)
        : null;

      const sessionId = resolveSessionId(
        requestedSessionId,
        existingRecord?.fields?.[STUDIO_FIELDS.savedConfigurations.sessionId],
        frontLabelRecord?.fields?.[STUDIO_FIELDS.labels.sessionId],
        backLabelRecord?.fields?.[STUDIO_FIELDS.labels.sessionId],
        frontVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.sessionId],
        backVersionRecord?.fields?.[STUDIO_FIELDS.labelVersions.sessionId]
      );

      const previewUrl = sanitizeUrl(
        firstNonEmpty(
          body.preview_url,
          body.previewUrl,
          snapshot.previewImage,
          snapshot.preview_url,
          snapshot.preview
        )
      );

      const displayName =
        sanitizeName(firstNonEmpty(body.display_name, body.displayName)) ||
        sanitizeName(`${snapshot?.bottle?.name || "Custom"} Configuration`);

      const status =
        normalizeStatus(firstNonEmpty(body.status, body.configuration_status)) ||
        "Saved";

      const configurationId =
        sanitizeText(firstNonEmpty(body.configuration_id, body.configurationId), 80) ||
        existingRecord?.fields?.[STUDIO_FIELDS.savedConfigurations.configurationId] ||
        defaultConfigurationId();

      const configJson = sanitizeText(JSON.stringify(snapshot || {}), 100_000);

      const sharedRequiredFields = {
        [STUDIO_FIELDS.savedConfigurations.customer]: toLinkedRecordArray(
          customerRecordId
        ),
        [STUDIO_FIELDS.savedConfigurations.configuratorTool]: "Zakeke",
      };

      const optionalFields = {
        [STUDIO_FIELDS.savedConfigurations.configurationId]: configurationId,
        [STUDIO_FIELDS.savedConfigurations.displayName]: displayName,
        [STUDIO_FIELDS.savedConfigurations.status]: status,
        [STUDIO_FIELDS.savedConfigurations.sessionId]: sessionId || undefined,
        [STUDIO_FIELDS.savedConfigurations.previewImageUrl]: previewUrl,
        [STUDIO_FIELDS.savedConfigurations.internalSku]: sanitizeText(
          firstNonEmpty(
            body.internal_sku,
            body.internalSku,
            snapshot.internalSKU,
            snapshot.internal_sku
          ),
          255
        ),
        [STUDIO_FIELDS.savedConfigurations.shopifyVariantId]: sanitizeText(
          firstNonEmpty(
            body.shopify_variant_id,
            body.shopifyVariantId,
            snapshot.shopifyVariantId
          ),
          255
        ),
        [STUDIO_FIELDS.savedConfigurations.alcoholSelection]: sanitizeText(
          firstNonEmpty(body.alcohol_selection, body.alcoholSelection, snapshot.liquor),
          120
        ),
        [STUDIO_FIELDS.savedConfigurations.bottleSelection]: sanitizeText(
          firstNonEmpty(body.bottle_selection, body.bottleSelection, snapshot?.bottle?.name),
          120
        ),
        [STUDIO_FIELDS.savedConfigurations.liquidSelection]: sanitizeText(
          firstNonEmpty(body.liquid_selection, body.liquidSelection, snapshot?.liquid?.name),
          120
        ),
        [STUDIO_FIELDS.savedConfigurations.closureSelection]: sanitizeText(
          firstNonEmpty(body.closure_selection, body.closureSelection, snapshot?.closure?.name),
          120
        ),
        [STUDIO_FIELDS.savedConfigurations.waxSelection]: normalizeWaxSelection(
          firstNonEmpty(
            body.wax_selection,
            body.waxSelection,
            snapshot?.closureExtras?.wax?.name
          )
        ),
        [STUDIO_FIELDS.savedConfigurations.configJson]: configJson,
        [STUDIO_FIELDS.savedConfigurations.creationSource]:
          "Shopify -> Netlify Backend (studio-save-configuration)",
        [STUDIO_FIELDS.savedConfigurations.labels]: toLinkedRecordArray(
          frontLabelId,
          backLabelId
        ),
        ...buildLinkedPatch(
          existingRecord,
          STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion,
          frontVersionId,
          {
            fallbackFieldNames:
              STUDIO_FIELD_FALLBACKS.savedConfigurations.currentFrontLabelVersion,
          }
        ),
        ...buildLinkedPatch(
          existingRecord,
          STUDIO_FIELDS.savedConfigurations.currentBackLabelVersion,
          backVersionId,
          {
            fallbackFieldNames:
              STUDIO_FIELD_FALLBACKS.savedConfigurations.currentBackLabelVersion,
          }
        ),
      };

      let record;
      if (existingRecordId) {
        record = await updateResilient(
          STUDIO_TABLES.savedConfigurations,
          existingRecordId,
          sharedRequiredFields,
          optionalFields
        );
      } else {
        record = await createResilient(
          STUDIO_TABLES.savedConfigurations,
          sharedRequiredFields,
          optionalFields
        );
      }

      const linkConfigToLabel = async (labelRecordId, currentVersionRecordId, side) => {
        if (!labelRecordId) return;
        const labelRecord =
          (frontLabelRecord && frontLabelRecord.id === labelRecordId && frontLabelRecord) ||
          (backLabelRecord && backLabelRecord.id === labelRecordId && backLabelRecord) ||
          (await getRecordOrNull(STUDIO_TABLES.labels, labelRecordId));
        if (!labelRecord) return;

        const mergedConfigIds = Array.from(
          new Set([
            ...getLinkedIds(labelRecord, STUDIO_FIELDS.labels.savedConfigurations),
            record.id,
          ])
        );
        await updateResilient(
          STUDIO_TABLES.labels,
          labelRecord.id,
          {},
          {
            [STUDIO_FIELDS.labels.savedConfigurations]:
              mergedConfigIds.length ? mergedConfigIds : undefined,
            ...(side === "front"
              ? buildLinkedPatch(
                  labelRecord,
                  STUDIO_FIELDS.labels.currentFrontLabelVersion,
                  currentVersionRecordId,
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
                  currentVersionRecordId,
                  {
                    fallbackFieldNames:
                      STUDIO_FIELD_FALLBACKS.labels.currentBackLabelVersion,
                  }
                )
              : {}),
            [STUDIO_FIELDS.labels.sessionId]: sessionId || undefined,
          }
        );
      };

      const linkVersionToSavedConfiguration = async (
        versionRecordId,
        sideLabelRecord
      ) => {
        if (!versionRecordId) return;
        const versionRecord = await getRecordOrNull(
          STUDIO_TABLES.labelVersions,
          versionRecordId
        );
        if (!versionRecord) return;
        const fallbackFieldNames = getVersionSavedConfigFallback(versionRecord);
        const mergedConfigIds = Array.from(
          new Set([
            ...getLinkedIds(
              versionRecord,
              STUDIO_FIELDS.labelVersions.savedConfigurations,
              fallbackFieldNames
            ),
            record.id,
          ])
        );
        const versionSessionId = resolveSessionId(
          sessionId,
          versionRecord.fields?.[STUDIO_FIELDS.labelVersions.sessionId],
          sideLabelRecord?.fields?.[STUDIO_FIELDS.labels.sessionId]
        );
        await updateResilient(
          STUDIO_TABLES.labelVersions,
          versionRecord.id,
          {},
          {
            ...buildLinkedPatch(
              versionRecord,
              STUDIO_FIELDS.labelVersions.savedConfigurations,
              mergedConfigIds,
              {
                fallbackFieldNames,
              }
            ),
            [STUDIO_FIELDS.labelVersions.sessionId]: versionSessionId || undefined,
          }
        );
      };

      await linkConfigToLabel(frontLabelId, frontVersionId, "front");
      await linkConfigToLabel(backLabelId, backVersionId, "back");
      await linkVersionToSavedConfiguration(frontVersionId, frontLabelRecord);
      await linkVersionToSavedConfiguration(backVersionId, backLabelRecord);

      return sendJson(200, {
        ok: true,
        saved_configuration_record_id: record.id,
        configuration_id:
          record.fields?.[STUDIO_FIELDS.savedConfigurations.configurationId] || null,
        customer_record_id: customerRecordId,
        session_id: sessionId || null,
        label_front_record_id: frontLabelId || null,
        label_back_record_id: backLabelId || null,
        label_front_version_id: frontVersionId || null,
        label_back_version_id: backVersionId || null,
        created_at: record.createdTime || null,
      });
    } catch (error) {
      return sendJson(error?.status || 500, mapErrorResponse(error));
    }
  },
  {
    methods: ["POST", "PATCH"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
