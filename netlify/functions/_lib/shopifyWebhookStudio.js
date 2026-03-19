import { findOneBy, escapeFormulaValue } from "../../../src/lib/airtable.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  STUDIO_FIELD_FALLBACKS,
  normalizeRecordId,
  getLinkedIds,
  buildLinkedPatch,
  createResilient,
  updateResilient,
  getRecordOrNull,
  listAllRecords,
  buildLinkedCustomerFormula,
  toLinkedRecordArray,
} from "./studio.js";

const normalizeText = (value, maxLen = 255) => {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.slice(0, maxLen);
};

export const normalizeEmail = (value) => {
  const text = normalizeText(value, 255);
  if (!text || !text.includes("@")) return null;
  return text.toLowerCase();
};

export const normalizeShopifyCustomerId = (value) => {
  const text = normalizeText(value, 255);
  if (!text) return null;
  if (text.startsWith("rec")) return null;
  return text;
};

const normalizePhone = (value) => normalizeText(value, 60);

const normalizeName = (value) => normalizeText(value, 120);

const normalizeLineItemPropertyKey = (value) => normalizeText(value, 120);

const normalizeLineItemPropertyValue = (value) => normalizeText(value, 1000);

const parseNoteAttributes = (order) => {
  const out = {};
  const attrs = Array.isArray(order?.note_attributes) ? order.note_attributes : [];
  for (const entry of attrs) {
    const key = normalizeLineItemPropertyKey(entry?.name ?? entry?.key);
    if (!key) continue;
    const value = normalizeLineItemPropertyValue(entry?.value);
    if (value == null) continue;
    out[key] = value;
    const lower = key.toLowerCase();
    if (!(lower in out)) out[lower] = value;
  }
  return out;
};

const parseLineItemProperties = (lineItem, fallbackProperties = {}) => {
  const out = { ...(fallbackProperties || {}) };
  const props = Array.isArray(lineItem?.properties) ? lineItem.properties : [];
  for (const pair of props) {
    const key = normalizeLineItemPropertyKey(pair?.name ?? pair?.key);
    if (!key) continue;
    const value = normalizeLineItemPropertyValue(pair?.value);
    if (value == null) continue;
    out[key] = value;
    const lower = key.toLowerCase();
    if (!(lower in out)) out[lower] = value;
  }
  return out;
};

const readProperty = (properties, ...keys) => {
  for (const key of keys) {
    const direct = properties?.[key];
    if (direct != null && String(direct).trim()) return String(direct).trim();
    const lower = properties?.[String(key || "").toLowerCase()];
    if (lower != null && String(lower).trim()) return String(lower).trim();
  }
  return null;
};

const uniqueRecordIds = (values = []) => {
  const out = [];
  for (const value of values || []) {
    const recId = normalizeRecordId(value);
    if (recId && !out.includes(recId)) out.push(recId);
  }
  return out;
};

const uniqueTextValues = (values = [], maxLen = 255) => {
  const out = [];
  for (const value of values || []) {
    const text = normalizeText(value, maxLen);
    if (text && !out.includes(text)) out.push(text);
  }
  return out;
};

export const normalizeOrderWebhookPayload = (payload, envelope = {}) => {
  const noteFallback = parseNoteAttributes(payload);
  const rawLineItems = Array.isArray(payload?.line_items) ? payload.line_items : [];
  const lineItems = rawLineItems.length ? rawLineItems : [{}];

  const normalizedLineItems = lineItems.map((lineItem) => {
    const mergedProperties = parseLineItemProperties(lineItem, noteFallback);
    return {
      id: lineItem?.id ?? null,
      variant_id: lineItem?.variant_id ?? null,
      quantity: Number(lineItem?.quantity || 0) || 0,
      properties: {
        _saved_configuration_id: normalizeRecordId(
          readProperty(
            mergedProperties,
            "_saved_configuration_id",
            "saved_configuration_id",
            "Config ID"
          )
        ),
        _session_id: normalizeText(
          readProperty(mergedProperties, "_session_id", "session_id"),
          255
        ),
        _label_front_version_id: normalizeRecordId(
          readProperty(
            mergedProperties,
            "_label_front_version_id",
            "label_front_version_id"
          )
        ),
        _label_back_version_id: normalizeRecordId(
          readProperty(
            mergedProperties,
            "_label_back_version_id",
            "label_back_version_id"
          )
        ),
        _ss_customer_airtable_id: normalizeRecordId(
          readProperty(
            mergedProperties,
            "_ss_customer_airtable_id",
            "ss_customer_airtable_id"
          )
        ),
      },
    };
  });

  const customer = payload?.customer || {};
  const billing = payload?.billing_address || {};
  const shipping = payload?.shipping_address || {};

  const customerShopifyId = normalizeShopifyCustomerId(
    customer?.admin_graphql_api_id ?? customer?.id
  );

  return {
    webhook_id: envelope?.webhook_id || null,
    topic: envelope?.topic || null,
    shop_domain: envelope?.shop_domain || null,
    received_at: envelope?.received_at || new Date().toISOString(),
    order: {
      id: payload?.id ?? null,
      name: normalizeText(payload?.name, 120),
      created_at: payload?.created_at || null,
      updated_at: payload?.updated_at || null,
      email: normalizeEmail(payload?.email ?? payload?.contact_email),
      customer: {
        shopify_id: customerShopifyId,
        email: normalizeEmail(customer?.email ?? payload?.email ?? payload?.contact_email),
        first_name: normalizeName(
          customer?.first_name ?? billing?.first_name ?? shipping?.first_name
        ),
        last_name: normalizeName(
          customer?.last_name ?? billing?.last_name ?? shipping?.last_name
        ),
        phone: normalizePhone(customer?.phone ?? billing?.phone ?? shipping?.phone),
      },
      line_items: normalizedLineItems,
    },
  };
};

export const normalizeCustomerWebhookPayload = (payload, envelope = {}) => {
  const customerShopifyId = normalizeShopifyCustomerId(
    payload?.admin_graphql_api_id ?? payload?.id
  );

  return {
    webhook_id: envelope?.webhook_id || null,
    topic: envelope?.topic || null,
    shop_domain: envelope?.shop_domain || null,
    received_at: envelope?.received_at || new Date().toISOString(),
    customer: {
      shopify_id: customerShopifyId,
      email: normalizeEmail(payload?.email),
      first_name: normalizeName(payload?.first_name),
      last_name: normalizeName(payload?.last_name),
      phone: normalizePhone(payload?.phone),
      created_at: payload?.created_at || null,
      updated_at: payload?.updated_at || null,
      verified_email:
        typeof payload?.verified_email === "boolean" ? payload.verified_email : null,
      state: normalizeText(payload?.state, 120),
      default_address:
        payload?.default_address && typeof payload.default_address === "object"
          ? payload.default_address
          : null,
    },
  };
};

const parseVersionSide = (record) => {
  const side = String(
    record?.fields?.[STUDIO_FIELDS.labelVersions.designSide] || ""
  )
    .trim()
    .toLowerCase();
  if (side === "front" || side === "back") return side;
  return null;
};

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

const listGuestCustomersByEmail = async (email) => {
  const normalized = normalizeEmail(email);
  if (!normalized) return [];

  const safeEmail = escapeFormulaValue(normalized);
  const formula = `AND(LOWER({Email})='${safeEmail}', OR({Shopify ID}=BLANK(), {Shopify ID}=''))`;

  try {
    const rows = await listAllRecords(STUDIO_TABLES.customers, {
      filterByFormula: formula,
    });
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
};

const listSavedConfigurationsBySessionId = async (sessionId) => {
  const normalized = normalizeText(sessionId, 255);
  if (!normalized) return [];
  const safeSession = escapeFormulaValue(normalized);

  try {
    const rows = await listAllRecords(STUDIO_TABLES.savedConfigurations, {
      filterByFormula: `{${STUDIO_FIELDS.savedConfigurations.sessionId}}='${safeSession}'`,
    });
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
};

export const upsertCanonicalCustomer = async ({
  shopifyId,
  email,
  firstName,
  lastName,
  phone,
  shopDomain,
  creationSource = "Shopify webhook",
}) => {
  const normalizedShopifyId = normalizeShopifyCustomerId(shopifyId);
  const normalizedEmail = normalizeEmail(email);
  const normalizedFirstName = normalizeName(firstName);
  const normalizedLastName = normalizeName(lastName);
  const normalizedPhone = normalizePhone(phone);

  if (!normalizedShopifyId && !normalizedEmail) {
    return {
      customerRecordId: null,
      created: false,
      matchedBy: null,
      updated: false,
    };
  }

  let existing = null;
  let matchedBy = null;

  if (normalizedShopifyId) {
    existing = await findOneBy(
      STUDIO_TABLES.customers,
      "Shopify ID",
      normalizedShopifyId
    );
    if (existing?.id) matchedBy = "shopify_id";
  }

  if (!existing?.id && normalizedEmail) {
    existing = await findOneBy(STUDIO_TABLES.customers, "Email", normalizedEmail);
    if (existing?.id) matchedBy = "email";
  }

  const canonicalFields = {
    "Shopify ID": normalizedShopifyId || undefined,
    Email: normalizedEmail || undefined,
    "First Name": normalizedFirstName || undefined,
    "Last Name": normalizedLastName || undefined,
    Phone: normalizedPhone || undefined,
    Source: "Shopify",
    "Shop Domain": normalizeText(shopDomain, 255) || undefined,
    "Creation Source": normalizeText(creationSource, 255) || undefined,
  };

  if (existing?.id) {
    const updates = {};
    const currentFields = existing?.fields || {};

    if (normalizedShopifyId && currentFields["Shopify ID"] !== normalizedShopifyId) {
      updates["Shopify ID"] = normalizedShopifyId;
    }
    if (normalizedEmail && currentFields.Email !== normalizedEmail) {
      updates.Email = normalizedEmail;
    }
    if (
      normalizedFirstName &&
      currentFields["First Name"] !== normalizedFirstName
    ) {
      updates["First Name"] = normalizedFirstName;
    }
    if (normalizedLastName && currentFields["Last Name"] !== normalizedLastName) {
      updates["Last Name"] = normalizedLastName;
    }
    if (normalizedPhone && currentFields.Phone !== normalizedPhone) {
      updates.Phone = normalizedPhone;
    }
    if (currentFields.Source !== "Shopify") {
      updates.Source = "Shopify";
    }
    const normalizedShopDomain = normalizeText(shopDomain, 255);
    if (normalizedShopDomain && currentFields["Shop Domain"] !== normalizedShopDomain) {
      updates["Shop Domain"] = normalizedShopDomain;
    }

    const updated =
      Object.keys(updates).length > 0
        ? await updateResilient(STUDIO_TABLES.customers, existing.id, {}, updates)
        : existing;

    return {
      customerRecordId: updated?.id || existing.id,
      created: false,
      matchedBy,
      updated: Object.keys(updates).length > 0,
      record: updated || existing,
    };
  }

  const created = await createResilient(STUDIO_TABLES.customers, {}, canonicalFields);

  return {
    customerRecordId: created?.id || null,
    created: true,
    matchedBy: normalizedShopifyId ? "shopify_id" : "email",
    updated: false,
    record: created,
  };
};

const uniqueLinkedIds = (raw) => {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const value of raw) {
    const recId = normalizeRecordId(value);
    if (recId && !out.includes(recId)) out.push(recId);
  }
  return out;
};

const replaceLinkedCustomerId = ({ linkedIds, fromCustomerId, toCustomerId }) => {
  const out = [];
  for (const entry of linkedIds || []) {
    const next = entry === fromCustomerId ? toCustomerId : entry;
    if (next && !out.includes(next)) out.push(next);
  }
  return out;
};

const relinkTableCustomerField = async ({
  table,
  fieldName,
  fromCustomerId,
  toCustomerId,
}) => {
  if (!fromCustomerId || !toCustomerId || fromCustomerId === toCustomerId) {
    return 0;
  }

  let records = [];
  try {
    records = await listAllRecords(table, {
      filterByFormula: buildLinkedCustomerFormula(fieldName, fromCustomerId),
    });
  } catch {
    return 0;
  }

  let touched = 0;
  for (const record of records || []) {
    const linkedIds = uniqueLinkedIds(record?.fields?.[fieldName]);
    if (!linkedIds.includes(fromCustomerId)) continue;

    const nextIds = replaceLinkedCustomerId({
      linkedIds,
      fromCustomerId,
      toCustomerId,
    });

    const unchanged =
      nextIds.length === linkedIds.length &&
      nextIds.every((entry, idx) => entry === linkedIds[idx]);
    if (unchanged) continue;

    await updateResilient(table, record.id, {}, { [fieldName]: nextIds });
    touched += 1;
  }

  return touched;
};

const markGuestCustomerAsMerged = async ({
  guestCustomerRecordId,
  canonicalCustomerRecordId,
}) => {
  if (!guestCustomerRecordId || !canonicalCustomerRecordId) return;

  const mergedAtIso = new Date().toISOString();
  const mergedInto = toLinkedRecordArray(canonicalCustomerRecordId);
  const updated = await updateResilient(STUDIO_TABLES.customers, guestCustomerRecordId, {}, {
    "Merge Status": "Merged",
    "Merged Into Customer": mergedInto,
    "Merged At": mergedAtIso,
  });

  const persistedStatus = String(updated?.fields?.["Merge Status"] || "")
    .trim()
    .toLowerCase();
  const persistedMergedInto = Array.isArray(updated?.fields?.["Merged Into Customer"])
    ? updated.fields["Merged Into Customer"].map((entry) => normalizeRecordId(entry))
    : [];
  const persistedMergedAt = String(updated?.fields?.["Merged At"] || "").trim();

  const isMarkedMerged =
    persistedStatus === "merged" &&
    Boolean(mergedInto?.[0]) &&
    persistedMergedInto.includes(mergedInto[0]) &&
    Boolean(persistedMergedAt);

  if (!isMarkedMerged) {
    const err = new Error(
      "Customers merge marker fields were not persisted. Ensure 'Merge Status', 'Merged Into Customer', and 'Merged At' exist and are writable."
    );
    err.code = "merge_marker_not_persisted";
    err.guestCustomerRecordId = guestCustomerRecordId;
    throw err;
  }
};

export const mergeGuestCustomersIntoCanonical = async ({
  canonicalCustomerRecordId,
  guestCustomerRecordIds = [],
}) => {
  const canonical = normalizeRecordId(canonicalCustomerRecordId);
  if (!canonical) {
    return {
      mergedPairs: [],
      relinkedSavedConfigurations: 0,
      relinkedLabels: 0,
      relinkedOrders: 0,
    };
  }

  const candidates = uniqueRecordIds(guestCustomerRecordIds).filter(
    (recordId) => recordId !== canonical
  );
  if (!candidates.length) {
    return {
      mergedPairs: [],
      relinkedSavedConfigurations: 0,
      relinkedLabels: 0,
      relinkedOrders: 0,
    };
  }

  let relinkedSavedConfigurations = 0;
  let relinkedLabels = 0;
  let relinkedOrders = 0;
  const mergedPairs = [];

  for (const guestId of candidates) {
    relinkedSavedConfigurations += await relinkTableCustomerField({
      table: STUDIO_TABLES.savedConfigurations,
      fieldName: STUDIO_FIELDS.savedConfigurations.customer,
      fromCustomerId: guestId,
      toCustomerId: canonical,
    });

    relinkedLabels += await relinkTableCustomerField({
      table: STUDIO_TABLES.labels,
      fieldName: STUDIO_FIELDS.labels.customers,
      fromCustomerId: guestId,
      toCustomerId: canonical,
    });

    relinkedOrders += await relinkTableCustomerField({
      table: STUDIO_TABLES.orders,
      fieldName: STUDIO_FIELDS.orders.customer,
      fromCustomerId: guestId,
      toCustomerId: canonical,
    });

    await markGuestCustomerAsMerged({
      guestCustomerRecordId: guestId,
      canonicalCustomerRecordId: canonical,
    });

    mergedPairs.push(`${guestId}->${canonical}`);
  }

  return {
    mergedPairs,
    relinkedSavedConfigurations,
    relinkedLabels,
    relinkedOrders,
  };
};

const ensureSavedConfigSignalsEntry = (map, savedConfigurationRecordId) => {
  const key = normalizeRecordId(savedConfigurationRecordId);
  if (!key) return null;

  if (!map.has(key)) {
    map.set(key, {
      saved_configuration_id: key,
      front_version_ids: new Set(),
      back_version_ids: new Set(),
      session_ids: new Set(),
    });
  }
  return map.get(key);
};

const addSignalVersionIds = (entry, signal) => {
  if (!entry || !signal) return;
  const frontVersionId = normalizeRecordId(signal?._label_front_version_id);
  const backVersionId = normalizeRecordId(signal?._label_back_version_id);
  const sessionId = normalizeText(signal?._session_id, 255);

  if (frontVersionId) entry.front_version_ids.add(frontVersionId);
  if (backVersionId) entry.back_version_ids.add(backVersionId);
  if (sessionId) entry.session_ids.add(sessionId);
};

export const collectOrderSignals = async (orderPayload) => {
  const normalizedOrder = normalizeOrderWebhookPayload(orderPayload || {});
  const signals = Array.isArray(normalizedOrder?.order?.line_items)
    ? normalizedOrder.order.line_items
    : [];

  const explicitCustomerRecordIds = uniqueRecordIds(
    signals.map((line) => line?.properties?._ss_customer_airtable_id)
  );

  const explicitSavedConfigIds = uniqueRecordIds(
    signals.map((line) => line?.properties?._saved_configuration_id)
  );

  const sessionIds = uniqueTextValues(
    signals.map((line) => line?.properties?._session_id),
    255
  );

  const candidateCustomerIds = new Set(explicitCustomerRecordIds);
  const savedConfigSignals = new Map();
  const sessionSignalMap = new Map();

  signals.forEach((line) => {
    const properties = line?.properties || {};
    const savedConfigId = normalizeRecordId(properties._saved_configuration_id);
    const sessionId = normalizeText(properties._session_id, 255);

    if (savedConfigId) {
      const entry = ensureSavedConfigSignalsEntry(savedConfigSignals, savedConfigId);
      addSignalVersionIds(entry, properties);
    }

    if (sessionId) {
      if (!sessionSignalMap.has(sessionId)) {
        sessionSignalMap.set(sessionId, {
          _session_id: sessionId,
          _label_front_version_id: null,
          _label_back_version_id: null,
        });
      }
      const entry = sessionSignalMap.get(sessionId);
      if (!entry._label_front_version_id) {
        entry._label_front_version_id =
          normalizeRecordId(properties._label_front_version_id) || null;
      }
      if (!entry._label_back_version_id) {
        entry._label_back_version_id =
          normalizeRecordId(properties._label_back_version_id) || null;
      }
    }
  });

  for (const savedConfigId of explicitSavedConfigIds) {
    const savedConfigRecord = await getRecordOrNull(
      STUDIO_TABLES.savedConfigurations,
      savedConfigId
    );
    if (!savedConfigRecord) continue;

    getLinkedIds(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.customer).forEach((id) => {
      candidateCustomerIds.add(id);
    });
  }

  for (const sessionId of sessionIds) {
    const matches = await listSavedConfigurationsBySessionId(sessionId);
    for (const savedConfigRecord of matches) {
      const entry = ensureSavedConfigSignalsEntry(savedConfigSignals, savedConfigRecord?.id);
      addSignalVersionIds(entry, sessionSignalMap.get(sessionId));

      getLinkedIds(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.customer).forEach((id) => {
        candidateCustomerIds.add(id);
      });
    }
  }

  const guestEmailMatches = await listGuestCustomersByEmail(
    normalizedOrder?.order?.email || normalizedOrder?.order?.customer?.email
  );
  for (const guestRecord of guestEmailMatches) {
    if (guestRecord?.id) candidateCustomerIds.add(guestRecord.id);
  }

  return {
    candidateCustomerRecordIds: uniqueRecordIds(Array.from(candidateCustomerIds)),
    savedConfigurationSignals: Array.from(savedConfigSignals.values()).map((entry) => ({
      saved_configuration_id: entry.saved_configuration_id,
      front_version_ids: uniqueRecordIds(Array.from(entry.front_version_ids)),
      back_version_ids: uniqueRecordIds(Array.from(entry.back_version_ids)),
      session_ids: uniqueTextValues(Array.from(entry.session_ids), 255),
    })),
  };
};

export const collectCustomerGuestCandidatesByEmail = async ({ email }) => {
  const rows = await listGuestCustomersByEmail(email);
  return uniqueRecordIds(rows.map((row) => row?.id));
};

const mergeVersionIdsFromLabels = async (labelRecordIds = []) => {
  const versionIds = new Set();
  const normalizedLabelIds = uniqueRecordIds(labelRecordIds);

  for (const labelId of normalizedLabelIds) {
    const labelRecord = await getRecordOrNull(STUDIO_TABLES.labels, labelId);
    if (!labelRecord) continue;

    getLinkedIds(labelRecord, STUDIO_FIELDS.labels.labelVersions).forEach((versionId) => {
      const recId = normalizeRecordId(versionId);
      if (recId) versionIds.add(recId);
    });
  }

  return uniqueRecordIds(Array.from(versionIds));
};

const ensureLabelVersionHasSavedConfiguration = async ({
  labelVersionRecordId,
  savedConfigurationRecordId,
}) => {
  const labelVersionId = normalizeRecordId(labelVersionRecordId);
  const savedConfigId = normalizeRecordId(savedConfigurationRecordId);
  if (!labelVersionId || !savedConfigId) return false;

  const versionRecord = await getRecordOrNull(STUDIO_TABLES.labelVersions, labelVersionId);
  if (!versionRecord) return false;

  const fallbackFieldNames = getVersionSavedConfigFallback(versionRecord);
  const linkedConfigIds = getLinkedIds(
    versionRecord,
    STUDIO_FIELDS.labelVersions.savedConfigurations,
    fallbackFieldNames
  );
  if (linkedConfigIds.includes(savedConfigId)) return false;

  const nextConfigIds = uniqueRecordIds([...linkedConfigIds, savedConfigId]);
  await updateResilient(STUDIO_TABLES.labelVersions, versionRecord.id, {}, {
    [STUDIO_FIELDS.labelVersions.savedConfigurations]: nextConfigIds,
  });
  return true;
};

export const ensureSavedConfigurationOrderLinkage = async ({
  savedConfigurationRecordId,
  canonicalCustomerRecordId,
  orderRecordId,
  orderStatus = "Order Received",
  preferredFrontVersionId,
  preferredBackVersionId,
}) => {
  const savedConfigId = normalizeRecordId(savedConfigurationRecordId);
  if (!savedConfigId) {
    return {
      updated: false,
      relinkedLabelVersions: 0,
      linkedCustomerIds: [],
    };
  }

  const savedConfigRecord = await getRecordOrNull(
    STUDIO_TABLES.savedConfigurations,
    savedConfigId
  );
  if (!savedConfigRecord) {
    return {
      updated: false,
      relinkedLabelVersions: 0,
      linkedCustomerIds: [],
    };
  }

  const linkedCustomerIds = getLinkedIds(
    savedConfigRecord,
    STUDIO_FIELDS.savedConfigurations.customer
  );
  const canonicalCustomerId = normalizeRecordId(canonicalCustomerRecordId);
  const effectiveCustomerIds = canonicalCustomerId
    ? [canonicalCustomerId]
    : linkedCustomerIds;

  const labelIds = getLinkedIds(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.labels);
  const existingVersionIds = getLinkedIds(
    savedConfigRecord,
    STUDIO_FIELDS.savedConfigurations.labelVersions
  );

  const mergedVersionIds = new Set(existingVersionIds);
  const frontVersionId = normalizeRecordId(preferredFrontVersionId);
  const backVersionId = normalizeRecordId(preferredBackVersionId);
  if (frontVersionId) mergedVersionIds.add(frontVersionId);
  if (backVersionId) mergedVersionIds.add(backVersionId);

  const labelVersionIdsFromLabels = await mergeVersionIdsFromLabels(labelIds);
  labelVersionIdsFromLabels.forEach((id) => mergedVersionIds.add(id));

  await updateResilient(STUDIO_TABLES.savedConfigurations, savedConfigId, {}, {
    ...(canonicalCustomerId
      ? {
          [STUDIO_FIELDS.savedConfigurations.customer]: effectiveCustomerIds,
        }
      : {}),
    [STUDIO_FIELDS.savedConfigurations.status]: orderStatus,
    [STUDIO_FIELDS.savedConfigurations.order]: normalizeRecordId(orderRecordId)
      ? [normalizeRecordId(orderRecordId)]
      : undefined,
    [STUDIO_FIELDS.savedConfigurations.labelVersions]: uniqueRecordIds(
      Array.from(mergedVersionIds)
    ),
    ...buildLinkedPatch(
      savedConfigRecord,
      STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion,
      frontVersionId,
      {
        fallbackFieldNames:
          STUDIO_FIELD_FALLBACKS.savedConfigurations.currentFrontLabelVersion,
      }
    ),
    ...buildLinkedPatch(
      savedConfigRecord,
      STUDIO_FIELDS.savedConfigurations.currentBackLabelVersion,
      backVersionId,
      {
        fallbackFieldNames:
          STUDIO_FIELD_FALLBACKS.savedConfigurations.currentBackLabelVersion,
      }
    ),
  });

  let relinkedLabelVersions = 0;
  for (const versionId of uniqueRecordIds(Array.from(mergedVersionIds))) {
    const changed = await ensureLabelVersionHasSavedConfiguration({
      labelVersionRecordId: versionId,
      savedConfigurationRecordId: savedConfigId,
    });
    if (changed) relinkedLabelVersions += 1;
  }

  return {
    updated: true,
    relinkedLabelVersions,
    linkedCustomerIds: effectiveCustomerIds,
  };
};

export const createOrderRecordForSavedConfiguration = async ({
  orderId,
  orderStatus,
  savedConfigurationRecordId,
  customerRecordIds = [],
}) => {
  const savedConfigId = normalizeRecordId(savedConfigurationRecordId);
  if (!savedConfigId) return null;

  const linkedCustomerIds = uniqueRecordIds(customerRecordIds);
  const normalizedOrderId = normalizeText(orderId, 255);
  const safeSavedConfigId = escapeFormulaValue(savedConfigId);

  const formulaParts = [
    `FIND('${safeSavedConfigId}', ARRAYJOIN({${STUDIO_FIELDS.orders.savedConfiguration}}))`,
  ];
  if (normalizedOrderId) {
    formulaParts.unshift(
      `{${STUDIO_FIELDS.orders.orderId}}='${escapeFormulaValue(normalizedOrderId)}'`
    );
  }

  const existingOrderRecords = await listAllRecords(STUDIO_TABLES.orders, {
    filterByFormula:
      formulaParts.length > 1 ? `AND(${formulaParts.join(",")})` : formulaParts[0],
    maxRecords: 1,
  });

  const fields = {
    [STUDIO_FIELDS.orders.orderId]: normalizedOrderId || undefined,
    [STUDIO_FIELDS.orders.customer]:
      linkedCustomerIds.length > 0 ? linkedCustomerIds : undefined,
    [STUDIO_FIELDS.orders.savedConfiguration]: [savedConfigId],
    [STUDIO_FIELDS.orders.orderStatus]:
      normalizeText(orderStatus, 120) || "Order Received",
  };

  const existingOrderRecord = Array.isArray(existingOrderRecords)
    ? existingOrderRecords[0] || null
    : null;

  if (existingOrderRecord?.id) {
    return updateResilient(STUDIO_TABLES.orders, existingOrderRecord.id, {}, fields);
  }

  return createResilient(STUDIO_TABLES.orders, {}, fields);
};
