import {
  createOne,
  updateOne,
  deleteOne,
  getOne,
  listRecords,
  findOneBy,
  escapeFormulaValue,
} from "../../../src/lib/airtable.js";

export const STUDIO_TABLES = {
  customers: process.env.AIRTABLE_CUSTOMERS_TABLE_ID || "Customers",
  addresses:
    process.env.AIRTABLE_ADDRESSES_TABLE_ID ||
    process.env.AIRTABLE_ADDRESSES_TABLE ||
    "Addresses",
  savedConfigurations:
    process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID || "Saved Configurations",
  labels: process.env.AIRTABLE_LABELS_TABLE_ID || "Labels",
  labelVersions:
    process.env.AIRTABLE_LABEL_VERSIONS_TABLE_ID || "Label Versions",
  orders:
    process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID ||
    process.env.AIRTABLE_ORDERS_TABLE ||
    "Orders & Fulfilment",
};

export const STUDIO_FIELDS = {
  customers: {
    orders: "Orders & Fulfilment",
    addresses: "Addresses",
  },
  addresses: {
    fullAddress: "Full Address",
    customer: "Customer",
    orders: "Orders & Fulfilment",
    firstName: "First Name",
    lastName: "Last Name",
    shopifyId: "Shopify ID",
    streetAddress1: "Street Address 1",
    streetAddress2: "Street Address 2",
    townCity: "Town / City",
    county: "County",
    postalCode: "Postal Code",
    country: "Country",
    phone: "Phone",
  },
  savedConfigurations: {
    configurationId: "Configuration ID",
    alcoholSelection: "Alcohol Selection",
    bottleSelection: "Bottle Selection",
    closureSelection: "Closure Selection",
    waxSelection: "Wax Selection",
    configJson: "Config JSON",
    configuratorTool: "Configurator Tool",
    creationSource: "Creation Source",
    customer: "Customer",
    displayName: "Display Name",
    internalSku: "Internal SKU",
    shopifyProductId: "Shopify Product ID",
    labelVersions: "Label Versions",
    labels: "Labels",
    liquidSelection: "Liquid Selection",
    previewImage: "Preview Image",
    previewImageUrl: "Preview Image URL",
    currentFrontLabelOutputImageUrl:
      "Output Image URL (from Current Front Label Versions)",
    order: "Orders & Fulfilment",
    status: "Status",
    sessionId: "Session ID",
    shopifyVariantId: "Shopify Variant ID",
    currentFrontLabelVersion: "Current Front Label Versions",
    currentBackLabelVersion: "Current Back Label Versions",
  },
  labels: {
    customers: "Customers",
    displayName: "Display Name",
    labelVersions: "Label Versions",
    savedConfigurations: "Saved Configurations",
    sessionId: "Session ID",
    currentFrontLabelVersion: "Current Front Label Versions",
    currentBackLabelVersion: "Current Back Label Versions",
  },
  labelVersions: {
    accepted: "Accepted",
    bottle: "Bottle",
    designSide: "Design Side",
    editPromptText: "Edit Prompt Text",
    inputReferenceUrl: "Input Reference URL",
    labels: "Labels",
    modelName: "Model Name",
    name: "Name",
    outputImageUrl: "Output Image URL",
    outputImage: "Output Image",
    outputPdfUrl: "Output PDF URL",
    outputS3Key: "Output S3 Key",
    outputS3Url: "Output S3 URL",
    outputZakekeUrl: "Output Zakeke URL",
    previousLabelVersion: "Previous Label Version",
    promptText: "Prompt Text",
    savedConfigurations: "Saved Configurations",
    sessionId: "Session ID",
    createdAt: "Created At",
    versionKind: "Version Kind",
    versionNumber: "Version Number",
  },
  orders: {
    orderId: "Order ID",
    customer: "Customer",
    savedConfiguration: "Saved Configuration",
    orderStatus: "Order Status",
    addresses: "Addresses",
    configurationId: "Configuration ID",
    sessionId: "Session ID",
    configuratorTool: "Configurator Tool",
    alcoholSelection: "Alcohol Selection",
    bottleSelection: "Bottle Selection",
    liquidSelection: "Liquid Selection",
    closureSelection: "Closure Selection",
    waxSelection: "Wax Selection",
    internalSku: "Internal SKU",
    shopifyProductId: "Shopify Product ID",
    shopifyVariantId: "Shopify Variant ID",
    configJson: "Config JSON",
    frontLabel: "Front Label",
    frontLabelUrl: "Front Label URL",
    previewImageUrl: "Preview Image URL",
    previewImage: "Preview Image",
    displayName: "Display Name",
    creationSource: "Creation Source",
  },
};

export const STUDIO_SINGLE_SELECT_OPTIONS = {
  customers: {
    source: ["Shopify", "Direct", "Lead"],
    creationSource: [
      "Not Logged-in Shopify -> Netlify Backend (create-airtable-customer)",
      "Logged-in Shopify -> Netlify Backend (create-airtable-customer)",
    ],
  },
  savedConfigurations: {
    status: ["Saved", "Ordered", "Archived"],
    creationSource: [
      "Shopify -> Netlify Backend (save-airtable-configuration)",
      "Shopify -> Netlify Backend (studio-save-configuration)",
    ],
  },
  orders: {
    orderStatus: ["Ordered", "Cancelled"],
    creationSource: [
      "Shopify -> Netlify Backend (save-airtable-configuration)",
      "Shopify -> Netlify Backend (studio-save-configuration)",
    ],
  },
};

// Canonical-first fallback names for in-flight Airtable schema migration.
export const STUDIO_FIELD_FALLBACKS = {
  savedConfigurations: {
    order: ["Order"],
    currentFrontLabelVersion: ["Current Front Label Version"],
    currentBackLabelVersion: ["Current Back Label Version"],
  },
  labels: {
    currentFrontLabelVersion: ["Current Front Label Version"],
    currentBackLabelVersion: ["Current Back Label Version"],
  },
  labelVersions: {
    savedConfigurationsFront: ["Saved Configurations - Current Front Label"],
    savedConfigurationsBack: ["Saved Configurations - Current Back Label"],
    createdAt: ["Created Date"],
  },
};

const UNKNOWN_FIELD_RE = /Unknown field name:\s*\"([^\"]+)\"/i;
const UNKNOWN_FIELD_ALT_RE = /Could not find field\s+\"([^\"]+)\"/i;

export const MAX_PAYLOAD_BYTES = 300_000;
export const MAX_PROMPT_LENGTH = 10_000;
export const MAX_NAME_LENGTH = 120;

export const sendJson = (status, obj) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
  });

export const parseBody = async (arg, method, isV2) => {
  if (!arg || method === "GET") return {};
  if (isV2) {
    const ct = (arg.headers.get("content-type") || "").toLowerCase();
    if (ct.includes("application/json")) return (await arg.json()) || {};
    if (ct.includes("application/x-www-form-urlencoded")) {
      const fd = await arg.formData();
      return Object.fromEntries([...fd.entries()]);
    }
    const text = await arg.text();
    if (!text) return {};
    try {
      return JSON.parse(text);
    } catch {
      return {};
    }
  }

  const ct = (arg.headers?.["content-type"] || "").toLowerCase();
  const raw = arg.body || "";
  if (!raw) return {};
  if (ct.includes("application/json")) return JSON.parse(raw);
  if (ct.includes("application/x-www-form-urlencoded")) {
    return Object.fromEntries(new URLSearchParams(raw));
  }
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
};

export const firstNonEmpty = (...values) => {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
};

export const normalizeSide = (value) => {
  const side = String(value || "").trim().toLowerCase();
  if (side === "front" || side === "back") return side;
  return null;
};

export const normalizeVersionKind = (value) => {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "initial") return "Initial";
  if (raw === "edit") return "Edit";
  if (raw === "upload") return "Upload";
  return null;
};

export const normalizeStatus = (value) => {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "saved") return "Saved";
  if (raw === "ordered") return "Ordered";
  if (raw === "archived") return "Archived";
  return null;
};

export const normalizeSingleSelectOption = (value, allowedOptions = []) => {
  const raw = String(value || "").trim();
  if (!raw) return null;
  return allowedOptions.includes(raw) ? raw : null;
};

export const normalizeOrderStatus = (value) => {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return null;
  if (raw === "ordered" || raw === "paid" || raw === "order received") {
    return "Ordered";
  }
  if (raw === "cancelled" || raw === "canceled") {
    return "Cancelled";
  }
  return null;
};

export const normalizeSavedConfigurationStatusFromOrderStatus = (value) => {
  const orderStatus = normalizeOrderStatus(value);
  if (orderStatus === "Ordered") return "Ordered";
  if (orderStatus === "Cancelled") return "Archived";
  return null;
};

export const normalizeRecordId = (value) => {
  const out = String(value || "").trim();
  if (!out) return null;
  return out.startsWith("rec") ? out : null;
};

export const toLinkedRecordArray = (...ids) => {
  const out = [];
  ids.forEach((id) => {
    const recId = normalizeRecordId(id);
    if (recId && !out.includes(recId)) out.push(recId);
  });
  return out.length ? out : undefined;
};

const toFieldNames = (...fieldNames) => {
  const out = [];
  fieldNames.flat().forEach((name) => {
    if (typeof name !== "string") return;
    const trimmed = name.trim();
    if (!trimmed || out.includes(trimmed)) return;
    out.push(trimmed);
  });
  return out;
};

export const getFieldValue = (record, fieldName, fallbackFieldNames = []) => {
  const names = toFieldNames(fieldName, fallbackFieldNames);
  for (const name of names) {
    if (!Object.hasOwn(record?.fields || {}, name)) continue;
    const value = record?.fields?.[name];
    if (value == null) continue;
    if (Array.isArray(value) && !value.length) continue;
    if (typeof value === "string" && !value.trim()) continue;
    return value;
  }
  return undefined;
};

export const getLinkedIds = (record, fieldName, fallbackFieldNames = []) => {
  const names = toFieldNames(fieldName, fallbackFieldNames);
  const out = [];
  names.forEach((name) => {
    const raw = record?.fields?.[name];
    if (!Array.isArray(raw)) return;
    raw.forEach((id) => {
      const recId = normalizeRecordId(id);
      if (recId && !out.includes(recId)) out.push(recId);
    });
  });
  return out;
};

export const buildFieldPatch = (
  record,
  fieldName,
  value,
  { fallbackFieldNames = [], includeFallbacksWhenUnknown = false } = {}
) => {
  const names = toFieldNames(fieldName, fallbackFieldNames);
  if (!names.length) return {};

  const existingNames = names.filter((name) => Object.hasOwn(record?.fields || {}, name));
  const targetNames = existingNames.length
    ? existingNames
    : includeFallbacksWhenUnknown
      ? names
      : [names[0]];

  return targetNames.reduce((acc, name) => {
    acc[name] = value;
    return acc;
  }, {});
};

export const buildLinkedPatch = (
  record,
  fieldName,
  linkedIds = [],
  options = {}
) => {
  const ids = Array.isArray(linkedIds) ? linkedIds : [linkedIds];
  return buildFieldPatch(record, fieldName, toLinkedRecordArray(...ids), options);
};

export const assertPayloadSize = (payload, maxBytes = MAX_PAYLOAD_BYTES) => {
  const size = Buffer.byteLength(JSON.stringify(payload || {}), "utf8");
  if (size > maxBytes) {
    const err = new Error(`Payload too large (${size} bytes, max ${maxBytes})`);
    err.status = 413;
    throw err;
  }
};

export const coerceBoolean = (value, fallback = false) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const v = value.trim().toLowerCase();
    if (v === "true" || v === "1" || v === "yes") return true;
    if (v === "false" || v === "0" || v === "no") return false;
  }
  return fallback;
};

export const sanitizeUrl = (value, maxLen = 2048) => {
  const text = String(value || "").trim();
  if (!text) return undefined;
  if (text.length > maxLen) return undefined;
  if (!/^https?:\/\//i.test(text)) return undefined;
  return text;
};

export const sanitizeText = (value, maxLen = MAX_PROMPT_LENGTH) => {
  const text = String(value || "").trim();
  if (!text) return undefined;
  return text.slice(0, maxLen);
};

export const sanitizeName = (value) => {
  const text = String(value || "").trim();
  if (!text) return undefined;
  return text.slice(0, MAX_NAME_LENGTH);
};

const extractUnknownFieldName = (error) => {
  const responseText = String(error?.responseText || "");
  let parsedMessage = "";
  try {
    const parsed = JSON.parse(responseText || "{}");
    parsedMessage = String(parsed?.error?.message || "");
  } catch {}
  const message = String(error?.message || "");
  const haystack = `${parsedMessage}\n${responseText}\n${message}`.replace(
    /\\"/g,
    '"'
  );
  const match = haystack.match(UNKNOWN_FIELD_RE) || haystack.match(UNKNOWN_FIELD_ALT_RE);
  return match?.[1] || null;
};

export const createResilient = async (
  table,
  requiredFields,
  optionalFields = {}
) => {
  const fields = { ...(requiredFields || {}), ...(optionalFields || {}) };
  let retries = 8;
  while (retries-- > 0) {
    try {
      return await createOne(table, fields);
    } catch (error) {
      const unknown = extractUnknownFieldName(error);
      if (!unknown || !(unknown in fields)) throw error;
      delete fields[unknown];
    }
  }
  return createOne(table, fields);
};

export const updateResilient = async (
  table,
  recordId,
  requiredFields,
  optionalFields = {}
) => {
  const fields = { ...(requiredFields || {}), ...(optionalFields || {}) };
  let retries = 8;
  while (retries-- > 0) {
    try {
      return await updateOne(table, recordId, fields);
    } catch (error) {
      const unknown = extractUnknownFieldName(error);
      if (!unknown || !(unknown in fields)) throw error;
      delete fields[unknown];
    }
  }
  return updateOne(table, recordId, fields);
};

export const listAllRecords = async (
  table,
  { filterByFormula, sort, maxRecords } = {}
) => {
  const records = [];
  let offset = null;
  do {
    const page = await listRecords(table, {
      filterByFormula,
      sort,
      maxRecords,
      pageSize: 100,
      offset,
    });
    const pageRecords = Array.isArray(page?.records) ? page.records : [];
    records.push(...pageRecords);
    offset = page?.offset || null;
  } while (offset);
  return records;
};

export const buildLinkedCustomerFormula = (fieldName, customerRecordId) => {
  const safe = escapeFormulaValue(customerRecordId);
  return `FIND('${safe}', ARRAYJOIN({${fieldName}}))`;
};

export const getRecordOrNull = async (table, recordId) => {
  if (!recordId) return null;
  return getOne(table, recordId);
};

export const deleteRecordOrNull = async (table, recordId) => {
  if (!recordId) return null;
  try {
    return await deleteOne(table, recordId);
  } catch (error) {
    if (Number(error?.status || 0) === 404) return null;
    throw error;
  }
};

export const mapErrorResponse = (error) => ({
  ok: false,
  error: error?.code || "server_error",
  message: error?.message || String(error),
  status: error?.status,
  url: error?.url,
  method: error?.method,
  responseText: error?.responseText,
});

const sanitizeCustomerIdentityValue = (value, maxLen = 255) => {
  const text = String(value || "").trim();
  if (!text) return null;
  return text.slice(0, maxLen);
};

const parseAirtableErrorType = (error) => {
  const responseText = String(error?.responseText || "");
  if (!responseText) return null;
  try {
    const parsed = JSON.parse(responseText);
    return String(parsed?.error?.type || "").trim() || null;
  } catch {
    return null;
  }
};

const isAirtableLookupRecoverableError = (error) => {
  const status = Number(error?.status || 0);
  if (status === 404) return true;
  if (status !== 403) return false;

  const airtableType = parseAirtableErrorType(error);
  if (airtableType === "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND") return true;

  const message = String(error?.message || "");
  if (/INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND/i.test(message)) return true;

  return false;
};

export const normalizeShopifyCustomerId = (value) => {
  const text = sanitizeCustomerIdentityValue(value, 255);
  if (!text) return null;
  if (normalizeRecordId(text)) return null;

  const gidMatch = text.match(/^gid:\/\/shopify\/Customer\/(\d+)(?:\?.*)?$/i);
  if (gidMatch) return gidMatch[1];

  return /^\d+$/.test(text) ? text : null;
};

export const buildShopifyCustomerIdLookupValues = (value) => {
  const normalized = normalizeShopifyCustomerId(value);
  if (!normalized) return [];
  return [normalized, `gid://shopify/Customer/${normalized}`];
};

const normalizeEmailValue = (value) => {
  const text = sanitizeCustomerIdentityValue(value, 255);
  if (!text || !text.includes("@")) return null;
  return text.toLowerCase();
};

const resolveCustomerIdentity = ({ body = {}, qs = {} } = {}) => {
  const shopifyIdCandidates = [
    body.shopify_customer_id,
    body.shopifyCustomerId,
    body.customer_shopify_id,
    body.shopify_id,
    body.shopifyId,
    qs.logged_in_customer_id,
    qs.customer_shopify_id,
    qs.shopify_customer_id,
    qs.shopify_id,
    body.customer_id,
    body.customerId,
    qs.customer_id,
  ];
  const emailCandidates = [
    body.email,
    body.customer_email,
    body.customerEmail,
    qs.email,
    qs.customer_email,
    qs.logged_in_customer_email,
  ];

  let shopifyId = null;
  for (const candidate of shopifyIdCandidates) {
    shopifyId = normalizeShopifyCustomerId(candidate);
    if (shopifyId) break;
  }

  let email = null;
  for (const candidate of emailCandidates) {
    email = normalizeEmailValue(candidate);
    if (email) break;
  }

  const firstName = sanitizeCustomerIdentityValue(
    firstNonEmpty(body.first_name, body.firstName, qs.first_name),
    120
  );
  const lastName = sanitizeCustomerIdentityValue(
    firstNonEmpty(body.last_name, body.lastName, qs.last_name),
    120
  );
  const phone = sanitizeCustomerIdentityValue(
    firstNonEmpty(body.phone, body.customer_phone, body.customerPhone, qs.phone),
    60
  );

  return { shopifyId, email, firstName, lastName, phone };
};

const findCustomerByField = async (fieldName, value) => {
  const text = sanitizeCustomerIdentityValue(value, 255);
  if (!text) return { record: null, uncertain: false };
  try {
    const record = await findOneBy(STUDIO_TABLES.customers, fieldName, text);
    return {
      record: record?.id ? record : null,
      uncertain: false,
    };
  } catch (error) {
    if (!isAirtableLookupRecoverableError(error)) throw error;
    console.warn("[studio] customer lookup skipped", {
      fieldName,
      status: error?.status,
      errorType: parseAirtableErrorType(error),
    });
    return {
      record: null,
      uncertain: true,
    };
  }
};

export const resolveCustomerRecordIdOrCreate = async ({
  providedCustomerRecordId,
  body = {},
  qs = {},
  allowCreate = false,
  endpoint = "unknown",
} = {}) => {
  const implicitCreateFlag =
    String(process.env.STUDIO_ALLOW_IMPLICIT_CUSTOMER_CREATE || "")
      .trim()
      .toLowerCase() === "true";
  const shouldCreate = Boolean(allowCreate || implicitCreateFlag);
  const providedRecordId = normalizeRecordId(providedCustomerRecordId);
  const identity = resolveCustomerIdentity({ body, qs });
  const hasIdentitySignals = Boolean(identity.shopifyId || identity.email);
  let lookupUncertain = false;
  const logResolution = ({
    customerCreationReason,
    recovered = false,
    created = false,
  } = {}) => {
    console.log("[studio] customer-resolution", {
      customer_creation_reason: customerCreationReason || "unknown",
      endpoint,
      shopify_id_present: Boolean(identity.shopifyId),
      email_present: Boolean(identity.email),
      provided_record_id: providedRecordId || null,
      recovered: Boolean(recovered),
      created: Boolean(created),
    });
  };

  if (providedRecordId) {
    let existing = null;
    try {
      existing = await getRecordOrNull(STUDIO_TABLES.customers, providedRecordId);
    } catch (error) {
      if (!isAirtableLookupRecoverableError(error)) throw error;
      if (Number(error?.status || 0) !== 404) {
        lookupUncertain = true;
      }
      console.warn("[studio] provided customer record lookup failed; attempting recovery", {
        providedRecordId,
        status: error?.status,
        errorType: parseAirtableErrorType(error),
      });
      existing = null;
    }
    if (existing?.id) {
      logResolution({
        customerCreationReason: "lookup_by_provided_record_id",
        recovered: false,
        created: false,
      });
      return {
        customerRecordId: existing.id,
        created: false,
        recovered: false,
      };
    }
  }

  const shopifySearchCandidates = buildShopifyCustomerIdLookupValues(
    identity.shopifyId
  );

  for (const shopifyId of shopifySearchCandidates) {
    const matchedResult = await findCustomerByField("Shopify ID", shopifyId);
    if (matchedResult.uncertain) lookupUncertain = true;
    const matched = matchedResult.record;
    if (matched?.id) {
      const recovered = Boolean(providedRecordId && matched.id !== providedRecordId);
      logResolution({
        customerCreationReason: "recovered_by_shopify_id",
        recovered,
        created: false,
      });
      return {
        customerRecordId: matched.id,
        created: false,
        recovered,
      };
    }
  }

  if (identity.email) {
    const matchedByEmailResult = await findCustomerByField("Email", identity.email);
    if (matchedByEmailResult.uncertain) lookupUncertain = true;
    const matchedByEmail = matchedByEmailResult.record;
    if (matchedByEmail?.id) {
      const recovered = Boolean(
        providedRecordId && matchedByEmail.id !== providedRecordId
      );
      logResolution({
        customerCreationReason: "recovered_by_email",
        recovered,
        created: false,
      });
      return {
        customerRecordId: matchedByEmail.id,
        created: false,
        recovered,
      };
    }
  }

  const shouldCreateAfterConfirmedMiss =
    hasIdentitySignals && !lookupUncertain;
  if (!(shouldCreate || shouldCreateAfterConfirmedMiss)) {
    logResolution({
      customerCreationReason: lookupUncertain
        ? "not_resolved_lookup_uncertain"
        : "not_resolved_no_identity",
      recovered: false,
      created: false,
    });
    return {
      customerRecordId: null,
      created: false,
      recovered: false,
    };
  }

  const created = await createResilient(
    STUDIO_TABLES.customers,
    {},
    {
      "Shopify ID": identity.shopifyId || undefined,
      Email: identity.email || undefined,
      "First Name": identity.firstName || undefined,
      "Last Name": identity.lastName || undefined,
      Phone: identity.phone || undefined,
    }
  );

  logResolution({
    customerCreationReason: "created_by_helper",
    recovered: Boolean(providedRecordId),
    created: true,
  });

  return {
    customerRecordId: created.id,
    created: true,
    recovered: Boolean(providedRecordId),
  };
};
