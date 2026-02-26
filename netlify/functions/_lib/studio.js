import {
  createOne,
  updateOne,
  getOne,
  listRecords,
  escapeFormulaValue,
} from "../../../src/lib/airtable.js";

export const STUDIO_TABLES = {
  customers: process.env.AIRTABLE_CUSTOMERS_TABLE_ID || "Customers",
  savedConfigurations:
    process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID || "Saved Configurations",
  labels: process.env.AIRTABLE_LABELS_TABLE_ID || "Labels",
  labelVersions:
    process.env.AIRTABLE_LABEL_VERSIONS_TABLE_ID || "Label Versions",
  orders:
    process.env.AIRTABLE_ORDERS_TABLE_ID ||
    process.env.AIRTABLE_ORDERS_TABLE ||
    "Orders & Fulfilment",
};

export const STUDIO_FIELDS = {
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
    labels: "Labels",
    liquidSelection: "Liquid Selection",
    previewImageUrl: "Preview Image URL",
    order: "Order",
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
    designSide: "Design Side",
    editPromptText: "Edit Prompt Text",
    inputCharacterUrl: "Input Character URL",
    inputLogoUrl: "Input Logo URL",
    inputReferenceUrl: "Input Reference URL",
    labels: "Labels",
    modelName: "Model Name",
    name: "Name",
    outputImageUrl: "Output Image URL",
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
};

// Canonical-first fallback names for in-flight Airtable schema migration.
export const STUDIO_FIELD_FALLBACKS = {
  savedConfigurations: {
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
  const message = String(error?.message || "");
  const haystack = `${responseText}\n${message}`;
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

export const mapErrorResponse = (error) => ({
  ok: false,
  error: error?.code || "server_error",
  message: error?.message || String(error),
  status: error?.status,
  url: error?.url,
  method: error?.method,
  responseText: error?.responseText,
});
