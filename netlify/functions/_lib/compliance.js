export const COMPLIANCE_SCHEMA_VERSION = 1;

export const COMPLIANCE_NOTE_ATTRIBUTE_KEYS = {
  schemaVersion: "_ss_compliance_schema_version",
  forResale: "_ss_for_resale",
  premiseLicence: "_ss_premise_licence",
  alcoholLicence: "_ss_alcohol_licence",
  licenceType: "_ss_licence_type",
  personalLicence: "_ss_personal_licence",
  tenLicence: "_ss_ten_licence",
  isBusinessPurchase: "_ss_is_business_purchase",
  companyName: "_ss_company_name",
  tradingName: "_ss_trading_name",
  companyNumber: "_ss_company_number",
  vatNumber: "_ss_vat_number",
  customerAirtableId: "_ss_customer_airtable_id",
  sessionId: "_ss_session_id",
  legacyProfileJson: "_ss_compliance_profile",
};

const normalizeText = (value, maxLen = 255) => {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.slice(0, maxLen);
};

const normalizeBoolean = (value) => {
  if (typeof value === "boolean") return value;
  const text = normalizeText(value, 32);
  if (!text) return null;
  const lower = text.toLowerCase();
  if (["1", "true", "yes", "on"].includes(lower)) return true;
  if (["0", "false", "no", "off"].includes(lower)) return false;
  return null;
};

const normalizeLicenceType = (value) => {
  const text = normalizeText(value, 32);
  if (!text) return null;
  const lower = text.toLowerCase();
  if (lower === "personal") return "personal";
  if (lower === "ten" || lower === "temporary_event_notice") return "ten";
  return null;
};

const parseJsonObject = (value) => {
  const text = normalizeText(value, 100_000);
  if (!text) return null;
  try {
    const parsed = JSON.parse(text);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
};

const normalizeSchemaVersion = (value) => {
  const numberValue = Number(value);
  if (!Number.isFinite(numberValue)) return COMPLIANCE_SCHEMA_VERSION;
  return Math.max(1, Math.trunc(numberValue));
};

export const normalizeComplianceProfile = (input = {}) => {
  const source = input && typeof input === "object" ? input : {};
  const forResale = normalizeBoolean(source.for_resale);

  const normalized = {
    schema_version: normalizeSchemaVersion(source.schema_version),
    for_resale: forResale,
    premise_licence: normalizeText(source.premise_licence, 255),
    alcohol_licence: normalizeText(source.alcohol_licence, 255),
    licence_type: normalizeLicenceType(source.licence_type),
    personal_licence: normalizeText(source.personal_licence, 255),
    ten_licence: normalizeText(source.ten_licence, 255),
    is_business_purchase: normalizeBoolean(source.is_business_purchase),
    company_name: normalizeText(source.company_name, 255),
    trading_name: normalizeText(source.trading_name, 255),
    company_number: normalizeText(source.company_number, 255),
    vat_number: normalizeText(source.vat_number, 255),
  };

  if (normalized.for_resale !== true) {
    return {
      ...normalized,
      premise_licence: null,
      alcohol_licence: null,
      licence_type: null,
      personal_licence: null,
      ten_licence: null,
      is_business_purchase: null,
      company_name: null,
      trading_name: null,
      company_number: null,
      vat_number: null,
    };
  }

  if (normalized.licence_type === "personal") {
    normalized.ten_licence = null;
  } else if (normalized.licence_type === "ten") {
    normalized.personal_licence = null;
  } else {
    normalized.personal_licence = null;
    normalized.ten_licence = null;
  }

  if (normalized.is_business_purchase !== true) {
    normalized.company_name = null;
    normalized.trading_name = null;
    normalized.company_number = null;
    normalized.vat_number = null;
  }

  return normalized;
};

export const validateComplianceProfile = (profile = {}) => {
  const normalized = normalizeComplianceProfile(profile);
  const errors = [];

  if (normalized.for_resale == null) {
    errors.push("For resale decision is required.");
  }

  if (normalized.for_resale === true) {
    if (!normalized.premise_licence) {
      errors.push("Premise licence number is required.");
    }
    if (!normalized.alcohol_licence) {
      errors.push("Alcohol licence number is required.");
    }
    if (!normalized.licence_type) {
      errors.push("Licence type is required.");
    }
    if (normalized.licence_type === "personal" && !normalized.personal_licence) {
      errors.push("Personal licence number is required.");
    }
    if (normalized.licence_type === "ten" && !normalized.ten_licence) {
      errors.push("Temporary Event Notice number is required.");
    }
    if (normalized.is_business_purchase == null) {
      errors.push("Business purchase decision is required.");
    }
    if (normalized.is_business_purchase === true) {
      if (!normalized.company_name) {
        errors.push("Registered company name is required.");
      }
      if (!normalized.company_number) {
        errors.push("Company number is required.");
      }
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    profile: normalized,
  };
};

export const hasReusableComplianceProfile = (profile = {}) => {
  const validation = validateComplianceProfile(profile);
  return validation.ok && validation.profile.for_resale === true;
};

const buildAttributeMap = (noteAttributes = []) => {
  const map = {};
  const entries = Array.isArray(noteAttributes) ? noteAttributes : [];
  for (const entry of entries) {
    const key = normalizeText(entry?.name ?? entry?.key, 255);
    if (!key) continue;
    const value = normalizeText(entry?.value, 10_000) ?? "";
    map[key] = value;
    map[key.toLowerCase()] = value;
  }
  return map;
};

const readAttr = (attributes, key) => {
  if (!key) return null;
  const direct = attributes?.[key];
  if (direct != null && String(direct).trim()) return String(direct).trim();
  const lower = attributes?.[String(key).toLowerCase()];
  if (lower != null && String(lower).trim()) return String(lower).trim();
  return null;
};

export const parseComplianceNoteAttributes = (orderOrAttributes = {}) => {
  const attributes = Array.isArray(orderOrAttributes)
    ? buildAttributeMap(orderOrAttributes)
    : buildAttributeMap(orderOrAttributes?.note_attributes);

  const legacyProfile = parseJsonObject(
    readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.legacyProfileJson)
  );

  const profile = normalizeComplianceProfile(
    legacyProfile || {
      schema_version: readAttr(
        attributes,
        COMPLIANCE_NOTE_ATTRIBUTE_KEYS.schemaVersion
      ),
      for_resale: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.forResale),
      premise_licence: readAttr(
        attributes,
        COMPLIANCE_NOTE_ATTRIBUTE_KEYS.premiseLicence
      ),
      alcohol_licence: readAttr(
        attributes,
        COMPLIANCE_NOTE_ATTRIBUTE_KEYS.alcoholLicence
      ),
      licence_type: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.licenceType),
      personal_licence: readAttr(
        attributes,
        COMPLIANCE_NOTE_ATTRIBUTE_KEYS.personalLicence
      ),
      ten_licence: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.tenLicence),
      is_business_purchase: readAttr(
        attributes,
        COMPLIANCE_NOTE_ATTRIBUTE_KEYS.isBusinessPurchase
      ),
      company_name: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.companyName),
      trading_name: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.tradingName),
      company_number: readAttr(
        attributes,
        COMPLIANCE_NOTE_ATTRIBUTE_KEYS.companyNumber
      ),
      vat_number: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.vatNumber),
    }
  );

  const hasSignals =
    profile.for_resale != null ||
    Boolean(
      readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.customerAirtableId) ||
        readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.sessionId)
    );

  return {
    hasComplianceAttributes: hasSignals,
    customer_airtable_id:
      readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.customerAirtableId) || null,
    session_id: readAttr(attributes, COMPLIANCE_NOTE_ATTRIBUTE_KEYS.sessionId) || null,
    profile,
  };
};

export const buildCustomerComplianceProfile = (input = {}) => {
  const validation = validateComplianceProfile(input);
  if (!validation.ok) return validation;

  return {
    ok: true,
    profile: {
      ...validation.profile,
      saved_at: new Date().toISOString(),
    },
    errors: [],
  };
};

export const buildOrderComplianceMetafieldPayload = ({
  order = {},
  customerRecordId = null,
  sessionId = null,
  profile = {},
} = {}) => ({
  schema_version: COMPLIANCE_SCHEMA_VERSION,
  synced_at: new Date().toISOString(),
  order_id: order?.id != null ? String(order.id) : null,
  order_name: normalizeText(order?.name, 120),
  customer_airtable_id: normalizeText(customerRecordId, 255),
  session_id: normalizeText(sessionId, 255),
  ...normalizeComplianceProfile(profile),
});
