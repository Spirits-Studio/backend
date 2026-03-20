import { findOneBy, escapeFormulaValue } from "../../../src/lib/airtable.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  STUDIO_FIELD_FALLBACKS,
  STUDIO_SINGLE_SELECT_OPTIONS,
  firstNonEmpty,
  buildShopifyCustomerIdLookupValues,
  normalizeShopifyCustomerId,
  normalizeRecordId,
  normalizeOrderStatus,
  normalizeSavedConfigurationStatusFromOrderStatus,
  normalizeSide,
  normalizeSingleSelectOption,
  isAirtableLookupRecoverableError,
  parseAirtableErrorType,
  getLinkedIds,
  getFieldValue,
  buildLinkedPatch,
  createResilient,
  updateResilient,
  getRecordOrNull,
  listAllRecords,
  listRecordsByLinkedRecordIds,
  sanitizeText,
  sanitizeUrl,
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

const normalizePhone = (value) => normalizeText(value, 60);

const normalizeName = (value) => normalizeText(value, 120);

const normalizeCustomerCreationSource = (value) =>
  normalizeSingleSelectOption(
    value,
    STUDIO_SINGLE_SELECT_OPTIONS.customers.creationSource
  );

const normalizeOrderCreationSource = (value) =>
  normalizeSingleSelectOption(
    value,
    STUDIO_SINGLE_SELECT_OPTIONS.orders.creationSource
  );

const normalizeAddressId = (value) => {
  const text = normalizeText(value, 255);
  if (!text) return null;
  return normalizeRecordId(text) ? null : text;
};

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

const uniqueLinkedRecordIds = (...values) =>
  uniqueRecordIds(
    values.flatMap((value) => {
      if (!value) return [];
      return Array.isArray(value) ? value : [value];
    })
  );

const buildFullAddress = ({
  firstName,
  lastName,
  streetAddress1,
  streetAddress2,
  townCity,
  county,
  postalCode,
  country,
}) =>
  normalizeText(
    [
      [firstName, lastName].filter(Boolean).join(" ").trim(),
      streetAddress1,
      streetAddress2,
      townCity,
      county,
      postalCode,
      country,
    ]
      .map((value) => normalizeText(value, 255))
      .filter(Boolean)
      .join(", "),
    1000
  );

const normalizeOrderAddress = (address, fallbackCustomer = {}) => {
  if (!address || typeof address !== "object") return null;

  const hasRawAddressSignals = Boolean(
    address?.id ||
      address?.shopifyId ||
      address?.shopify_id ||
      address?.customer_address_id ||
      address?.address1 ||
      address?.streetAddress1 ||
      address?.street_address_1 ||
      address?.address2 ||
      address?.streetAddress2 ||
      address?.street_address_2 ||
      address?.city ||
      address?.townCity ||
      address?.town_city ||
      address?.province ||
      address?.province_code ||
      address?.county ||
      address?.zip ||
      address?.postalCode ||
      address?.postal_code ||
      address?.country ||
      address?.country_code ||
      address?.phone
  );
  if (!hasRawAddressSignals) return null;

  const normalized = {
    shopifyId: normalizeAddressId(
      address?.shopifyId ??
        address?.shopify_id ??
        address?.id ??
        address?.customer_address_id
    ),
    firstName: normalizeName(
      address?.firstName ??
        address?.first_name ??
        fallbackCustomer?.first_name ??
        fallbackCustomer?.firstName
    ),
    lastName: normalizeName(
      address?.lastName ??
        address?.last_name ??
        fallbackCustomer?.last_name ??
        fallbackCustomer?.lastName
    ),
    streetAddress1: normalizeText(
      address?.streetAddress1 ?? address?.street_address_1 ?? address?.address1,
      255
    ),
    streetAddress2: normalizeText(
      address?.streetAddress2 ?? address?.street_address_2 ?? address?.address2,
      255
    ),
    townCity: normalizeText(address?.townCity ?? address?.town_city ?? address?.city, 255),
    county: normalizeText(
      address?.county ?? address?.province ?? address?.province_code,
      255
    ),
    postalCode: normalizeText(address?.postalCode ?? address?.postal_code ?? address?.zip, 120),
    country: normalizeText(address?.country ?? address?.country_code, 255),
    phone: normalizePhone(address?.phone ?? fallbackCustomer?.phone),
  };

  normalized.fullAddress =
    normalizeText(address?.fullAddress ?? address?.full_address, 1000) ||
    buildFullAddress(normalized);

  const hasMeaningfulAddress = Boolean(
    normalized.shopifyId ||
      normalized.fullAddress ||
      normalized.phone
  );

  return hasMeaningfulAddress ? normalized : null;
};

const parseJsonField = (value) => {
  const text = typeof value === "string" ? value.trim() : "";
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

const firstTextValue = (value, maxLen = 255) => {
  if (Array.isArray(value)) {
    for (const entry of value) {
      const text = normalizeText(entry, maxLen);
      if (text) return text;
    }
    return null;
  }
  return normalizeText(value, maxLen);
};

const firstAttachmentUrl = (value) => {
  if (!Array.isArray(value)) return null;
  for (const entry of value) {
    const url = sanitizeUrl(
      entry?.url || entry?.thumbnails?.large?.url || entry?.thumbnails?.full?.url
    );
    if (url) return url;
  }
  return null;
};

const toAttachmentFieldFromUrl = (value) => {
  const url = sanitizeUrl(value);
  return url ? [{ url }] : undefined;
};

const readSelectedLabelVersionUrl = (selectedLabelVersion) =>
  sanitizeUrl(
    firstNonEmpty(
      selectedLabelVersion?.outputImageUrl,
      selectedLabelVersion?.output_image_url,
      selectedLabelVersion?.outputS3Url,
      selectedLabelVersion?.output_s3_url
    )
  ) || null;

const getFrontLabelUrlFromSnapshot = (snapshot) => {
  if (!snapshot || typeof snapshot !== "object") return null;

  const directUrl = sanitizeUrl(
    firstNonEmpty(
      snapshot.front_label_url,
      snapshot.frontLabelUrl,
      snapshot.front_image_url,
      snapshot.frontImageUrl
    )
  );
  if (directUrl) return directUrl;

  const selectedSide = normalizeSide(
    firstNonEmpty(
      snapshot?.selectedLabelVersion?.designSide,
      snapshot?.selectedLabelVersion?.design_side
    )
  );
  if (!selectedSide || selectedSide === "front") {
    const selectedUrl = readSelectedLabelVersionUrl(snapshot?.selectedLabelVersion);
    if (selectedUrl) return selectedUrl;
  }

  const frontVersionLikeCandidates = [
    snapshot?.front,
    snapshot?.front_label,
    snapshot?.frontLabel,
    snapshot?.labels?.front,
    snapshot?.labelVersions?.front,
    snapshot?.selectedFrontLabelVersion,
    snapshot?.selected_front_label_version,
  ];

  for (const candidate of frontVersionLikeCandidates) {
    if (!candidate || typeof candidate !== "object") continue;
    const nestedUrl =
      readSelectedLabelVersionUrl(candidate?.selectedLabelVersion) ||
      readSelectedLabelVersionUrl(candidate);
    if (nestedUrl) return nestedUrl;
  }

  return null;
};

const normalizeOrderQuantity = (value) => {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) return null;
  const integerValue = Math.trunc(numericValue);
  return integerValue > 0 ? integerValue : null;
};

const ALLOWED_STUDIO_ORDER_PRODUCT_IDS = new Set([
  "10781413704026",
  "10532134027610",
  "10197521465690",
  "10243095429466",
  "10243096445274",
  "10243100311898",
]);

const normalizeShopifyProductId = (value) => normalizeText(value, 255);
const normalizeShopifyVariantId = (value) => normalizeText(value, 255);
const SHOPIFY_ORDER_DETAILS_SCHEMA_VERSION = 1;

const pickText = (...values) => {
  for (const value of values) {
    const text = normalizeText(value, 255);
    if (text) return text;
  }
  return null;
};

const pickUrl = (...values) => {
  for (const value of values) {
    const url = sanitizeUrl(value);
    if (url) return url;
  }
  return null;
};

const pickQuantity = (...values) => {
  for (const value of values) {
    const quantity = normalizeOrderQuantity(value);
    if (quantity != null) return quantity;
  }
  return null;
};

const buildOrderSnapshotFields = (
  savedConfigRecord,
  { quantity = null, lineItemMetadata = null } = {}
) => {
  const fields = savedConfigRecord?.fields || {};
  const configJson = sanitizeText(
    getFieldValue(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.configJson),
    100_000
  );
  const configSnapshot = parseJsonField(configJson);
  const metadata = lineItemMetadata && typeof lineItemMetadata === "object"
    ? lineItemMetadata
    : {};

  const previewImageUrl =
    pickUrl(
      metadata.preview_url,
      firstTextValue(
        getFieldValue(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.previewImageUrl),
        2048
      )
    ) ||
    firstAttachmentUrl(
      getFieldValue(savedConfigRecord, STUDIO_FIELDS.savedConfigurations.previewImage)
    ) ||
    pickUrl(
      configSnapshot?.preview_url,
      configSnapshot?.previewUrl,
      configSnapshot?.previewImage,
      configSnapshot?.preview
    ) ||
    null;

  const frontLabelUrl =
    pickUrl(
      metadata.front_label_url,
      firstTextValue(
        getFieldValue(
          savedConfigRecord,
          STUDIO_FIELDS.savedConfigurations.currentFrontLabelOutputImageUrl
        ),
        2048
      )
    ) ||
    getFrontLabelUrlFromSnapshot(configSnapshot) ||
    null;
  const normalizedQuantity =
    pickQuantity(
      quantity,
      metadata.quantity,
      firstNonEmpty(configSnapshot?.quantity, configSnapshot?.qty)
    );
  const productName = pickText(
    metadata.product,
    configSnapshot?.product_name,
    configSnapshot?.productName
  );

  return {
    [STUDIO_FIELDS.orders.configurationId]: normalizeText(
      fields[STUDIO_FIELDS.savedConfigurations.configurationId],
      80
    ) || undefined,
    [STUDIO_FIELDS.orders.sessionId]:
      pickText(
        metadata.session_id,
        fields[STUDIO_FIELDS.savedConfigurations.sessionId]
      ) || undefined,
    [STUDIO_FIELDS.orders.configuratorTool]: normalizeText(
      fields[STUDIO_FIELDS.savedConfigurations.configuratorTool],
      120
    ) || undefined,
    [STUDIO_FIELDS.orders.alcoholSelection]: normalizeText(
      fields[STUDIO_FIELDS.savedConfigurations.alcoholSelection],
      120
    ) || undefined,
    [STUDIO_FIELDS.orders.bottleSelection]:
      pickText(
        metadata.bottle,
        fields[STUDIO_FIELDS.savedConfigurations.bottleSelection]
      ) || undefined,
    [STUDIO_FIELDS.orders.liquidSelection]:
      pickText(
        metadata.liquid,
        fields[STUDIO_FIELDS.savedConfigurations.liquidSelection]
      ) || undefined,
    [STUDIO_FIELDS.orders.closureSelection]:
      pickText(
        metadata.closure,
        fields[STUDIO_FIELDS.savedConfigurations.closureSelection]
      ) || undefined,
    [STUDIO_FIELDS.orders.waxSelection]:
      pickText(
        metadata.wax,
        fields[STUDIO_FIELDS.savedConfigurations.waxSelection]
      ) || undefined,
    [STUDIO_FIELDS.orders.internalSku]:
      pickText(
        metadata.sku,
        fields[STUDIO_FIELDS.savedConfigurations.internalSku]
      ) || undefined,
    [STUDIO_FIELDS.orders.shopifyProduct]:
      productName || undefined,
    [STUDIO_FIELDS.orders.shopifyProductId]:
      pickText(
        metadata.product_id,
        fields[STUDIO_FIELDS.savedConfigurations.shopifyProductId]
      ) || undefined,
    [STUDIO_FIELDS.orders.shopifyVariantId]:
      pickText(
        metadata.variant_id,
        fields[STUDIO_FIELDS.savedConfigurations.shopifyVariantId]
      ) || undefined,
    [STUDIO_FIELDS.orders.quantity]: normalizedQuantity ?? undefined,
    [STUDIO_FIELDS.orders.configJson]: configJson || undefined,
    [STUDIO_FIELDS.orders.frontLabelUrl]: frontLabelUrl || undefined,
    [STUDIO_FIELDS.orders.frontLabel]: toAttachmentFieldFromUrl(frontLabelUrl),
    [STUDIO_FIELDS.orders.previewImageUrl]: previewImageUrl || undefined,
    [STUDIO_FIELDS.orders.previewImage]: toAttachmentFieldFromUrl(previewImageUrl),
    [STUDIO_FIELDS.orders.displayName]:
      pickText(
        metadata.display_name,
        fields[STUDIO_FIELDS.savedConfigurations.displayName]
      ) || undefined,
    [STUDIO_FIELDS.orders.creationSource]:
      normalizeOrderCreationSource(
        fields[STUDIO_FIELDS.savedConfigurations.creationSource]
      ) || undefined,
  };
};

export const normalizeOrderWebhookPayload = (payload, envelope = {}) => {
  const noteFallback = parseNoteAttributes(payload);
  const rawLineItems = Array.isArray(payload?.line_items) ? payload.line_items : [];
  const lineItems = rawLineItems.length ? rawLineItems : [{}];

  const normalizedLineItems = lineItems.map((lineItem) => {
    const mergedProperties = parseLineItemProperties(lineItem, noteFallback);
    return {
      id: lineItem?.id ?? null,
      product_id: normalizeShopifyProductId(lineItem?.product_id),
      variant_id: normalizeShopifyVariantId(lineItem?.variant_id),
      title: normalizeText(lineItem?.title ?? lineItem?.name, 255),
      sku: normalizeText(lineItem?.sku, 255),
      quantity: normalizeOrderQuantity(lineItem?.quantity) || 0,
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
        _display_name: normalizeText(
          readProperty(
            mergedProperties,
            "_display_name",
            "display_name",
            "Display Name"
          ),
          255
        ),
        _preview_url: sanitizeUrl(
          readProperty(mergedProperties, "_preview_url", "preview_url", "Preview URL")
        ) || null,
        _front_label_url: sanitizeUrl(
          readProperty(
            mergedProperties,
            "_front_label_url",
            "front_label_url",
            "Front Label URL"
          )
        ) || null,
        _sku: normalizeText(
          readProperty(mergedProperties, "_sku", "sku", "SKU"),
          255
        ),
        bottle: normalizeText(readProperty(mergedProperties, "Bottle", "bottle"), 120),
        liquid: normalizeText(readProperty(mergedProperties, "Liquid", "liquid"), 120),
        closure: normalizeText(
          readProperty(
            mergedProperties,
            "Closure",
            "closure",
            "Wood Closure",
            "wood closure"
          ),
          120
        ),
        wax: normalizeText(
          readProperty(mergedProperties, "Wax", "wax", "Wax Seal", "wax seal"),
          120
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
      billing_address: normalizeOrderAddress(billing, customer),
      shipping_address: normalizeOrderAddress(shipping, customer),
      line_items: normalizedLineItems,
    },
  };
};

export const normalizeCustomerWebhookPayload = (payload, envelope = {}) => {
  const customerShopifyId = normalizeShopifyCustomerId(
    payload?.admin_graphql_api_id ?? payload?.id
  );
  const normalizedPhone = normalizePhone(
    payload?.phone ??
      payload?.default_address?.phone ??
      payload?.addresses?.find?.((address) => address?.phone)?.phone
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
      phone: normalizedPhone,
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
  creationSource = null,
  preferredCustomerRecordIds = [],
  allowCreate = true,
}) => {
  const normalizedShopifyId = normalizeShopifyCustomerId(shopifyId);
  const normalizedEmail = normalizeEmail(email);
  const normalizedFirstName = normalizeName(firstName);
  const normalizedLastName = normalizeName(lastName);
  const normalizedPhone = normalizePhone(phone);
  const normalizedShopDomain = normalizeText(shopDomain, 255);
  const normalizedCreationSource = normalizeCustomerCreationSource(creationSource);

  if (!normalizedShopifyId && !normalizedEmail && !normalizedPhone) {
    return {
      customerRecordId: null,
      created: false,
      matchedBy: null,
      updated: false,
    };
  }

  let existing = null;
  let matchedBy = null;

  const preferredIds = uniqueRecordIds(preferredCustomerRecordIds);
  const tryPreferredRecord = async () => {
    if (preferredIds.length !== 1) return null;
    let preferred = null;
    try {
      preferred = await getRecordOrNull(STUDIO_TABLES.customers, preferredIds[0]);
    } catch (error) {
      if (!isAirtableLookupRecoverableError(error)) throw error;
      console.warn("[webhook] preferred customer lookup skipped", {
        preferredCustomerRecordId: preferredIds[0],
        status: error?.status,
        errorType: parseAirtableErrorType(error),
      });
      return null;
    }
    const currentShopifyId = normalizeShopifyCustomerId(
      preferred?.fields?.["Shopify ID"]
    );
    const currentEmail = normalizeEmail(preferred?.fields?.Email);
    const hasClaimablePreferredShopifyId =
      !currentShopifyId ||
      (normalizedShopifyId && currentShopifyId === normalizedShopifyId);
    const hasClaimablePreferredEmail =
      !normalizedShopifyId &&
      (!currentEmail || !normalizedEmail || currentEmail === normalizedEmail);

    if (
      preferred?.id &&
      (hasClaimablePreferredShopifyId || hasClaimablePreferredEmail)
    ) {
      return preferred;
    }
    return null;
  };

  if (normalizedShopifyId) {
    for (const lookupValue of buildShopifyCustomerIdLookupValues(
      normalizedShopifyId
    )) {
      existing = await findOneBy(STUDIO_TABLES.customers, "Shopify ID", lookupValue);
      if (existing?.id) {
        matchedBy = "shopify_id";
        break;
      }
    }
  }

  if (!existing?.id) {
    const preferred = await tryPreferredRecord();
    if (preferred?.id) {
      existing = preferred;
      matchedBy = "preferred_record";
    }
  }

  if (!existing?.id && normalizedEmail) {
    existing = await findOneBy(STUDIO_TABLES.customers, "Email", normalizedEmail);
    if (existing?.id) matchedBy = "email";
  }

  if (!existing?.id && normalizedPhone) {
    existing = await findOneBy(STUDIO_TABLES.customers, "Phone", normalizedPhone);
    if (existing?.id) matchedBy = "phone";
  }

  const canonicalFields = {
    "Shopify ID": normalizedShopifyId || undefined,
    Email: normalizedEmail || undefined,
    "First Name": normalizedFirstName || undefined,
    "Last Name": normalizedLastName || undefined,
    Phone: normalizedPhone || undefined,
    Source: "Shopify",
    "Shop Domain": normalizedShopDomain || undefined,
    "Creation Source": normalizedCreationSource || undefined,
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

  if (!allowCreate) {
    return {
      customerRecordId: null,
      created: false,
      matchedBy: null,
      updated: false,
      record: null,
      skippedCreate: true,
    };
  }

  const created = await createResilient(STUDIO_TABLES.customers, {}, canonicalFields);

  return {
    customerRecordId: created?.id || null,
    created: true,
    matchedBy: normalizedShopifyId
      ? "shopify_id"
      : normalizedEmail
        ? "email"
        : "phone",
    updated: false,
    record: created,
  };
};

const listAddressesByLinkedOrder = async (orderRecordId) => {
  const orderId = normalizeRecordId(orderRecordId);
  if (!orderId) return [];

  try {
    const rows = await listRecordsByLinkedRecordIds(STUDIO_TABLES.addresses, {
      fieldName: STUDIO_FIELDS.addresses.orders,
      linkedRecordIds: orderId,
      maxMatches: 5,
    });
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
};

export const upsertBillingAddressForOrder = async ({
  order = {},
  customerRecordIds = [],
  orderRecordId,
}) => {
  const normalizedOrderRecordId = normalizeRecordId(orderRecordId);
  const linkedCustomerIds = uniqueRecordIds(customerRecordIds);
  const billingAddress =
    normalizeOrderAddress(order?.billing_address, order?.customer) ||
    normalizeOrderAddress(order?.shipping_address, order?.customer);

  if (!billingAddress) return null;

  let existingAddress = null;
  if (billingAddress.shopifyId) {
    existingAddress = await findOneBy(
      STUDIO_TABLES.addresses,
      STUDIO_FIELDS.addresses.shopifyId,
      billingAddress.shopifyId
    );
  }

  if (!existingAddress?.id && normalizedOrderRecordId) {
    const linkedOrderMatches = await listAddressesByLinkedOrder(normalizedOrderRecordId);
    existingAddress = linkedOrderMatches[0] || null;
  }

  const existingCustomerIds = getLinkedIds(
    existingAddress,
    STUDIO_FIELDS.addresses.customer
  );
  const existingOrderIds = getLinkedIds(existingAddress, STUDIO_FIELDS.addresses.orders);

  const fields = {
    [STUDIO_FIELDS.addresses.fullAddress]: billingAddress.fullAddress || undefined,
    [STUDIO_FIELDS.addresses.customer]:
      uniqueLinkedRecordIds(existingCustomerIds, linkedCustomerIds).length > 0
        ? uniqueLinkedRecordIds(existingCustomerIds, linkedCustomerIds)
        : undefined,
    [STUDIO_FIELDS.addresses.orders]:
      uniqueLinkedRecordIds(existingOrderIds, normalizedOrderRecordId).length > 0
        ? uniqueLinkedRecordIds(existingOrderIds, normalizedOrderRecordId)
        : undefined,
    [STUDIO_FIELDS.addresses.firstName]: billingAddress.firstName || undefined,
    [STUDIO_FIELDS.addresses.lastName]: billingAddress.lastName || undefined,
    [STUDIO_FIELDS.addresses.shopifyId]: billingAddress.shopifyId || undefined,
    [STUDIO_FIELDS.addresses.streetAddress1]:
      billingAddress.streetAddress1 || undefined,
    [STUDIO_FIELDS.addresses.streetAddress2]:
      billingAddress.streetAddress2 || undefined,
    [STUDIO_FIELDS.addresses.townCity]: billingAddress.townCity || undefined,
    [STUDIO_FIELDS.addresses.county]: billingAddress.county || undefined,
    [STUDIO_FIELDS.addresses.postalCode]: billingAddress.postalCode || undefined,
    [STUDIO_FIELDS.addresses.country]: billingAddress.country || undefined,
    [STUDIO_FIELDS.addresses.phone]: billingAddress.phone || undefined,
  };

  if (existingAddress?.id) {
    return updateResilient(STUDIO_TABLES.addresses, existingAddress.id, {}, fields);
  }

  return createResilient(STUDIO_TABLES.addresses, {}, fields);
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
      customer_airtable_id: null,
      display_name: null,
      preview_url: null,
      front_label_url: null,
      sku: null,
      bottle: null,
      liquid: null,
      closure: null,
      wax: null,
      product: null,
      product_id: null,
      variant_id: null,
      session_id: null,
      quantity: 0,
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

const assignSignalText = (entry, key, value, maxLen = 255) => {
  if (!entry || entry[key]) return;
  const text = normalizeText(value, maxLen);
  if (text) entry[key] = text;
};

const assignSignalUrl = (entry, key, value) => {
  if (!entry || entry[key]) return;
  const url = sanitizeUrl(value);
  if (url) entry[key] = url;
};

const assignSignalRecordId = (entry, key, value) => {
  if (!entry || entry[key]) return;
  const recordId = normalizeRecordId(value);
  if (recordId) entry[key] = recordId;
};

const mergeOrderLineSignalMetadata = (entry, line = {}) => {
  if (!entry || !line || typeof line !== "object") return;
  const properties = line?.properties || {};

  assignSignalRecordId(
    entry,
    "customer_airtable_id",
    properties?._ss_customer_airtable_id
  );
  assignSignalText(entry, "session_id", properties?._session_id, 255);
  assignSignalText(entry, "display_name", properties?._display_name, 255);
  assignSignalUrl(entry, "preview_url", properties?._preview_url);
  assignSignalUrl(entry, "front_label_url", properties?._front_label_url);
  assignSignalText(entry, "sku", properties?._sku || line?.sku, 255);
  assignSignalText(entry, "bottle", properties?.bottle, 120);
  assignSignalText(entry, "liquid", properties?.liquid, 120);
  assignSignalText(entry, "closure", properties?.closure, 120);
  assignSignalText(entry, "wax", properties?.wax, 120);
  assignSignalText(entry, "product", line?.title, 255);
  assignSignalText(entry, "product_id", line?.product_id, 255);
  assignSignalText(entry, "variant_id", line?.variant_id, 255);
};

export const collectOrderSignals = async (orderPayload) => {
  const normalizedOrder = normalizeOrderWebhookPayload(orderPayload || {});
  const allSignals = Array.isArray(normalizedOrder?.order?.line_items)
    ? normalizedOrder.order.line_items
    : [];
  const signals = allSignals.filter((line) =>
    ALLOWED_STUDIO_ORDER_PRODUCT_IDS.has(
      normalizeShopifyProductId(line?.product_id) || ""
    )
  );
  const sessionOnlySignals = signals.filter(
    (line) => !normalizeRecordId(line?.properties?._saved_configuration_id)
  );

  const explicitCustomerRecordIds = uniqueRecordIds(
    signals.map((line) => line?.properties?._ss_customer_airtable_id)
  );

  const explicitSavedConfigIds = uniqueRecordIds(
    signals.map((line) => line?.properties?._saved_configuration_id)
  );

  const sessionIds = uniqueTextValues(
    sessionOnlySignals.map((line) => line?.properties?._session_id),
    255
  );

  const candidateCustomerIds = new Set(explicitCustomerRecordIds);
  const savedConfigSignals = new Map();
  const sessionSignalMap = new Map();

  signals.forEach((line) => {
    const properties = line?.properties || {};
    const savedConfigId = normalizeRecordId(properties._saved_configuration_id);
    const sessionId = normalizeText(properties._session_id, 255);
    const lineQuantity = normalizeOrderQuantity(line?.quantity) || 0;

    if (savedConfigId) {
      const entry = ensureSavedConfigSignalsEntry(savedConfigSignals, savedConfigId);
      addSignalVersionIds(entry, properties);
      mergeOrderLineSignalMetadata(entry, line);
      entry.quantity += lineQuantity;
    }

    if (!savedConfigId && sessionId) {
      if (!sessionSignalMap.has(sessionId)) {
        sessionSignalMap.set(sessionId, {
          _session_id: sessionId,
          _label_front_version_id: null,
          _label_back_version_id: null,
          _ss_customer_airtable_id: null,
          _display_name: null,
          _preview_url: null,
          _front_label_url: null,
          _sku: null,
          bottle: null,
          liquid: null,
          closure: null,
          wax: null,
          product: null,
          product_id: null,
          variant_id: null,
          _quantity: 0,
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
      if (!entry._ss_customer_airtable_id) {
        entry._ss_customer_airtable_id =
          normalizeRecordId(properties._ss_customer_airtable_id) || null;
      }
      if (!entry._display_name) {
        entry._display_name = normalizeText(properties._display_name, 255) || null;
      }
      if (!entry._preview_url) {
        entry._preview_url = sanitizeUrl(properties._preview_url) || null;
      }
      if (!entry._front_label_url) {
        entry._front_label_url = sanitizeUrl(properties._front_label_url) || null;
      }
      if (!entry._sku) {
        entry._sku = normalizeText(properties._sku || line?.sku, 255) || null;
      }
      if (!entry.bottle) {
        entry.bottle = normalizeText(properties.bottle, 120) || null;
      }
      if (!entry.liquid) {
        entry.liquid = normalizeText(properties.liquid, 120) || null;
      }
      if (!entry.closure) {
        entry.closure = normalizeText(properties.closure, 120) || null;
      }
      if (!entry.wax) {
        entry.wax = normalizeText(properties.wax, 120) || null;
      }
      if (!entry.product) {
        entry.product = normalizeText(line?.title, 255) || null;
      }
      if (!entry.product_id) {
        entry.product_id = normalizeText(line?.product_id, 255) || null;
      }
      if (!entry.variant_id) {
        entry.variant_id = normalizeText(line?.variant_id, 255) || null;
      }
      entry._quantity += lineQuantity;
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
      const sessionSignal = sessionSignalMap.get(sessionId);
      addSignalVersionIds(entry, sessionSignal);
      mergeOrderLineSignalMetadata(entry, {
        title: sessionSignal?.product,
        product_id: sessionSignal?.product_id,
        variant_id: sessionSignal?.variant_id,
        sku: sessionSignal?._sku,
        properties: {
          _session_id: sessionSignal?._session_id,
          _ss_customer_airtable_id: sessionSignal?._ss_customer_airtable_id,
          _display_name: sessionSignal?._display_name,
          _preview_url: sessionSignal?._preview_url,
          _front_label_url: sessionSignal?._front_label_url,
          _sku: sessionSignal?._sku,
          bottle: sessionSignal?.bottle,
          liquid: sessionSignal?.liquid,
          closure: sessionSignal?.closure,
          wax: sessionSignal?.wax,
        },
      });
      entry.quantity += normalizeOrderQuantity(sessionSignal?._quantity) || 0;

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
    customerRecordIds: uniqueRecordIds(Array.from(candidateCustomerIds)),
    savedConfigurationSignals: Array.from(savedConfigSignals.values()).map((entry) => ({
      saved_configuration_id: entry.saved_configuration_id,
      front_version_ids: uniqueRecordIds(Array.from(entry.front_version_ids)),
      back_version_ids: uniqueRecordIds(Array.from(entry.back_version_ids)),
      session_ids: uniqueTextValues(Array.from(entry.session_ids), 255),
      customer_airtable_id: normalizeRecordId(entry.customer_airtable_id),
      display_name: normalizeText(entry.display_name, 255),
      preview_url: sanitizeUrl(entry.preview_url) || null,
      front_label_url: sanitizeUrl(entry.front_label_url) || null,
      sku: normalizeText(entry.sku, 255),
      bottle: normalizeText(entry.bottle, 120),
      liquid: normalizeText(entry.liquid, 120),
      closure: normalizeText(entry.closure, 120),
      wax: normalizeText(entry.wax, 120),
      product: normalizeText(entry.product, 255),
      product_id: normalizeText(entry.product_id, 255),
      variant_id: normalizeText(entry.variant_id, 255),
      session_id:
        normalizeText(entry.session_id, 255) ||
        uniqueTextValues(Array.from(entry.session_ids), 255)[0] ||
        null,
      quantity: normalizeOrderQuantity(entry.quantity) || null,
    })),
  };
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
  orderStatus = "Ordered",
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
  const normalizedSavedConfigurationStatus =
    normalizeSavedConfigurationStatusFromOrderStatus(orderStatus) || "Ordered";

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
    [STUDIO_FIELDS.savedConfigurations.status]: normalizedSavedConfigurationStatus,
    ...buildLinkedPatch(
      savedConfigRecord,
      STUDIO_FIELDS.savedConfigurations.order,
      normalizeRecordId(orderRecordId),
      {
        fallbackFieldNames: STUDIO_FIELD_FALLBACKS.savedConfigurations.order,
      }
    ),
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
  savedConfigurationRecord = null,
  customerRecordIds = [],
  quantity = null,
  lineItemMetadata = null,
}) => {
  const savedConfigId = normalizeRecordId(savedConfigurationRecordId);
  if (!savedConfigId) return null;

  const savedConfigRecord =
    savedConfigurationRecord && savedConfigurationRecord.id === savedConfigId
      ? savedConfigurationRecord
      : await getRecordOrNull(STUDIO_TABLES.savedConfigurations, savedConfigId);
  if (!savedConfigRecord?.id) return null;

  const linkedCustomerIds = uniqueRecordIds(customerRecordIds);
  const normalizedOrderId = normalizeText(orderId, 255);
  const normalizedOrderStatus = normalizeOrderStatus(orderStatus) || "Ordered";
  const existingOrderRecords = normalizedOrderId
    ? await listAllRecords(STUDIO_TABLES.orders, {
        filterByFormula: `{${STUDIO_FIELDS.orders.orderId}}='${escapeFormulaValue(
          normalizedOrderId
        )}'`,
        maxRecords: 25,
      })
    : [];

  const fields = {
    [STUDIO_FIELDS.orders.orderId]: normalizedOrderId || undefined,
    [STUDIO_FIELDS.orders.customer]:
      linkedCustomerIds.length > 0 ? linkedCustomerIds : undefined,
    [STUDIO_FIELDS.orders.savedConfiguration]: [savedConfigId],
    [STUDIO_FIELDS.orders.orderStatus]: normalizedOrderStatus,
    ...buildOrderSnapshotFields(savedConfigRecord, { quantity, lineItemMetadata }),
  };

  const existingOrderRecord = Array.isArray(existingOrderRecords)
    ? existingOrderRecords.find((record) =>
        getLinkedIds(record, STUDIO_FIELDS.orders.savedConfiguration).includes(savedConfigId)
      ) || null
    : null;

  if (existingOrderRecord?.id) {
    return updateResilient(STUDIO_TABLES.orders, existingOrderRecord.id, {}, fields);
  }

  return createResilient(STUDIO_TABLES.orders, {}, fields);
};

const buildOrderCustomerName = (order = {}) =>
  pickText(
    [order?.customer?.first_name, order?.customer?.last_name].filter(Boolean).join(" "),
    [order?.billing_address?.firstName, order?.billing_address?.lastName]
      .filter(Boolean)
      .join(" "),
    [order?.shipping_address?.firstName, order?.shipping_address?.lastName]
      .filter(Boolean)
      .join(" "),
    order?.customer?.email,
    order?.email
  );

const buildOrderShippingAddress = (order = {}) =>
  pickText(
    order?.shipping_address?.fullAddress,
    order?.billing_address?.fullAddress
  );

export const buildShopifyOrderDetailsMetafieldPayload = ({
  order = {},
  orderStatus = null,
  items = [],
} = {}) => {
  const normalizedItems = Array.isArray(items)
    ? items.filter((item) => item && typeof item === "object")
    : [];

  return {
    schema_version: SHOPIFY_ORDER_DETAILS_SCHEMA_VERSION,
    synced_at: new Date().toISOString(),
    order_id: pickText(order?.id),
    order_name: pickText(order?.name),
    order_status: normalizeOrderStatus(orderStatus) || pickText(orderStatus),
    customer_name: buildOrderCustomerName(order),
    shipping_address: buildOrderShippingAddress(order),
    items: normalizedItems,
  };
};

export const buildShopifyOrderDetailsItem = ({
  order = {},
  savedConfigurationRecordId,
  savedConfigurationRecord,
  orderRecordId = null,
  customerRecordId = null,
  signal = {},
} = {}) => {
  const savedConfigId = normalizeRecordId(savedConfigurationRecordId);
  if (!savedConfigId || !savedConfigurationRecord?.id) return null;

  const snapshotFields = buildOrderSnapshotFields(savedConfigurationRecord, {
    quantity: signal?.quantity,
    lineItemMetadata: signal,
  });

  return {
    airtable_order_record_id: normalizeRecordId(orderRecordId),
    saved_configuration_id: savedConfigId,
    session_id:
      pickText(
        signal?.session_id,
        snapshotFields[STUDIO_FIELDS.orders.sessionId]
      ) || null,
    airtable_customer_id:
      normalizeRecordId(signal?.customer_airtable_id) ||
      normalizeRecordId(customerRecordId),
    order_id: pickText(order?.id),
    customer_name: buildOrderCustomerName(order),
    shipping_address: buildOrderShippingAddress(order),
    product:
      pickText(
        signal?.product,
        snapshotFields[STUDIO_FIELDS.orders.shopifyProduct]
      ) || null,
    product_id:
      pickText(
        signal?.product_id,
        snapshotFields[STUDIO_FIELDS.orders.shopifyProductId]
      ) || null,
    variant_id:
      pickText(
        signal?.variant_id,
        snapshotFields[STUDIO_FIELDS.orders.shopifyVariantId]
      ) || null,
    sku:
      pickText(
        signal?.sku,
        snapshotFields[STUDIO_FIELDS.orders.internalSku]
      ) || null,
    quantity:
      pickQuantity(
        signal?.quantity,
        snapshotFields[STUDIO_FIELDS.orders.quantity]
      ) || null,
    preview_image_url:
      pickUrl(
        signal?.preview_url,
        snapshotFields[STUDIO_FIELDS.orders.previewImageUrl]
      ) || null,
    front_label_url:
      pickUrl(
        signal?.front_label_url,
        snapshotFields[STUDIO_FIELDS.orders.frontLabelUrl]
      ) || null,
    front_label_version_id:
      normalizeRecordId(signal?.front_version_ids?.[0]) || null,
    back_label_version_id:
      normalizeRecordId(signal?.back_version_ids?.[0]) || null,
    display_name:
      pickText(
        signal?.display_name,
        snapshotFields[STUDIO_FIELDS.orders.displayName]
      ) || null,
    bottle:
      pickText(
        signal?.bottle,
        snapshotFields[STUDIO_FIELDS.orders.bottleSelection]
      ) || null,
    liquid:
      pickText(
        signal?.liquid,
        snapshotFields[STUDIO_FIELDS.orders.liquidSelection]
      ) || null,
    closure:
      pickText(
        signal?.closure,
        snapshotFields[STUDIO_FIELDS.orders.closureSelection]
      ) || null,
    wax:
      pickText(
        signal?.wax,
        snapshotFields[STUDIO_FIELDS.orders.waxSelection]
      ) || null,
  };
};
