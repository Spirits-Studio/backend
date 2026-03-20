import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  normalizeRecordId,
  sanitizeText,
  sanitizeUrl,
  getFieldValue,
  getLinkedIds,
  getRecordOrNull,
  listAllRecords,
} from "./studio.js";
import { escapeFormulaValue } from "../../../src/lib/airtable.js";

const CUSTOMER_FIELDS = {
  fullName: "Full Name",
  firstName: "First Name",
  lastName: "Last Name",
  email: "Email",
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

const firstText = (...values) => {
  for (const value of values) {
    const text = sanitizeText(value, 1000);
    if (text) return text;
  }
  return null;
};

const toPositiveInteger = (value) => {
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  const normalized = Math.floor(number);
  return normalized > 0 ? normalized : null;
};

export const buildSavedConfigurationLookupFormula = (savedConfigurationId) =>
  `FIND('${escapeFormulaValue(savedConfigurationId)}', ARRAYJOIN({${STUDIO_FIELDS.orders.savedConfiguration}}))`;

const resolveQuantity = (record, configJson) => {
  return (
    toPositiveInteger(getFieldValue(record, STUDIO_FIELDS.orders.quantity)) ||
    toPositiveInteger(configJson?.quantity) ||
    toPositiveInteger(configJson?.qty) ||
    toPositiveInteger(configJson?.order?.quantity) ||
    toPositiveInteger(configJson?.order?.qty) ||
    toPositiveInteger(configJson?.cart?.quantity) ||
    toPositiveInteger(configJson?.selectedQuantity) ||
    null
  );
};

const resolveCustomerName = (record) => {
  if (!record?.id) return null;
  return (
    firstText(
      getFieldValue(record, CUSTOMER_FIELDS.fullName),
      [
        getFieldValue(record, CUSTOMER_FIELDS.firstName),
        getFieldValue(record, CUSTOMER_FIELDS.lastName),
      ]
        .filter(Boolean)
        .join(" ")
    ) ||
    sanitizeText(getFieldValue(record, CUSTOMER_FIELDS.email), 255) ||
    null
  );
};

const resolveAddressText = (record) => {
  if (!record?.id) return null;
  return (
    firstText(
      getFieldValue(record, STUDIO_FIELDS.addresses.fullAddress),
      [
        getFieldValue(record, STUDIO_FIELDS.addresses.firstName),
        getFieldValue(record, STUDIO_FIELDS.addresses.lastName),
      ]
        .filter(Boolean)
        .join(" "),
      getFieldValue(record, STUDIO_FIELDS.addresses.streetAddress1),
      getFieldValue(record, STUDIO_FIELDS.addresses.streetAddress2),
      getFieldValue(record, STUDIO_FIELDS.addresses.townCity),
      getFieldValue(record, STUDIO_FIELDS.addresses.county),
      getFieldValue(record, STUDIO_FIELDS.addresses.postalCode),
      getFieldValue(record, STUDIO_FIELDS.addresses.country)
    ) || null
  );
};

const resolveLineItem = (record, customerRecordsById, addressRecordsById) => {
  const configJson = parseJsonField(
    getFieldValue(record, STUDIO_FIELDS.orders.configJson) || ""
  );
  const customerNames = getLinkedIds(record, STUDIO_FIELDS.orders.customer)
    .map((id) => resolveCustomerName(customerRecordsById.get(id)))
    .filter(Boolean);
  const shippingAddresses = getLinkedIds(record, STUDIO_FIELDS.orders.addresses)
    .map((id) => resolveAddressText(addressRecordsById.get(id)))
    .filter(Boolean);

  return {
    recordId: record.id,
    orderId: sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.orderId), 255) || null,
    customerName: customerNames[0] || null,
    shippingAddress: shippingAddresses.join("\n") || null,
    productName:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.shopifyProduct), 255) ||
      null,
    productId:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.shopifyProductId), 255) ||
      null,
    variantId:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.shopifyVariantId), 255) ||
      null,
    sku:
      sanitizeText(
        getFieldValue(record, STUDIO_FIELDS.orders.internalSku) ||
          getFieldValue(record, "SKU"),
        255
      ) || null,
    quantity: resolveQuantity(record, configJson),
    previewImageUrl:
      sanitizeUrl(
        getFieldValue(record, STUDIO_FIELDS.orders.previewImageUrl) ||
          configJson?.preview_url ||
          configJson?.previewUrl ||
          configJson?.previewImage ||
          configJson?.preview
      ) || null,
    displayName:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.displayName), 255) || null,
    bottleSelection:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.bottleSelection), 255) ||
      null,
    liquidSelection:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.liquidSelection), 255) ||
      null,
    closureSelection:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.closureSelection), 255) ||
      null,
    waxSelection:
      sanitizeText(getFieldValue(record, STUDIO_FIELDS.orders.waxSelection), 255) || null,
    frontLabelUrl:
      sanitizeUrl(
        getFieldValue(record, STUDIO_FIELDS.orders.frontLabelUrl) ||
          configJson?.selectedLabelVersion?.outputImageUrl ||
          configJson?.selectedLabelVersion?.outputS3Url
      ) || null,
  };
};

export const fetchOrderDetailsPayload = async ({
  orderId = null,
  savedConfigurationId = null,
} = {}) => {
  const normalizedOrderId = sanitizeText(orderId, 255) || null;
  const normalizedSavedConfigurationId = normalizeRecordId(savedConfigurationId);

  if (!normalizedOrderId && !normalizedSavedConfigurationId) {
    const error = new Error("order_id or saved_configuration_id is required.");
    error.status = 400;
    error.code = "missing_order_id";
    throw error;
  }

  const orderRecords = await listAllRecords(STUDIO_TABLES.orders, {
    filterByFormula: normalizedOrderId
      ? `{${STUDIO_FIELDS.orders.orderId}}='${escapeFormulaValue(normalizedOrderId)}'`
      : buildSavedConfigurationLookupFormula(normalizedSavedConfigurationId),
    maxRecords: 25,
  });

  if (!Array.isArray(orderRecords) || orderRecords.length === 0) {
    const error = new Error(
      normalizedOrderId
        ? "No order details were found for that order_id."
        : "No order details were found for that saved_configuration_id."
    );
    error.status = 404;
    error.code = "not_found";
    throw error;
  }

  const customerIds = Array.from(
    new Set(
      orderRecords.flatMap((record) =>
        getLinkedIds(record, STUDIO_FIELDS.orders.customer)
      )
    )
  );
  const addressIds = Array.from(
    new Set(
      orderRecords.flatMap((record) =>
        getLinkedIds(record, STUDIO_FIELDS.orders.addresses)
      )
    )
  );

  const [customerRecords, addressRecords] = await Promise.all([
    Promise.all(customerIds.map((id) => getRecordOrNull(STUDIO_TABLES.customers, id))),
    Promise.all(addressIds.map((id) => getRecordOrNull(STUDIO_TABLES.addresses, id))),
  ]);

  const customerRecordsById = new Map(
    customerRecords.filter(Boolean).map((record) => [record.id, record])
  );
  const addressRecordsById = new Map(
    addressRecords.filter(Boolean).map((record) => [record.id, record])
  );

  const records = orderRecords.map((record) =>
    resolveLineItem(record, customerRecordsById, addressRecordsById)
  );

  return {
    ok: true,
    orderId:
      normalizedOrderId ||
      sanitizeText(getFieldValue(orderRecords[0], STUDIO_FIELDS.orders.orderId), 255) ||
      null,
    savedConfigurationId: normalizedSavedConfigurationId || null,
    records,
  };
};
