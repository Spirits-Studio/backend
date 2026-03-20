import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  sendJson,
  parseBody,
  firstNonEmpty,
  sanitizeText,
  sanitizeUrl,
  getFieldValue,
  getLinkedIds,
  getRecordOrNull,
  listAllRecords,
  mapErrorResponse,
} from "./_lib/studio.js";
import { escapeFormulaValue } from "../../src/lib/airtable.js";

const STAFF_EMAIL_SUFFIX = "@spiritsstudio.co.uk";
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

const normalizeStaffEmail = (value) => {
  const text = String(value || "").trim().toLowerCase();
  return text || null;
};

const isStaffEmail = (value) => {
  const email = normalizeStaffEmail(value);
  return Boolean(email && email.endsWith(STAFF_EMAIL_SUFFIX));
};

const toPositiveInteger = (value) => {
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  const normalized = Math.floor(number);
  return normalized > 0 ? normalized : null;
};

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

export default withShopifyProxy(async (arg, { qs, isV2, method }) => {
  try {
    const body = (await parseBody(arg, method, isV2)) || {};
    const staffEmail = normalizeStaffEmail(qs.logged_in_customer_email);
    const orderId = sanitizeText(
      firstNonEmpty(qs.order_id, qs.orderId, body.order_id, body.orderId),
      255
    );

    if (!isStaffEmail(staffEmail)) {
      return sendJson(403, {
        ok: false,
        error: "forbidden",
        message: "Only signed-in Spirits Studio staff can view order details.",
      });
    }

    if (!orderId) {
      return sendJson(400, {
        ok: false,
        error: "missing_order_id",
        message: "order_id is required.",
      });
    }

    const orderRecords = await listAllRecords(STUDIO_TABLES.orders, {
      filterByFormula: `{${STUDIO_FIELDS.orders.orderId}}='${escapeFormulaValue(orderId)}'`,
      maxRecords: 25,
    });

    if (!Array.isArray(orderRecords) || orderRecords.length === 0) {
      return sendJson(404, {
        ok: false,
        error: "not_found",
        message: "No order details were found for that order_id.",
      });
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

    return sendJson(200, {
      ok: true,
      orderId,
      records,
    });
  } catch (error) {
    return sendJson(Number(error?.status || 500), mapErrorResponse(error));
  }
});
