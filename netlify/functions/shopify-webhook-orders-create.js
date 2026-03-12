import crypto from "crypto";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  normalizeRecordId,
  createResilient,
  updateResilient,
  getRecordOrNull,
  getLinkedIds,
  listAllRecords,
  buildLinkedCustomerFormula,
  resolveCustomerRecordIdOrCreate,
} from "./_lib/studio.js";

function verifyHmac(req, rawBody) {
  const digest = crypto
    .createHmac("sha256", process.env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, "utf8")
    .digest("base64");
  return req.headers["x-shopify-hmac-sha256"] === digest;
}

const getLineItemProperties = (lineItem) =>
  (lineItem?.properties || []).reduce((acc, pair) => {
    if (pair?.name) acc[pair.name] = pair.value;
    return acc;
  }, {});

const normalizeText = (value, maxLen = 255) => {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.slice(0, maxLen);
};

const normalizeEmail = (value) => {
  const text = normalizeText(value, 255);
  if (!text || !text.includes("@")) return null;
  return text.toLowerCase();
};

const normalizeShopifyCustomerId = (value) => {
  const text = normalizeText(value, 255);
  if (!text) return null;
  if (text.startsWith("rec")) return null;
  return text;
};

const uniqueLinkedIds = (raw) => {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const entry of raw) {
    const recId = normalizeRecordId(entry);
    if (recId && !out.includes(recId)) out.push(recId);
  }
  return out;
};

const replaceLinkedCustomerId = ({ linkedIds, fromCustomerId, toCustomerId }) => {
  const out = [];
  for (const id of linkedIds || []) {
    const next = id === fromCustomerId ? toCustomerId : id;
    if (next && !out.includes(next)) out.push(next);
  }
  return out;
};

const resolveOrderCustomerIdentity = (order) => {
  const customer = order?.customer || {};
  const billing = order?.billing_address || {};
  const shipping = order?.shipping_address || {};
  const shopifyId = normalizeShopifyCustomerId(
    customer?.admin_graphql_api_id ?? customer?.id ?? order?.customer?.id
  );
  const email = normalizeEmail(customer?.email ?? order?.email ?? order?.contact_email);
  const firstName = normalizeText(
    customer?.first_name ?? billing?.first_name ?? shipping?.first_name,
    120
  );
  const lastName = normalizeText(
    customer?.last_name ?? billing?.last_name ?? shipping?.last_name,
    120
  );
  const phone = normalizeText(customer?.phone ?? billing?.phone ?? shipping?.phone, 60);

  return {
    shopifyId,
    email,
    firstName,
    lastName,
    phone,
  };
};

const resolveCanonicalCustomerRecordId = async (order) => {
  const identity = resolveOrderCustomerIdentity(order);
  if (!identity.shopifyId && !identity.email) return null;

  const resolution = await resolveCustomerRecordIdOrCreate({
    body: {
      shopify_customer_id: identity.shopifyId || undefined,
      email: identity.email || undefined,
      first_name: identity.firstName || undefined,
      last_name: identity.lastName || undefined,
      phone: identity.phone || undefined,
    },
    qs: {},
    allowCreate: true,
    endpoint: "shopify-webhook-orders-create",
  });

  return normalizeRecordId(resolution?.customerRecordId);
};

const relinkTableCustomerField = async ({
  table,
  fieldName,
  fromCustomerId,
  toCustomerId,
}) => {
  if (!fromCustomerId || !toCustomerId || fromCustomerId === toCustomerId) return 0;

  let records = [];
  try {
    records = await listAllRecords(table, {
      filterByFormula: buildLinkedCustomerFormula(fieldName, fromCustomerId),
    });
  } catch (error) {
    console.warn("[webhook-orders-create] customer relink skipped for field", {
      table,
      fieldName,
      from_customer_id: fromCustomerId,
      to_customer_id: toCustomerId,
      status: error?.status,
      message: error?.message,
    });
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

const relinkCustomerHistory = async ({ fromCustomerId, toCustomerId }) => {
  if (!fromCustomerId || !toCustomerId || fromCustomerId === toCustomerId) {
    return 0;
  }

  const targets = [
    {
      table: STUDIO_TABLES.savedConfigurations,
      fields: [STUDIO_FIELDS.savedConfigurations.customer],
    },
    {
      table: STUDIO_TABLES.labels,
      fields: [STUDIO_FIELDS.labels.customers],
    },
  ];

  let touched = 0;
  for (const target of targets) {
    for (const fieldName of target.fields) {
      touched += await relinkTableCustomerField({
        table: target.table,
        fieldName,
        fromCustomerId,
        toCustomerId,
      });
    }
  }
  return touched;
};

export default async (req, res) => {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";

  if (!verifyHmac(req, raw)) {
    res.status(401).end("invalid hmac");
    return;
  }

  const order = JSON.parse(raw);
  const orderId = String(order?.id || "");
  const canonicalCustomerRecordId = await resolveCanonicalCustomerRecordId(order);
  const mergedCustomerPairs = new Set();
  let mergedRecordCount = 0;

  const touchedConfigIds = [];

  for (const lineItem of order?.line_items || []) {
    const props = getLineItemProperties(lineItem);
    const savedConfigurationRecordId = normalizeRecordId(
      props["_saved_configuration_id"] || props["Config ID"] || ""
    );
    if (!savedConfigurationRecordId) continue;

    touchedConfigIds.push(savedConfigurationRecordId);

    const savedConfig = await getRecordOrNull(
      STUDIO_TABLES.savedConfigurations,
      savedConfigurationRecordId
    );
    if (!savedConfig) continue;

    const linkedCustomerIds = getLinkedIds(
      savedConfig,
      STUDIO_FIELDS.savedConfigurations.customer
    );
    const effectiveCustomerIds =
      canonicalCustomerRecordId
        ? [canonicalCustomerRecordId]
        : linkedCustomerIds;

    if (canonicalCustomerRecordId) {
      for (const linkedCustomerId of linkedCustomerIds) {
        if (!linkedCustomerId || linkedCustomerId === canonicalCustomerRecordId) continue;
        const key = `${linkedCustomerId}->${canonicalCustomerRecordId}`;
        if (mergedCustomerPairs.has(key)) continue;
        mergedCustomerPairs.add(key);
        mergedRecordCount += await relinkCustomerHistory({
          fromCustomerId: linkedCustomerId,
          toCustomerId: canonicalCustomerRecordId,
        });
      }
    }

    // Airtable schema currently exposes a single valid choice for this field.
    const orderStatus = "Order Received";

    const orderRecord = await createResilient(
      STUDIO_TABLES.orders,
      {},
      {
        "Order ID": orderId || undefined,
        Customer: effectiveCustomerIds.length ? effectiveCustomerIds : undefined,
        "Saved Configuration": [savedConfigurationRecordId],
        "Order Status": orderStatus || undefined,
      }
    );

    const shouldPatchCanonicalCustomer =
      Boolean(canonicalCustomerRecordId) &&
      !(
        linkedCustomerIds.length === 1 &&
        linkedCustomerIds[0] === canonicalCustomerRecordId
      );

    await updateResilient(
      STUDIO_TABLES.savedConfigurations,
      savedConfigurationRecordId,
      {},
      {
        ...(shouldPatchCanonicalCustomer
          ? {
              [STUDIO_FIELDS.savedConfigurations.customer]: effectiveCustomerIds,
            }
          : {}),
        [STUDIO_FIELDS.savedConfigurations.status]: "Ordered",
        [STUDIO_FIELDS.savedConfigurations.order]: orderRecord?.id
          ? [orderRecord.id]
          : undefined,
      }
    );
  }

  res.status(200).json({
    ok: true,
    order_id: orderId,
    updated_saved_configurations: touchedConfigIds,
    canonical_customer_record_id: canonicalCustomerRecordId,
    merged_customer_pairs: Array.from(mergedCustomerPairs),
    merged_records_updated: mergedRecordCount,
  });
};
