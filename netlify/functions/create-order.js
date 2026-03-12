import crypto from 'crypto';
import {
  STUDIO_FIELDS,
  STUDIO_TABLES,
  createResilient,
  firstNonEmpty,
  normalizeRecordId,
  resolveCustomerRecordIdOrCreate,
  toLinkedRecordArray,
} from './_lib/studio.js';

const sendJson = (res, status, payload) => {
  res.status(status).json(payload);
};

const parseRequestJson = async (req) => {
  if (req?.body && typeof req.body === 'object') return req.body;
  if (typeof req?.body === 'string') {
    try {
      return JSON.parse(req.body || '{}');
    } catch {
      return {};
    }
  }

  const chunks = [];
  for await (const chunk of req || []) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString('utf8') || '{}';
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
};

const collectRecordIds = (value) => {
  const out = [];
  const push = (entry) => {
    const recId = normalizeRecordId(entry);
    if (recId && !out.includes(recId)) out.push(recId);
  };

  if (Array.isArray(value)) {
    value.forEach(push);
  } else {
    push(value);
  }
  return out;
};

export default async (req, res) => {
  if (req.method === 'OPTIONS') return sendJson(res, 204, {});
  if (req.method !== 'POST') return sendJson(res, 405, {});

  try {
    const body = await parseRequestJson(req);
    const providedCustomerRecordId = normalizeRecordId(
      firstNonEmpty(
        body.userRecordId,
        body.user_record_id,
        body.customer_record_id,
        body.customerRecordId
      )
    );
    const savedConfigurationRecordId = normalizeRecordId(
      firstNonEmpty(
        body.saved_configuration_record_id,
        body.savedConfigurationRecordId,
        body.configId,
        body.config_id
      )
    );
    if (!providedCustomerRecordId || !savedConfigurationRecordId) {
      return sendJson(res, 400, { error: 'missing_params' });
    }

    const customerResolution = await resolveCustomerRecordIdOrCreate({
      providedCustomerRecordId,
      body,
      qs: {},
      allowCreate: false,
      endpoint: "create-order",
    });
    const customerRecordId = normalizeRecordId(customerResolution?.customerRecordId);
    if (!customerRecordId) {
      return sendJson(res, 409, {
        error: "customer_not_resolved",
        message:
          "Could not resolve Airtable customer record id for this request. Resolve identity via create-airtable-customer first.",
        provided_customer_record_id: providedCustomerRecordId,
      });
    }

    const requestedOrderId =
      firstNonEmpty(body.order_id, body.orderId, body.shopify_order_id, body.shopifyOrderId) ||
      null;
    const orderId = requestedOrderId || crypto.randomUUID();
    const addressIds = collectRecordIds(
      body.address_record_ids || body.addressRecordIds || body.address_record_id || body.addressRecordId
    );

    const rec = await createResilient(
      STUDIO_TABLES.orders,
      {},
      {
        [STUDIO_FIELDS.orders.orderId]: orderId,
        [STUDIO_FIELDS.orders.customer]: toLinkedRecordArray(customerRecordId),
        [STUDIO_FIELDS.orders.savedConfiguration]:
          toLinkedRecordArray(savedConfigurationRecordId),
        [STUDIO_FIELDS.orders.orderStatus]: "Order Received",
        [STUDIO_FIELDS.orders.addresses]:
          addressIds.length ? toLinkedRecordArray(...addressIds) : undefined,
      }
    );

    return sendJson(res, 200, {
      orderId,
      recordId: rec.id,
      customer_record_id: customerRecordId,
      customer_record_recovered: Boolean(customerResolution?.recovered),
      saved_configuration_record_id: savedConfigurationRecordId,
    });
  } catch (e) {
    return sendJson(res, 500, { error: 'server_error', detail: String(e.message || e) });
  }
};
