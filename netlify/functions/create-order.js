import { createOne } from '../../src/lib/airtable.js';
import { json, parseJson } from '../../src/lib/utils.js';
import crypto from 'crypto';
import {
  normalizeRecordId,
  resolveCustomerRecordIdOrCreate,
  toLinkedRecordArray,
} from './_lib/studio.js';

export default async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, 204, {});
  if (req.method !== 'POST') return json(res, 405, {});

  try {
    const { userRecordId, configId, variantId, bottleCount, pricePack } = await parseJson(req);
    const providedCustomerRecordId = normalizeRecordId(userRecordId);
    if (!providedCustomerRecordId || !configId || !variantId) {
      return json(res, 400, { error: 'missing_params' });
    }

    const customerResolution = await resolveCustomerRecordIdOrCreate({
      providedCustomerRecordId,
      body: { userRecordId },
      qs: {},
      allowCreate: false,
      endpoint: "create-order",
    });
    const customerRecordId = normalizeRecordId(customerResolution?.customerRecordId);
    if (!customerRecordId) {
      return json(res, 409, {
        error: "customer_not_resolved",
        message:
          "Could not resolve Airtable customer record id for this request. Resolve identity via create-airtable-customer first.",
        provided_customer_record_id: providedCustomerRecordId,
      });
    }

    const orderId = crypto.randomUUID();
    const rec = await createOne(process.env.AIRTABLE_ORDERS_TABLE, {
      orderId, userId: toLinkedRecordArray(customerRecordId), configId, variantId, bottleCount,
      pricePack, createdAt: new Date().toISOString()
    });

    return json(res, 200, {
      orderId,
      recordId: rec.id,
      customer_record_id: customerRecordId,
      customer_record_recovered: Boolean(customerResolution?.recovered),
    });
  } catch (e) {
    return json(res, 500, { error: 'server_error', detail: String(e.message || e) });
  }
};
