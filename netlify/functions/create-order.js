import { createOne } from '../../src/lib/airtable.js';
import { json, parseJson } from '../../src/lib/utils.js';
import crypto from 'crypto';

export default async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, 204, {});
  if (req.method !== 'POST') return json(res, 405, {});

  try {
    const { userRecordId, configId, variantId, bottleCount, pricePack } = await parseJson(req);
    if (!userRecordId || !configId || !variantId) return json(res, 400, { error: 'missing_params' });

    const orderId = crypto.randomUUID();
    const rec = await createOne(process.env.AIRTABLE_ORDERS_TABLE, {
      orderId, userId: [userRecordId], configId, variantId, bottleCount,
      pricePack, createdAt: new Date().toISOString()
    });

    return json(res, 200, { orderId, recordId: rec.id });
  } catch (e) {
    return json(res, 500, { error: 'server_error', detail: String(e.message || e) });
  }
};