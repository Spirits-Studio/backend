import { findOneBy, createOne } from '../../src/lib/airtable.js';
import { json, parseJson } from '../../src/lib/utils.js';

export default async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, 204, {});
  if (req.method !== 'POST') return json(res, 405, {});

  try {
    const { email = null, shopifyCustomerId = null, visitorId = null } = await parseJson(req);

    let rec = null;
    if (visitorId) rec = await findOneBy(process.env.AIRTABLE_USERS_TABLE, 'visitorId', visitorId);
    if (!rec && email) rec = await findOneBy(process.env.AIRTABLE_USERS_TABLE, 'email', email);

    if (rec) return json(res, 200, { recordId: rec.id, reused: true });

    const created = await createOne(process.env.AIRTABLE_USERS_TABLE, {
      email, shopifyCustomerId, visitorId,
      isGuest: !shopifyCustomerId,
      createdAt: new Date().toISOString(),
      lastSeen: new Date().toISOString()
    });

    return json(res, 200, { recordId: created.id, reused: false });
  } catch (e) {
    return json(res, 500, { error: 'server_error', detail: String(e.message || e) });
  }
};