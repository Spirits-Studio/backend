import { createOne, updateOne } from '../../src/lib/airtable.js';
import { json, parseJson } from '../../src/lib/utils.js';
import crypto from 'crypto';
import { fetch } from 'undici';

export default async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, 204, {});
  if (req.method !== 'POST') return json(res, 405, {});

  try {
    const { configId = null, userRecordId, payload } = await parseJson(req);
    if (!userRecordId || !payload) return json(res, 400, { error: 'missing_params' });

    let id = configId || crypto.randomUUID();

    if (!configId) {
      const rec = await createOne(process.env.AIRTABLE_SAVED_CONFIGS_TABLE, {
        configId: id,
        userId: [userRecordId],
        status: 'draft',
        previewUrl: payload?.label?.previewUrl || null,
        zakekeState: JSON.stringify(payload?.zakeke || {}),
        vistaState: JSON.stringify(payload?.vista || {}),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      });
      if (process.env.MAKE_WEBHOOK_URL) {
        fetch(process.env.MAKE_WEBHOOK_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `cfg-${id}` },
          body: JSON.stringify({ event: 'config.saved', configId: id, userRecordId })
        }).catch(()=>{});
      }
      return json(res, 200, { configId: id, recordId: rec.id });
    } else {
      await updateOne(process.env.AIRTABLE_SAVED_CONFIGS_TABLE, configId, {
        previewUrl: payload?.label?.previewUrl || null,
        updatedAt: new Date().toISOString()
      });
      return json(res, 200, { configId: id });
    }
  } catch (e) {
    return json(res, 500, { error: 'server_error', detail: String(e.message || e) });
  }
};