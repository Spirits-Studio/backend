import crypto from 'crypto';
import { createOne, updateOne, findOneBy } from '../../src/lib/airtable.js';

function verifyHmac(req, rawBody) {
  const digest = crypto
    .createHmac('sha256', process.env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, 'utf8')
    .digest('base64');
  return req.headers['x-shopify-hmac-sha256'] === digest;
}

export default async (req, res) => {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString('utf8') || '{}';

  if (!verifyHmac(req, raw)) {
    res.status(401).end('invalid hmac'); return;
  }

  const order = JSON.parse(raw);
  // find configId on each line item
  for (const li of order.line_items || []) {
    const props = (li.properties || []).reduce((a, p) => (a[p.name] = p.value, a), {});
    const cfg = props['Config ID'];
    if (!cfg) continue;

    const userEmail = order.email || order.customer?.email || null;
    // Optionally link to existing user by email
    // const userRec = await findOneBy(process.env.AIRTABLE_USERS_TABLE, 'email', userEmail);

    await createOne(process.env.AIRTABLE_ORDERS_TABLE, {
      orderId: String(order.id),
      shopifyOrderId: String(order.id),
      email: userEmail,
      configId: cfg,
      totalPrice: order.total_price,
      createdAt: new Date().toISOString()
    });

    // You may also update SavedConfigs.status = "ordered"
    // (if your table uses configId as primary key, adjust to update by record id)
  }

  res.status(200).end('ok');
};