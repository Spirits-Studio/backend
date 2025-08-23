// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from './_lib/shopifyProxy.js';

let cache = { token: null, exp: 0 };

function b64(str) {
  return Buffer.from(str, 'utf8').toString('base64');
}

async function fetchZakekeToken() {
  if (cache.token && Date.now() < cache.exp) {
    return { token: cache.token, expires_in: Math.floor((cache.exp - Date.now()) / 1000) };
  }

  const clientId = process.env.ZAKEKE_CLIENT_ID || '';
  const clientSecret = process.env.ZAKEKE_SECRET_KEY || '';
  if (!clientId || !clientSecret) throw new Error('missing_zakeke_env');

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    // Use S2S if you’ll call Orders/Compositions APIs with this token:
    // access_type: 'S2S'
  });

  const res = await fetch('https://api.zakeke.com/token', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${b64(`${clientId}:${clientSecret}`)}`
    },
    body
  });

  const raw = await res.text();
  if (!res.ok) {
    console.error('Zakeke token request failed', { status: res.status, body: raw });
    throw new Error(`zakeke_${res.status}`);
  }

  let json;
  try { json = JSON.parse(raw); } catch { throw new Error('zakeke_non_json'); }

  // field is "access-token" (dash), not "access_token"
  const token = json['access-token'];
  const ttl = json.expires_in;
  if (!token || !ttl) throw new Error('zakeke_missing_fields');

  const skew = 60;
  cache = { token, exp: Date.now() + (ttl - skew) * 1000 };
  return { token, expires_in: ttl };
}

export default withShopifyProxy(async () => {
  try {
    const { token, expires_in } = await fetchZakekeToken();
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token, expiresIn: expires_in })
    };
  } catch (e) {
    return {
      statusCode: 502,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: String(e) })
    };
  }
}, { methods: ['GET','POST'], allowlist: [process.env.SHOPIFY_STORE_DOMAIN], requireShop: true });