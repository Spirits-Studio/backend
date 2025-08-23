// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import fetch from 'node-fetch';

let cache = { token: null, exp: 0 };

async function fetchZakekeToken({ accessType } = {}) {
  const currentTimestamp = Math.floor(Date.now() / 1000);
  if (cache.token && cache.exp > currentTimestamp) {
    return { token: cache.token, expires_in: cache.exp - currentTimestamp, cached: true };
  }

  const client_id = process.env.ZAKEKE_CLIENT_ID;
  const client_secret = process.env.ZAKEKE_CLIENT_SECRET;
  const url = 'https://api.zakeke.com/token';

  if (!client_id || !client_secret) {
    throw {
      code: 'missing_zakeke_credentials',
      status: 500,
      message: 'Zakeke client ID or secret is not configured.'
    };
  }

  const grant_type = (accessType || process.env.ZAKEKE_ACCESS_TYPE || 'client_credentials').toLowerCase();
  const body = new URLSearchParams({ client_id, client_secret, grant_type });

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json'
    },
    body
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw {
      code: 'zakeke_auth_failed',
      status: response.status,
      message: 'Failed to obtain Zakeke token.',
      data: errorData
    };
  }

  const { access_token, expires_in } = await response.json();
  const exp = currentTimestamp + expires_in - 300; // Cache for 5 mins less than expiry

  cache = { token: access_token, exp };

  return { token: access_token, expires_in, cached: false };
}

function jsonResponse(status, bodyObj, extraHeaders = {}) {
  return {
    statusCode: status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
      ...extraHeaders
    },
    body: JSON.stringify(bodyObj)
  };
}

export default withShopifyProxy(
  async (event) => {
    try {
      console.log("Incoming event path:", event.path);
      console.log("Incoming query parameters:", event.queryStringParameters);

      const qs = event.queryStringParameters || {};
      const refresh = qs.refresh === '1' || qs.refresh === 'true';
      const accessType = qs.access_type;

      if (refresh) {
        cache = { token: null, exp: 0 };
        console.log("Refresh triggered: cache cleared");
      }

      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });

      console.log(`Returning token details: token length=${token ? token.length : 0}, expires_in=${expires_in}, cached=${cached}`);

      return jsonResponse(
        200,
        { token, expiresIn: expires_in, cached, accessType: (accessType || process.env.ZAKEKE_ACCESS_TYPE || null) }
      );
    } catch (e) {
      console.error("Error occurred:", e);

      const code = e?.code || 'server_error';
      const status = e?.status && Number.isInteger(e.status) ? e.status : 502;

      return jsonResponse(status, {
        error: code,
        message: e?.message || String(e),
        details: e?.missing || e?.data || undefined
      });
    }
  },
  {
    methods: ['GET', 'POST'],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);