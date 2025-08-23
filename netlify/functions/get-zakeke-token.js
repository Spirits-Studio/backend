
import { withShopifyProxy } from "./_lib/shopifyProxy.js";

function jsonResponse(status, bodyObj, extraHeaders = {}) {
  return new Response(JSON.stringify(bodyObj), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
      ...extraHeaders
    }
  });
}

export default withShopifyProxy(
  async (event) => {
    try {
      console.log("Incoming event path:", event.path);
      console.log("Incoming query parameters:", event.queryStringParameters);

      // Optional controls via query string:
      // ?refresh=1 -> bypass cache
      // ?access_type=S2S|C2S -> force access type
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
    // Only allow requests proxied from your Shopify store
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);