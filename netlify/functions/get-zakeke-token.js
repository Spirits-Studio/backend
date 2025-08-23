// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";


async function fetchZakekeToken() {
  const url = 'https://api.zakeke.com/token';
  const params = new URLSearchParams();

  params.append('grant_type', 'client_credentials');
  params.append('client_id', process.env.ZAKEKE_CLIENT_ID);
  params.append('client_secret', process.env.ZAKEKE_CLIENT_SECRET);

  fetch(url, {
      method: 'POST',
      headers: {
          'Accept': 'application/json',
          'Accept-Language': 'en-US',
          'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: params
  })
  .then(response => response.json())
  .then(data => console.log(data))
  .catch(error => console.error('Error:', error));
}

export default withShopifyProxy(
  async (event) => {
    try {
      const qs = event.queryStringParameters || {};
      const refresh = qs.refresh === "1" || qs.refresh === "true";
      const accessType = qs.access_type; // e.g. S2S

      if (refresh) cache = { token: null, exp: 0 };

      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });

      // don’t log the token; just metadata
      console.log("zakeke token ok", { len: token.length, expires_in, cached, accessType });

      return jsonResponse(200, { token, expiresIn: expires_in, cached, accessType: accessType || null });
    } catch (e) {
      console.error("get-zakeke-token error", e);
      const status = Number.isInteger(e?.status) ? e.status : 502;
      return jsonResponse(status, {
        error: e?.code || "server_error",
        message: e?.message || String(e),
        details: e?.missing || e?.data
      });
    }
  },
  {
    methods: ["GET", "POST"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);