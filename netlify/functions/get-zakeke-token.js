// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";


async function fetchZakekeToken() {  
const url = 'https://api.zakeke.com/token';
const clientId = process.env.ZAKEKE_CLIENT_ID
const clientSecret = process.env.ZAKEKE_CLIENT_SECRET
const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

fetch(url, {
    method: 'POST',
    headers: {
        'Accept': 'application/json',
        'Accept-Language': 'en-US',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${credentials}`,
    },
    body: 'grant_type=client_credentials'
})
.then(response => response.json())
.then(data => console.log(data))
.catch(error => console.error('Error:', error));

export default withShopifyProxy(
  async (event) => {
    try {
      const qs = event.queryStringParameters || {};
      const refresh = qs.refresh === "1" || qs.refresh === "true";
      const accessType = qs.access_type; // e.g. S2S

      if (refresh) cache = { token: null, exp: 0 };

      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });

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