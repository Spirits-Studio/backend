// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";

let cached = { token: null, exp: 0 };

async function fetchZakekeToken() {
  if (cached.token && Date.now() < cached.exp) {
    return { access_token: cached.token, expires_in: Math.floor((cached.exp - Date.now()) / 1000) };
  }

  const res = await fetch("https://api.zakeke.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: process.env.ZAKEKE_CLIENT_ID || "",
      client_secret: process.env.ZAKEKE_SECRET_KEY || "",
      grant_type: "client_credentials",
      scope: "api"
    })
  });

  const text = await res.text();
  if (!res.ok) {
    console.error("Zakeke token request failed", { status: res.status, body: text });
    throw new Error(`zakeke_token_failed status=${res.status}`);
  }

  const data = JSON.parse(text); // { access_token, token_type, expires_in }
  const skew = 60;
  cached = { token: data.access_token, exp: Date.now() + Math.max(0, data.expires_in - skew) * 1000 };
  return data;
}

export const handler = withShopifyProxy(
  async (_event, { shop }) => {
    // (shop is verified & allow‑listed already)
    const data = await fetchZakekeToken();
    console.log("data in withShopifyProxy", data);
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: data.access_token, expiresIn: data.expires_in, shop })
    };
  },
  {
    methods: ["GET", "POST"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN], // e.g. barrelnbond.myshopify.com
    requireShop: true
  }
);