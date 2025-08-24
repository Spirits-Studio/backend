// netlify/functions/get-vistaCreate-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";

function send(status, obj, extraHeaders = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      ...extraHeaders
    }
  });
}

function getVistaCreateKey() {
  const key = process.env.VISTACREATE_API_KEY;
  if (!key) {
    throw {
      status: 500,
      code: "missing_vistacreate_env",
      message: "Missing VistaCreate API key",
      missing: { VISTACREATE_API_KEY: false }
    };
  }
  return key;
}

export default withShopifyProxy(async () => {
  console.log("get-vistaCreate-key hit");
  try {
    const key = getVistaCreateKey();
    console.log("withShopifyProxy get-vistaCreate-key token found", key );
    return send(200, { key });
    
  } catch (e) {
    console.log("withShopifyProxy get-vistaCreate-key error block hit", e);
    return send(e.status || 502, {
      error: e.code || "server_error",
      message: e.message || String(e),
      details: e.missing
    });
  }
}, {
  methods: ["GET", "POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true
});