// netlify/functions/_lib/shopifyProxy.js
import crypto from "crypto";

/** Higher‑order wrapper for App Proxy endpoints */
export function withShopifyProxy(handler, {
  methods = ["GET"],
  allowlist = [],
  requireShop = true,
} = {}) {
  return async (event) => {
    // Method guard (incl. CORS preflight)
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 204, headers: corsHeaders() };
    }
    if (!methods.includes(event.httpMethod)) {
      return { statusCode: 405, headers: corsHeaders(), body: "Method Not Allowed" };
    }

    // HMAC verify
    const qs = { ...(event.queryStringParameters || {}) };
    const provided = qs.signature || qs.hmac;
    if (!provided) return { statusCode: 401, headers: corsHeaders(), body: "Unauthorized" };
    delete qs.signature; delete qs.hmac;

    const message = Object.keys(qs).sort().map(k => `${k}=${qs[k] ?? ""}`).join("");
    const digestHex = crypto.createHmac("sha256", process.env.SHOPIFY_CLIENT_SECRET)
      .update(message, "utf8").digest("hex");

    const a = Buffer.from(digestHex, "hex");
    const b = Buffer.from(provided, "hex");
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      return { statusCode: 401, headers: corsHeaders(), body: "Unauthorized" };
    }

    // shop guard (optional)
    const shop = (qs.shop || "").toLowerCase();
    if (requireShop && !shop) {
      return { statusCode: 400, headers: corsHeaders(), body: "Missing shop" };
    }
    if (allowlist.length && !allowlist.includes(shop)) {
      return { statusCode: 403, headers: corsHeaders(), body: "Forbidden" };
    }

    // pass through to your handler, giving you the parsed shop
    try {
      const res = await handler(event, { shop, qs });
      // add CORS headers consistently
      if (res && res.headers) res.headers = { ...corsHeaders(), ...res.headers };
      return res || { statusCode: 204, headers: corsHeaders() };
    } catch (e) {
      return {
        statusCode: 500,
        headers: { ...corsHeaders(), "Content-Type": "application/json" },
        body: JSON.stringify({ error: String(e) })
      };
    }
  };
}

function corsHeaders() {
  return {
    // keep it strict; your calls come via proxy on your own domain
    "Access-Control-Allow-Origin": "https://barrelnbond.com",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "no-store",
  };
}