// netlify/functions/_lib/shopifyProxy.js
import crypto from "crypto";

/**
 * Higher‑order wrapper for Shopify App Proxy endpoints.
 * Works in Netlify Functions v2 (Request) and v1 (event) and ALWAYS returns a Web Response.
 */
export function withShopifyProxy(
  handler,
  { methods = ["GET"], allowlist = [], requireShop = true } = {}
) {
  return async (arg, extra) => {
    const isV2 = arg && typeof arg.method === "string" && !("httpMethod" in arg);

    const respond = (status, body = "", headers = {}) =>
      new Response(typeof body === "string" ? body : JSON.stringify(body), {
        status,
        headers: { ...corsHeaders(), ...headers },
      });

    // Method + OPTIONS guard
    const method = (isV2 ? arg.method : arg.httpMethod || "").toUpperCase();
    if (method === "OPTIONS") return respond(204);
    if (!methods.includes(method)) return respond(405, "Method Not Allowed");

    // Query params
    let qs = {};
    try {
      if (isV2) {
        const url = new URL(arg.url);
        qs = Object.fromEntries(url.searchParams.entries());
      } else {
        qs = { ...(arg.queryStringParameters || {}) };
      }
    } catch {}

    // HMAC verify
    const provided = qs.signature || qs.hmac;
    if (!provided) return respond(401, "Unauthorized");
    delete qs.signature; delete qs.hmac;

    const message = Object.keys(qs).sort().map(k => `${k}=${qs[k] ?? ""}`).join("");
    const digestHex = crypto
      .createHmac("sha256", process.env.SHOPIFY_CLIENT_SECRET)
      .update(message, "utf8")
      .digest("hex");

    const a = Buffer.from(digestHex, "hex");
    const b = Buffer.from(provided, "hex");
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      return respond(401, "Unauthorized");
    }

    // Shop guard
    const shop = (qs.shop || "").toLowerCase();
    if (requireShop && !shop) return respond(400, "Missing shop");
    if (allowlist.length && !allowlist.includes(shop)) return respond(403, "Forbidden");

    try {
      // Call handler. It may return a Response (preferred) or a v1-style object.
      const res = await handler(arg, { shop, qs, isV2, method });

      if (res instanceof Response) {
        // Merge CORS headers
        const merged = new Headers(res.headers);
        const cors = corsHeaders();
        Object.keys(cors).forEach((k) => merged.set(k, cors[k]));
        const body = await res.text();
        return new Response(body, { status: res.status, headers: merged });
      }

      if (res && typeof res === "object" && "statusCode" in res) {
        return respond(res.statusCode, res.body || "", res.headers || {});
      }

      return respond(204);
    } catch (e) {
      return respond(
        500,
        { error: String(e) },
        { "Content-Type": "application/json" }
      );
    }
  };
}

function corsHeaders() {
  return {
    // keep it strict; your calls come via proxy on your own domain
    "Access-Control-Allow-Origin": "https://barrelnbond.com",
    "Access-Control-Allow-Methods": "GET,OPTIONS,POST",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "no-store",
  };
}