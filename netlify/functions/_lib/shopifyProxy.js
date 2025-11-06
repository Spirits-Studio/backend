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

    const originHeader = (() => {
      try {
        const h = isV2 ? arg.headers : (arg.headers || {});
        if (typeof h?.get === 'function') return h.get('origin') || h.get('Origin') || '';
        return h.origin || h.Origin || '';
      } catch {
        return '';
      }
    })();

    const respond = (status, body = "", headers = {}) =>
      new Response(typeof body === "string" ? body : JSON.stringify(body), {
        status,
        headers: { ...corsHeaders(originHeader), ...headers },
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

    const secret = process.env.SHOPIFY_API_SECRET || process.env.SHOPIFY_CLIENT_SECRET;
    if (!secret) {
      return respond(500, "Missing Shopify app secret (SHOPIFY_API_SECRET)");
    }

    const providedHex = String(provided).toLowerCase();

    const digestHex = crypto
      .createHmac("sha256", secret)
      .update(message, "utf8")
      .digest("hex");

    const a = Buffer.from(digestHex, "hex");
    const b = Buffer.from(providedHex, "hex");
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
        const cors = corsHeaders(originHeader);
        Object.keys(cors).forEach((k) => merged.set(k, cors[k]));
        merged.set('X-Proxy-Verified', '1');
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

function corsHeaders(origin) {
  const ALLOWED_ORIGINS = new Set([
    'https://spiritsstudio.co.uk',
    'https://www.spiritsstudio.co.uk',
    'https://wnbrmm-sg.myshopify.com',
    'http://127.0.0.1:9292',
    'https://127.0.0.1:9292',
  ]);

  const headers = {
    'Access-Control-Allow-Methods': 'GET,OPTIONS,POST',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Allow-Credentials': 'true',
    'Cache-Control': 'no-store',
  };

  if (origin && ALLOWED_ORIGINS.has(origin)) {
    headers['Access-Control-Allow-Origin'] = origin; // echo back exact origin
    headers['Vary'] = 'Origin';
  } else {
    // default to production site only
    headers['Access-Control-Allow-Origin'] = 'https://spiritsstudio.co.uk';
  }

  return headers;
}