// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";

// In-memory cache for the short-lived token
let cache = { token: null, exp: 0 };

function send(status, obj, extraHeaders = {}) {
  return {
    statusCode: status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      ...extraHeaders
    },
    body: JSON.stringify(obj)
  };
}

async function fetchZakekeToken({ accessType } = {}) {
  const now = Date.now();
  if (cache.token && cache.exp > now) {
    const ttl = Math.max(0, Math.floor((cache.exp - now) / 1000));
    return { token: cache.token, expires_in: ttl, cached: true };
  }

  const clientId = process.env.ZAKEKE_CLIENT_ID;
  const clientSecret = process.env.ZAKEKE_SECRET_KEY || process.env.ZAKEKE_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    const missing = {
      ZAKEKE_CLIENT_ID: !!clientId,
      ZAKEKE_SECRET_KEY: !!process.env.ZAKEKE_SECRET_KEY,
      ZAKEKE_CLIENT_SECRET: !!process.env.ZAKEKE_CLIENT_SECRET
    };
    throw { status: 500, code: "missing_zakeke_env", message: "Missing Zakeke env vars", missing };
  }

  // Per Zakeke docs: credentials in Basic Auth header, grant_type in x-www-form-urlencoded body
  const body = new URLSearchParams({ grant_type: "client_credentials" });
  if (accessType && /^(S2S|C2S)$/i.test(accessType)) {
    body.set("access_type", accessType.toUpperCase());
  }

  const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");

  const res = await fetch("https://api.zakeke.com/token", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Accept-Language": "en-US",
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: `Basic ${credentials}`
    },
    body
  });

  const raw = await res.text();

  if (!res.ok) {
    console.error("Zakeke token request failed", { status: res.status, body: raw?.slice(0, 400) });
    throw { status: 502, code: `zakeke_http_${res.status}`, message: raw };
  }

  let data;
  try { data = JSON.parse(raw); }
  catch { throw { status: 502, code: "zakeke_non_json", message: raw.slice(0, 200) }; }

  const token = data["access-token"]; // note: dash
  const expires_in = Number(data.expires_in || 0);
  if (!token || !expires_in) {
    throw { status: 502, code: "zakeke_missing_fields", message: "No token/expires_in", data };
  }

  // Cache with 60s safety skew
  cache = { token, exp: now + (expires_in - 60) * 1000 };

  return { token, expires_in, cached: false };
}

export default withShopifyProxy(
  async (event) => {
    try {
      const qs = event.queryStringParameters || {};
      const refresh = qs.refresh === "1" || qs.refresh === "true";
      const accessType = qs.access_type;

      console.log("get-zakeke-token hit", { path: event.path, qs });

      if (refresh) {
        cache = { token: null, exp: 0 };
        console.log("cache cleared due to refresh=1");
      }

      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });

      console.log("zakeke token ok", {
        len: token.length,
        expires_in,
        cached,
        accessType: accessType || null
      });

      return send(200, {
        token,
        expiresIn: expires_in,
        cached,
        accessType: accessType || null
      });
    } catch (e) {
      console.error("get-zakeke-token error", e);
      return send(e.status || 502, {
        error: e.code || "server_error",
        message: e.message || String(e),
        details: e.missing || e.data
      });
    }
  },
  {
    methods: ["GET", "POST"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);