// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";

let cache = { token: null, exp: 0 };

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

async function fetchZakekeToken({ accessType } = {}) {
  const now = Date.now();
  if (cache.token && cache.exp > now) {
    const ttl = Math.max(0, Math.floor((cache.exp - now) / 1000));
    return { token: cache.token, expires_in: ttl, cached: true };
  }

  const url = "https://api.zakeke.com/token";
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

  const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");

  // first try: Basic auth header + grant_type in body
  let res = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Accept-Language": "en-US",
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: `Basic ${credentials}`
    },
    body: "grant_type=client_credentials"
  });

  // fallback: some tenants require creds in body (no Basic)
  if (res.status === 401 || res.status === 415) {
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret
    });
    res = await fetch(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Accept-Language": "en-US",
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body
    });
  }

  const raw = await res.text();
  if (!res.ok) {
    console.error("Zakeke token request failed", { status: res.status, body: raw?.slice(0, 400) });
    throw { status: 502, code: `zakeke_http_${res.status}`, message: raw };
  }

  let data;
  try { data = JSON.parse(raw); }
  catch { throw { status: 502, code: "zakeke_non_json", message: raw.slice(0, 200) }; }

  const token = data["access-token"];
  const expires_in = Number(data.expires_in || 0);
  if (!token || !expires_in) {
    throw { status: 502, code: "zakeke_missing_fields", message: "No token/expires_in", data };
  }

  cache = { token, exp: now + (expires_in - 60) * 1000 };
  return { token, expires_in, cached: false };
}

export default withShopifyProxy(
  async (event) => {
    try {
      const urlObj = event?.url ? new URL(event.url) : null;
      const qs =
        urlObj
          ? Object.fromEntries(urlObj.searchParams.entries())
          : (event.queryStringParameters || {});
      const path = event?.path || urlObj?.pathname;

      const refresh = qs.refresh === "1" || qs.refresh === "true";
      const accessType = qs.access_type;

      console.log("get-zakeke-token hit", { path, qs });

      if (refresh) cache = { token: null, exp: 0 };
      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });
      return send(200, { token, expiresIn: expires_in, cached, accessType: accessType || null });
    } catch (e) {
      return send(e.status || 502, {
        error: e.code || "server_error",
        message: e.message || String(e),
        details: e.missing || e.data
      });
    }
  },
  { methods: ["GET", "POST"], allowlist: [process.env.SHOPIFY_STORE_DOMAIN], requireShop: true }
);