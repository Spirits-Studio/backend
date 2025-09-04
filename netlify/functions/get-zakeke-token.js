import { withShopifyProxy } from "./_lib/shopifyProxy.js";

// Cache tokens per variant (e.g., default vs S2S)
// Keyed by accessType uppercased or 'DEFAULT'
const cache = {};

function send(status, obj, extraHeaders = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json",
      // Extra no-cache headers to defeat aggressive proxies
      "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
      Pragma: "no-cache",
      Expires: "0",
      ...extraHeaders
    }
  });
}

async function fetchZakekeToken({ accessType } = {}) {
  const now = Date.now();
  const key = (accessType || "DEFAULT").toUpperCase();
  const entry = cache[key];
  if (entry && entry.token && entry.exp > now) {
    const ttl = Math.max(0, Math.floor((entry.exp - now) / 1000));
    return { token: entry.token, expires_in: ttl, cached: true };
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

  console.log("zakeke env present", {
    id_tail: (process.env.ZAKEKE_CLIENT_ID || "").slice(-4),
    hasSecret: !!(process.env.ZAKEKE_SECRET_KEY || process.env.ZAKEKE_CLIENT_SECRET)
  });

  const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");

  // Build base form body
  const baseForm = new URLSearchParams({ grant_type: "client_credentials" });
  if (accessType) baseForm.set("access_type", accessType);

  // first try: Basic auth header + form body
  let res = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Accept-Language": "en-US",
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: `Basic ${credentials}`
    },
    body: baseForm
  });

  // fallback: some tenants require creds in body (no Basic)
  if (res.status === 401 || res.status === 415) {
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret
    });
    if (accessType) body.set("access_type", accessType);
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

  console.log("Zakeke token response", { data });

  const token = data.access_token || data["access_token"];
  const expires_in = Number(data.expires_in || 0);
  if (!token || !expires_in) {
    throw { status: 502, code: "zakeke_missing_fields", message: "No token/expires_in", data };
  }

  cache[key] = { token, exp: now + Math.max(0, (expires_in - 60)) * 1000 };
  return { token, expires_in, cached: false, accessType: key };
}

export default withShopifyProxy(
  async (event) => {
    console.log("withShopifyProxy reached")
    try {
      console.log("withShopifyProxy try block reached")
      const urlObj = event?.url ? new URL(event.url) : null;
      const qs =
        urlObj
          ? Object.fromEntries(urlObj.searchParams.entries())
          : (event.queryStringParameters || {});
      const path = event?.path || urlObj?.pathname;

      const refresh = qs.refresh === "1" || qs.refresh === "true";
      const accessType = qs.access_type;

      console.log("get-zakeke-token hit", { path, qs });

      if (refresh) {
        // Clear all cached variants
        Object.keys(cache).forEach(k => delete cache[k]);
      }
      console.log("withShopifyProxy fetchZakekeToken reached")
      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });
      console.log("withShopifyProxy fetchZakekeToken successful", { expires_in, cached, accessType });
      return send(200, { token, expiresIn: expires_in, cached, accessType: accessType || null });

    } catch (e) {
      console.log("withShopifyProxy error block reached e", e)
      return send(e.status || 502, {
        error: e.code || "server_error",
        message: e.message || String(e),
        details: e.missing || e.data
      });
    }
  },
  { methods: ["GET", "POST"], allowlist: [process.env.SHOPIFY_STORE_DOMAIN], requireShop: true }
);
