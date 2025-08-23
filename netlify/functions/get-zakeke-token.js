// netlify/functions/get-zakeke-token.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";

let cache = { token: null, exp: 0 };

function jsonResponse(status, bodyObj, extraHeaders = {}) {
  return new Response(JSON.stringify(bodyObj), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      ...extraHeaders
    }
  });
}

function basicAuth(id, secret) {
  return "Basic " + Buffer.from(`${id}:${secret}`, "utf8").toString("base64");
}

async function fetchZakekeToken({ accessType } = {}) {
  const now = Date.now();
  if (cache.token && cache.exp > now) {
    const ttl = Math.max(0, Math.floor((cache.exp - now) / 1000));
    return { token: cache.token, expires_in: ttl, cached: true };
  }

  const clientId = process.env.ZAKEKE_CLIENT_ID;
  const clientSecret = process.env.ZAKEKE_SECRET_KEY || process.env.ZAKEKE_CLIENT_SECRET; // support your current var

  if (!clientId || !clientSecret) {
    const missing = { ZAKEKE_CLIENT_ID: !!clientId, ZAKEKE_SECRET: !!clientSecret };
    throw { code: "missing_zakeke_env", status: 500, message: "Missing Zakeke creds", missing };
  }

  const body = new URLSearchParams({ grant_type: "client_credentials" });
  // Optional: access_type=S2S|C2S (NOT part of grant_type)
  if (accessType && /^(S2S|C2S)$/i.test(accessType)) {
    body.set("access_type", accessType.toUpperCase());
  }

  const res = await fetch("https://api.zakeke.com/token", {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: basicAuth(clientId, clientSecret)
    },
    body
  });

  const raw = await res.text();
  if (!res.ok) {
    console.error("Zakeke token fail", res.status, raw.slice(0, 400));
    throw { code: `zakeke_http_${res.status}`, status: 502, message: raw };
  }

  let json;
  try { json = JSON.parse(raw); } catch { throw { code: "zakeke_non_json", status: 502, message: raw.slice(0, 200) }; }

  const token = json["access-token"]; // <-- dash!
  const expires_in = Number(json.expires_in || 0);
  if (!token || !expires_in) {
    throw { code: "zakeke_missing_fields", status: 502, message: "No token/expires_in", data: json };
  }

  // cache with 60s skew
  cache = { token, exp: now + (expires_in - 60) * 1000 };
  return { token, expires_in, cached: false };
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