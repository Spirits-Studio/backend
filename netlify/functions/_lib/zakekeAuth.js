import crypto from "crypto";

const cache = {};

export const generateVisitorCode = () => crypto.randomUUID?.() || crypto.randomBytes(16).toString("hex");

export function clearZakekeTokenCache() {
  const keys = Object.keys(cache);
  keys.forEach((k) => delete cache[k]);
  return keys.length;
}

export async function fetchZakekeToken({ accessType, visitorcode, customercode } = {}) {
  const now = Date.now();
  const keyParts = [
    (accessType || "DEFAULT").toUpperCase(),
    visitorcode ? `V:${visitorcode}` : null,
    customercode ? `C:${customercode}` : null,
  ].filter(Boolean);
  const key = keyParts.length ? keyParts.join("|") : "DEFAULT";
  const entry = cache[key];
  if (entry && entry.token && entry.exp > now) {
    const ttl = Math.max(0, Math.floor((entry.exp - now) / 1000));
    return {
      token: entry.token,
      expires_in: ttl,
      cached: true,
      visitorcode: entry.visitorcode,
      customercode: entry.customercode,
      accessType: entry.accessType,
    };
  }

  const url = "https://api.zakeke.com/token";
  const clientId = process.env.ZAKEKE_CLIENT_ID;
  const clientSecret = process.env.ZAKEKE_SECRET_KEY || process.env.ZAKEKE_CLIENT_SECRET;

  if (!clientId || !clientSecret) {
    const missing = {
      ZAKEKE_CLIENT_ID: !!clientId,
      ZAKEKE_SECRET_KEY: !!process.env.ZAKEKE_SECRET_KEY,
      ZAKEKE_CLIENT_SECRET: !!process.env.ZAKEKE_CLIENT_SECRET,
    };
    throw { status: 500, code: "missing_zakeke_env", message: "Missing Zakeke env vars", missing };
  }

  console.log("zakeke env present", {
    id_tail: (process.env.ZAKEKE_CLIENT_ID || "").slice(-4),
    hasSecret: !!(process.env.ZAKEKE_SECRET_KEY || process.env.ZAKEKE_CLIENT_SECRET),
  });

  const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");

  const baseForm = new URLSearchParams({ grant_type: "client_credentials" });
  if (accessType) baseForm.set("access_type", accessType);
  if (visitorcode) baseForm.set("visitorcode", visitorcode);
  if (customercode) baseForm.set("customercode", customercode);

  let res = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Accept-Language": "en-US",
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: `Basic ${credentials}`,
    },
    body: baseForm,
  });

  if (res.status === 401 || res.status === 415) {
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
    });
    if (accessType) body.set("access_type", accessType);
    if (visitorcode) body.set("visitorcode", visitorcode);
    if (customercode) body.set("customercode", customercode);
    res = await fetch(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Accept-Language": "en-US",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
    });
  }

  const raw = await res.text();
  if (!res.ok) {
    console.error("Zakeke token request failed", { status: res.status, body: raw?.slice(0, 400) });
    throw { status: 502, code: `zakeke_http_${res.status}`, message: raw };
  }

  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    throw { status: 502, code: "zakeke_non_json", message: raw.slice(0, 200) };
  }

  console.log("Zakeke token response", { data });

  const token = data.access_token || data["access_token"] || data["access-token"];
  const expires_in = Number(data.expires_in || 0);
  if (!token || !expires_in) {
    throw { status: 502, code: "zakeke_missing_fields", message: "No token/expires_in", data };
  }

  cache[key] = {
    token,
    exp: now + Math.max(0, (expires_in - 60)) * 1000,
    visitorcode,
    customercode,
    accessType: (accessType || "DEFAULT").toUpperCase(),
  };

  return {
    token,
    expires_in,
    cached: false,
    accessType: (accessType || "DEFAULT").toUpperCase(),
    visitorcode,
    customercode,
  };
}
