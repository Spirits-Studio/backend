import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { fetchZakekeToken } from "./_lib/zakekeAuth.js";

const send = (status, obj) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
      Pragma: "no-cache",
      Expires: "0",
    },
  });

const parseBody = async (arg, method, isV2) => {
  if (!arg || method === "GET") return {};
  try {
    if (isV2) {
      const ct = (arg.headers.get("content-type") || "").toLowerCase();
      if (ct.includes("application/json")) return await arg.json();
      if (ct.includes("application/x-www-form-urlencoded")) {
        const fd = await arg.formData();
        return Object.fromEntries([...fd.entries()]);
      }
      const text = await arg.text();
      if (!text) return {};
      try { return JSON.parse(text); } catch { return {}; }
    } else {
      const ct = (arg.headers?.["content-type"] || "").toLowerCase();
      const raw = arg.body || "";
      if (!raw) return {};
      if (ct.includes("application/json")) return JSON.parse(raw);
      if (ct.includes("application/x-www-form-urlencoded")) {
        return Object.fromEntries(new URLSearchParams(raw));
      }
      try { return JSON.parse(raw); } catch { return {}; }
    }
  } catch (err) {
    console.warn("Failed to parse request body for link-zakeke-identity", err);
  }
  return {};
};

const firstValue = (...vals) => {
  for (const val of vals) {
    if (typeof val === "string" && val.trim()) return val.trim();
  }
  return undefined;
};

export default withShopifyProxy(
  async (event, { qs = {}, isV2, method }) => {
    try {
      const urlObj = event?.url ? new URL(event.url) : null;
      const legacyQs =
        urlObj
          ? Object.fromEntries(urlObj.searchParams.entries())
          : (event?.queryStringParameters || {});
      const mergedQs = { ...legacyQs, ...qs };

      const body = await parseBody(event, method, isV2) || {};

      const visitorcode = firstValue(
        body.visitorcode,
        body.visitorCode,
        body.visitor_code,
        mergedQs.visitorcode,
        mergedQs.visitorCode,
        mergedQs.visitor_code
      );

      const customercode = firstValue(
        body.customercode,
        body.customerCode,
        body.customer_code,
        mergedQs.customercode,
        mergedQs.customerCode,
        mergedQs.customer_code
      );

      if (!visitorcode || !customercode) {
        return send(400, {
          ok: false,
          error: "missing_parameters",
          message: "visitorcode and customercode are required to link identities",
        });
      }

      const accessType = firstValue(
        body.access_type,
        body.accessType,
        mergedQs.access_type,
        mergedQs.accessType
      ) || "S2S";

      const result = await fetchZakekeToken({
        accessType,
        visitorcode,
        customercode,
      });

      return send(200, {
        ok: true,
        linked: true,
        visitorCode: visitorcode,
        customerCode: customercode,
        token: result.token,
        expiresIn: result.expires_in,
        cached: result.cached,
        accessType: result.accessType || accessType,
      });
    } catch (e) {
      console.error("link-zakeke-identity error", e);
      return send(e.status || 502, {
        ok: false,
        error: e.code || "server_error",
        message: e.message || String(e),
        details: e.missing || e.data,
      });
    }
  },
  {
    methods: ["POST"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true,
  }
);
