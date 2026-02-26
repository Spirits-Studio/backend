import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  fetchZakekeToken,
  generateVisitorCode,
  clearZakekeTokenCache
} from "./_lib/zakekeAuth.js";

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

const parseBody = async (arg, method, isV2) => {
  if (!arg) return {};
  if (method === "GET") return {};

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
    console.warn("Failed to parse request body for get-zakeke-token", err);
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
    console.log("withShopifyProxy reached")
    try {
      console.log("withShopifyProxy try block reached")
      const urlObj = event?.url ? new URL(event.url) : null;
      const legacyQs =
        urlObj
          ? Object.fromEntries(urlObj.searchParams.entries())
          : (event?.queryStringParameters || {});
      const mergedQs = { ...legacyQs, ...qs };
      const path = event?.path || urlObj?.pathname;

      const body = await parseBody(event, method, isV2) || {};
      // Compatibility window: keep supporting the legacy nested payload shape.
      const hasNestedZakekeTokenBody =
        body &&
        typeof body === "object" &&
        body.zakekeTokenBody &&
        typeof body.zakekeTokenBody === "object" &&
        !Array.isArray(body.zakekeTokenBody);
      const nestedBody = hasNestedZakekeTokenBody ? body.zakekeTokenBody : {};
      if (hasNestedZakekeTokenBody) {
        console.warn(
          "[DEPRECATION] get-zakeke-token received nested `zakekeTokenBody`; send top-level `{ visitorcode, customercode }`.",
          { path }
        );
      }

      const refresh = mergedQs.refresh === "1" || mergedQs.refresh === "true";
      const accessType =
        mergedQs.access_type ||
        body.access_type ||
        body.accessType ||
        nestedBody.access_type ||
        nestedBody.accessType;

      const visitorcode = firstValue(
        body.visitorcode,
        body.visitorCode,
        body.visitor_code,
        nestedBody.visitorcode,
        nestedBody.visitorCode,
        nestedBody.visitor_code,
        mergedQs.visitorcode,
        mergedQs.visitorCode,
        mergedQs.visitor_code
      );

      const customercode = firstValue(
        body.customercode,
        body.customerCode,
        body.customer_code,
        nestedBody.customercode,
        nestedBody.customerCode,
        nestedBody.customer_code,
        mergedQs.customercode,
        mergedQs.customerCode,
        mergedQs.customer_code
      );

      const finalVisitorCode = visitorcode || generateVisitorCode();

      console.log("get-zakeke-token hit", {
        path,
        qs: mergedQs,
        hasBody: Object.keys(body).length > 0,
        visitorcode: visitorcode ? "provided" : "generated",
        customercode: !!customercode
      });

      if (refresh) {
        // Clear all cached variants
        const cleared = clearZakekeTokenCache();
        console.log("Zakeke token cache cleared", { cleared });
      }
      console.log("withShopifyProxy fetchZakekeToken reached")
      const {
        token,
        expires_in,
        cached,
        visitorcode: responseVisitor,
        customercode: responseCustomer
      } = await fetchZakekeToken({
        accessType,
        visitorcode: finalVisitorCode,
        customercode
      });
      console.log("withShopifyProxy fetchZakekeToken successful", {
        expires_in,
        cached,
        accessType,
        visitorcode: responseVisitor,
        customercode: responseCustomer
      });
      return send(200, {
        token,
        expiresIn: expires_in,
        cached,
        accessType: accessType || null,
        visitorCode: responseVisitor || finalVisitorCode,
        customerCode: responseCustomer || customercode || null
      });

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
