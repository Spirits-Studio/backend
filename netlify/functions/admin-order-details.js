import {
  sendJson,
  parseBody,
  firstNonEmpty,
  normalizeRecordId,
  mapErrorResponse,
} from "./_lib/studio.js";
import { fetchOrderDetailsPayload } from "./_lib/orderDetails.js";

const DEV_ORIGINS = new Set([
  "http://127.0.0.1:9292",
  "https://127.0.0.1:9292",
]);

const isAllowedOrigin = (origin) => {
  const normalized = String(origin || "").trim();
  if (!normalized) return false;
  if (DEV_ORIGINS.has(normalized)) return true;
  return /^https:\/\/([a-z0-9-]+\.)?(shopify\.com|myshopify\.com|shopifycdn\.com)$/i.test(
    normalized
  );
};

const buildCorsHeaders = (origin) => {
  const headers = {
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
    "Access-Control-Max-Age": "86400",
    "Cache-Control": "no-store",
  };

  if (isAllowedOrigin(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Vary"] = "Origin";
  }

  return headers;
};

const sendCorsJson = (status, payload, origin) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...buildCorsHeaders(origin),
    },
  });

export default async (arg) => {
  const isV2 = arg && typeof arg.method === "string" && !("httpMethod" in arg);
  const method = (isV2 ? arg.method : arg.httpMethod || "").toUpperCase();
  const headers = isV2 ? arg.headers : arg.headers || {};
  const origin =
    (typeof headers?.get === "function"
      ? headers.get("origin") || headers.get("Origin")
      : headers.origin || headers.Origin) || "";

  if (method === "OPTIONS") {
    return new Response("", {
      status: 204,
      headers: buildCorsHeaders(origin),
    });
  }

  if (method !== "GET") {
    return sendCorsJson(
      405,
      { ok: false, error: "method_not_allowed" },
      origin
    );
  }

  try {
    let qs = {};
    if (isV2) {
      const url = new URL(arg.url);
      qs = Object.fromEntries(url.searchParams.entries());
    } else {
      qs = { ...(arg.queryStringParameters || {}) };
    }

    const body = (await parseBody(arg, method, isV2)) || {};
    const orderId = firstNonEmpty(qs.order_id, qs.orderId, body.order_id, body.orderId);
    const savedConfigurationId = normalizeRecordId(
      firstNonEmpty(
        qs.saved_configuration_id,
        qs.savedConfigurationId,
        body.saved_configuration_id,
        body.savedConfigurationId
      )
    );

    const payload = await fetchOrderDetailsPayload({
      orderId,
      savedConfigurationId,
    });

    return sendCorsJson(200, payload, origin);
  } catch (error) {
    return sendCorsJson(Number(error?.status || 500), mapErrorResponse(error), origin);
  }
};
