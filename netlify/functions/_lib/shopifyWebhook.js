import crypto from "crypto";
import { findOneBy } from "../../../src/lib/airtable.js";
import { createResilient, updateResilient } from "./studio.js";

export const SHOPIFY_WEBHOOK_HEADERS = {
  topic: "x-shopify-topic",
  shopDomain: "x-shopify-shop-domain",
  webhookId: "x-shopify-webhook-id",
  hmac: "x-shopify-hmac-sha256",
};

export const getWebhookEventsTable = () =>
  process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID ||
  process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE ||
  null;

const IDEMPOTENCY_STATUS = {
  received: "received",
  processed: "processed",
  skipped: "skipped",
  error: "error",
};

const terminalIdempotentStatuses = new Set([
  IDEMPOTENCY_STATUS.processed,
  IDEMPOTENCY_STATUS.skipped,
]);

const normalizeIdempotencyStatus = (value) => {
  const text = String(value || "")
    .trim()
    .toLowerCase();
  if (!text) return null;
  return text;
};

const webhookLogDefaults = {
  topic: null,
  webhook_id: null,
  shop_domain: null,
  order_id: null,
  shopify_customer_id: null,
  email: null,
  canonical_customer_record_id: null,
  merge_candidates_count: 0,
  relinked_saved_configs: 0,
  relinked_labels: 0,
  relinked_orders: 0,
  idempotent_skip: false,
  status: null,
  error: null,
};

const debugFlagEnabled = (value) =>
  String(value || "")
    .trim()
    .toLowerCase() === "true";

const toPlainHeaderValue = (value) => {
  if (Array.isArray(value)) return String(value[0] ?? "").trim();
  return String(value ?? "").trim();
};

export const normalizeWebhookHeaders = (headersInput = {}) => {
  const out = {};

  if (typeof headersInput?.forEach === "function") {
    headersInput.forEach((value, key) => {
      out[String(key || "").toLowerCase()] = toPlainHeaderValue(value);
    });
    return out;
  }

  Object.entries(headersInput || {}).forEach(([key, value]) => {
    out[String(key || "").toLowerCase()] = toPlainHeaderValue(value);
  });
  return out;
};

const isWebRequestLike = (req) =>
  Boolean(req) &&
  typeof req?.text === "function" &&
  typeof req?.headers?.get === "function";

const hasAsyncIterator = (value) =>
  Boolean(value) && typeof value[Symbol.asyncIterator] === "function";

export const readWebhookRawBody = async (req) => {
  if (isWebRequestLike(req)) {
    const text = await req.text();
    return String(text || "");
  }

  if (typeof req?.body === "string") return req.body;
  if (Buffer.isBuffer(req?.body)) return req.body.toString("utf8");
  if (typeof req?.rawBody === "string") return req.rawBody;
  if (Buffer.isBuffer(req?.rawBody)) return req.rawBody.toString("utf8");
  if (req?.isBase64Encoded && typeof req?.body === "string") {
    return Buffer.from(req.body, "base64").toString("utf8");
  }
  if (req?.body && typeof req.body === "object") {
    return JSON.stringify(req.body);
  }

  const chunks = [];
  if (!hasAsyncIterator(req)) return "";

  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk || "")));
  }
  return Buffer.concat(chunks).toString("utf8");
};

export const parseWebhookEnvelope = async (req) => {
  const headers = normalizeWebhookHeaders(
    req?.headers || req?.multiValueHeaders || {}
  );
  const rawBody = (await readWebhookRawBody(req)) || "";

  let payload = {};
  let parseError = null;
  if (rawBody) {
    try {
      payload = JSON.parse(rawBody);
    } catch (error) {
      parseError = error;
      payload = {};
    }
  }

  const topic = headers[SHOPIFY_WEBHOOK_HEADERS.topic] || null;
  const shopDomain = headers[SHOPIFY_WEBHOOK_HEADERS.shopDomain] || null;
  const webhookId = headers[SHOPIFY_WEBHOOK_HEADERS.webhookId] || null;
  const hmac = headers[SHOPIFY_WEBHOOK_HEADERS.hmac] || null;

  return {
    headers,
    rawBody,
    payload,
    parseError,
    topic,
    shop_domain: shopDomain,
    webhook_id: webhookId,
    hmac,
    received_at: new Date().toISOString(),
  };
};

const toBase64Buffer = (value) => {
  try {
    return Buffer.from(String(value || ""), "base64");
  } catch {
    return Buffer.from("");
  }
};

export const verifyWebhookHmac = ({
  rawBody,
  providedHmac,
  secret = process.env.SHOPIFY_WEBHOOK_SECRET,
}) => {
  if (!secret || !providedHmac) return false;

  const digest = crypto
    .createHmac("sha256", secret)
    .update(rawBody || "", "utf8")
    .digest("base64");

  const expected = toBase64Buffer(digest);
  const provided = toBase64Buffer(providedHmac);
  if (!expected.length || expected.length !== provided.length) return false;

  return crypto.timingSafeEqual(expected, provided);
};

export const shouldLogWebhookVerificationDebug = () =>
  debugFlagEnabled(process.env.SHOPIFY_WEBHOOK_DEBUG);

const redactValue = (value, { prefix = 12, suffix = 6 } = {}) => {
  const text = String(value || "").trim();
  if (!text) return null;
  if (text.length <= prefix + suffix) return text;
  return `${text.slice(0, prefix)}...${text.slice(-suffix)}`;
};

const fingerprintSecret = (secret) => {
  const text = String(secret || "");
  if (!text) return null;
  return crypto.createHash("sha256").update(text, "utf8").digest("hex").slice(0, 16);
};

export const createWebhookVerificationDebugInfo = ({
  rawBody,
  headers,
  providedHmac,
  secret = process.env.SHOPIFY_WEBHOOK_SECRET,
}) => {
  const normalizedHeaders = normalizeWebhookHeaders(headers || {});
  const bodyText = String(rawBody || "");
  const computedHmac = secret
    ? crypto.createHmac("sha256", secret).update(bodyText, "utf8").digest("base64")
    : null;

  return {
    verification_debug: {
      enabled: true,
      has_secret: Boolean(secret),
      secret_fingerprint: fingerprintSecret(secret),
      raw_body_bytes: Buffer.byteLength(bodyText, "utf8"),
      raw_body_sha256: hashWebhookPayload(bodyText),
      provided_hmac: redactValue(providedHmac),
      computed_hmac: redactValue(computedHmac),
      provided_hmac_length: String(providedHmac || "").length || 0,
      computed_hmac_length: String(computedHmac || "").length || 0,
      topic_header: normalizedHeaders[SHOPIFY_WEBHOOK_HEADERS.topic] || null,
      shop_domain_header:
        normalizedHeaders[SHOPIFY_WEBHOOK_HEADERS.shopDomain] || null,
      webhook_id_header:
        normalizedHeaders[SHOPIFY_WEBHOOK_HEADERS.webhookId] || null,
      content_type: normalizedHeaders["content-type"] || null,
      user_agent: normalizedHeaders["user-agent"] || null,
      header_keys: Object.keys(normalizedHeaders).sort(),
    },
  };
};

export const createWebhookPayloadDebugInfo = ({ envelope = {} } = {}) => ({
  request_debug: {
    enabled: true,
    topic: envelope?.topic || null,
    shop_domain: envelope?.shop_domain || null,
    webhook_id: envelope?.webhook_id || null,
    received_at: envelope?.received_at || null,
    headers: normalizeWebhookHeaders(envelope?.headers || {}),
    raw_body: String(envelope?.rawBody || ""),
    parsed_payload: envelope?.payload || {},
    parse_error: envelope?.parseError
      ? mapWebhookErrorMessage(envelope.parseError, 10_000)
      : null,
  },
});

export const hashWebhookPayload = (rawBody) =>
  crypto.createHash("sha256").update(rawBody || "", "utf8").digest("hex");

const truncate = (value, maxLen = 2000) => {
  const text = String(value || "").trim();
  if (!text) return null;
  return text.length > maxLen ? `${text.slice(0, maxLen - 3)}...` : text;
};

export const mapWebhookErrorMessage = (error, maxLen = 2000) => {
  if (!error) return null;
  return truncate(error?.message || String(error), maxLen);
};

export const createWebhookLogContext = (seed = {}) => ({
  ...webhookLogDefaults,
  ...(seed || {}),
});

export const createWebhookLogger = (seed = {}) => {
  let context = createWebhookLogContext(seed);

  const emit = (level, message, extra = {}) => {
    const payload = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...createWebhookLogContext({ ...context, ...(extra || {}) }),
    };

    if (level === "error") {
      console.error("[shopify-webhook]", payload);
      return;
    }
    if (level === "warn") {
      console.warn("[shopify-webhook]", payload);
      return;
    }
    console.log("[shopify-webhook]", payload);
  };

  return {
    setContext(patch = {}) {
      context = createWebhookLogContext({ ...context, ...(patch || {}) });
      return context;
    },
    getContext() {
      return createWebhookLogContext(context);
    },
    info(message, extra = {}) {
      emit("info", message, extra);
    },
    warn(message, extra = {}) {
      emit("warn", message, extra);
    },
    error(message, extra = {}) {
      emit("error", message, extra);
    },
  };
};

const shouldAttemptIdempotency = () => {
  const disabled =
    String(process.env.SHOPIFY_WEBHOOK_IDEMPOTENCY_DISABLED || "")
      .trim()
      .toLowerCase() === "true";
  return !disabled && Boolean(getWebhookEventsTable());
};

export const beginWebhookIdempotency = async ({
  webhookId,
  topic,
  shopDomain,
  payloadHash,
  logger,
}) => {
  if (!webhookId || !shouldAttemptIdempotency()) {
    return {
      enabled: false,
      skip: false,
      recordId: null,
      status: null,
    };
  }

  try {
    const webhookEventsTable = getWebhookEventsTable();
    const existing = await findOneBy(webhookEventsTable, "Webhook ID", webhookId);
    if (existing?.id) {
      const existingStatus = normalizeIdempotencyStatus(existing?.fields?.Status);
      if (existingStatus && terminalIdempotentStatuses.has(existingStatus)) {
        return {
          enabled: true,
          skip: true,
          blocked: false,
          recordId: existing.id,
          status: existingStatus,
        };
      }

      return {
        enabled: true,
        skip: false,
        blocked: false,
        recordId: existing.id,
        status: existingStatus || IDEMPOTENCY_STATUS.received,
      };
    }

    const created = await createResilient(
      webhookEventsTable,
      {},
      {
        "Webhook ID": webhookId,
        Topic: topic || undefined,
        "Shop Domain": shopDomain || undefined,
        "Payload Hash": payloadHash || undefined,
        Status: "received",
      }
    );

    return {
      enabled: true,
      skip: false,
      blocked: false,
      recordId: created?.id || null,
      status: IDEMPOTENCY_STATUS.received,
    };
  } catch (error) {
    logger?.error("idempotency store unavailable; refusing to process webhook", {
      status: error?.status || null,
      error: mapWebhookErrorMessage(error),
    });
    return {
      enabled: false,
      skip: false,
      blocked: true,
      recordId: null,
      status: null,
      error,
    };
  }
};

export const completeWebhookIdempotency = async ({
  recordId,
  status,
  error,
  logger,
}) => {
  if (!recordId) return;

  try {
    const webhookEventsTable = getWebhookEventsTable();
    await updateResilient(
      webhookEventsTable,
      recordId,
      {},
      {
        Status: status || undefined,
        "Processed At": new Date().toISOString(),
        Error: mapWebhookErrorMessage(error),
      }
    );
  } catch (updateError) {
    logger?.warn("idempotency status update failed", {
      status: updateError?.status || null,
      error: mapWebhookErrorMessage(updateError),
    });
  }
};

export const sendWebhookJson = (res, status, payload, req = null) => {
  if (res && typeof res.status === "function" && typeof res.json === "function") {
    return res.status(status).json(payload);
  }

  if (isWebRequestLike(req)) {
    return new Response(JSON.stringify(payload), {
      status,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
      },
    });
  }

  return {
    statusCode: status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
    body: JSON.stringify(payload),
  };
};

export const sendWebhookText = (res, status, text, req = null) => {
  const body = String(text || "");
  if (res && typeof res.status === "function" && typeof res.end === "function") {
    return res.status(status).end(body);
  }

  if (isWebRequestLike(req)) {
    return new Response(body, {
      status,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store",
      },
    });
  }

  return {
    statusCode: status,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
    body,
  };
};
