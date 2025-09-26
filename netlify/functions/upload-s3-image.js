import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import sharp from "sharp";

const DEFAULT_BUCKET = "barrel-n-bond";
const DEFAULT_REGION = "eu-west-2";
const DEFAULT_PREFIX = "orders";

const labelDimensions = {
  // Fill in e.g. "classic-750": { front: { width: 1200, height: 1600 }, back: {...} }
};

const respond = (status, body) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    }
  });

const s3Client = new S3Client({
  region:
    process.env.S3_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION ||
    DEFAULT_REGION
});

const parseBody = async (arg, isV2) => {
  if (!arg) return {};

  if (isV2) {
    const ct = (arg.headers.get("content-type") || "").toLowerCase();
    try {
      if (ct.includes("application/json")) return await arg.json();
      if (ct.includes("application/x-www-form-urlencoded")) {
        const form = await arg.formData();
        return Object.fromEntries([...form.entries()]);
      }
      const text = await arg.text();
      if (!text) return {};
      try {
        return JSON.parse(text);
      } catch {
        return {};
      }
    } catch (err) {
      console.warn("upload-s3-image: failed to parse v2 body", err);
      return {};
    }
  }

  const rawHeaders = arg.headers || {};
  const ct = (rawHeaders["content-type"] || rawHeaders["Content-Type"] || "").toLowerCase();
  let body = arg.body || "";
  if (!body) return {};

  if (arg.isBase64Encoded) {
    try {
      body = Buffer.from(body, "base64").toString("utf8");
    } catch (err) {
      console.warn("upload-s3-image: failed to decode base64 body", err);
    }
  }

  try {
    if (ct.includes("application/json")) return JSON.parse(body);
    if (ct.includes("application/x-www-form-urlencoded")) {
      return Object.fromEntries(new URLSearchParams(body));
    }
    return JSON.parse(body);
  } catch (err) {
    console.warn("upload-s3-image: unsupported body format", err);
    return {};
  }
};

const sanitizeIdentifier = (value) => {
  if (!value) return "";
  return String(value).replace(/\?.*$/, "").trim();
};

const normalizePrefix = (value) => {
  if (value === undefined || value === null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const stripped = trimmed.replace(/^\/+|\/+$/g, "");
  const parts = stripped.split("/").map((part) => part.trim()).filter(Boolean);
  return parts.length ? parts.join("/") : null;
};

const normalizeStage = (value) => {
  if (!value) return null;
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === "session" || normalized === "sessions") return "session";
  if (normalized === "order" || normalized === "orders") return "order";
  return null;
};

const normalizeBottle = (value) => {
  if (!value) return "";
  return String(value).trim().toLowerCase();
};

const toBoolean = (value) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return ["1", "true", "yes", "on"].includes(normalized);
  }
  return false;
};

const handler = async (event, { qs = {}, isV2, method }) => {
  if (method !== "POST") {
    return respond(405, {
      ok: false,
      error: "method_not_allowed"
    });
  }

  try {
    const body = await parseBody(event, isV2);

    const downloadLink =
      body.download_link ||
      body.downloadLink ||
      body.download_url ||
      body.downloadUrl;

    const orderIdRaw =
      body.order_id ||
      body.orderId ||
      body.order_id_raw ||
      body.orderID;

    const sessionIdRaw =
      body.session_id ||
      body.sessionId ||
      qs.session_id ||
      qs.sessionId;

    const designSideRaw =
      body.design_side ||
      body.designSide ||
      body.design ||
      body.side;

    const stage = normalizeStage(qs.stage || body.stage);
    const bottleKey = normalizeBottle(qs.bottle || body.bottle);
    const shouldResize = toBoolean(qs.resize ?? body.resize);

    console.log("upload-s3-image: request validated", {
      method,
      stage,
      shouldResize,
      bottle: bottleKey
    });

    if (!downloadLink || typeof downloadLink !== "string") {
      return respond(400, {
        ok: false,
        error: "missing_download_link",
        message: "Expected download_link in the request payload"
      });
    }

    if (stage !== "session" && (!orderIdRaw || typeof orderIdRaw !== "string")) {
      return respond(400, {
        ok: false,
        error: "missing_order_id",
        message: "Expected order_id in the request payload"
      });
    }

    if (!designSideRaw || typeof designSideRaw !== "string") {
      return respond(400, {
        ok: false,
        error: "missing_design_side",
        message: "Expected design_side in the request payload"
      });
    }

    const orderId = sanitizeIdentifier(orderIdRaw);
    if (stage !== "session" && !orderId) {
      return respond(400, {
        ok: false,
        error: "invalid_order_id",
        message: "order_id must contain at least one valid character"
      });
    }

    const designSide = (designSideRaw || "").trim();
    if (!designSide) {
      return respond(400, {
        ok: false,
        error: "invalid_design_side",
        message: "design_side must contain at least one character"
      });
    }

    const bucket =
      process.env.S3_BUCKET ||
      process.env.VISTACREATE_S3_BUCKET ||
      process.env.ASSETS_BUCKET ||
      DEFAULT_BUCKET;

    let key = "";
    const meta = { bucket, stage, orderId, designSide };

    if (stage === "session") {
      const sessionId = sanitizeIdentifier(sessionIdRaw);
      if (!sessionId) {
        return respond(400, {
          ok: false,
          error: "missing_session_id",
          message: "stage=session requires session_id in the payload or query string"
        });
      }
      key = `sessions/${sessionId}/${designSide}_label`;
      meta.sessionId = sessionId;
    } else if (stage === "order") {
      key = `orders/${orderId}/${designSide}_label`;
    } else {
      const prefixOverride =
        normalizePrefix(qs.file_path || qs.filePath || qs.prefix || qs.folder);

      const envPrefixRaw =
        process.env.S3_PREFIX ||
        process.env.VISTACREATE_S3_PREFIX ||
        DEFAULT_PREFIX;
      const envPrefix = normalizePrefix(envPrefixRaw) || DEFAULT_PREFIX;
      const basePrefix = prefixOverride || envPrefix;

      const segments = [orderId, `${designSide}_label`];
      if (basePrefix) segments.unshift(basePrefix);
      key = segments.join("/");

      meta.prefixOverride = prefixOverride;
      meta.envPrefix = envPrefix;
      meta.basePrefix = basePrefix;
    }

    meta.key = key;

    const assetResponse = await fetch(downloadLink);
    if (!assetResponse.ok) {
      return respond(502, {
        ok: false,
        error: "download_failed",
        status: assetResponse.status,
        message: `Failed to download asset from source URL (${assetResponse.status})`
      });
    }

    const arrayBuffer = await assetResponse.arrayBuffer();
    let bodyBuffer = Buffer.from(arrayBuffer);
    const originalContentType = (assetResponse.headers.get("content-type") || "").toLowerCase();
    let contentType = originalContentType || "application/octet-stream";

    console.log("upload-s3-image: fetched asset", {
      downloadLink,
      bytes: bodyBuffer.length,
      contentType: originalContentType
    });

    if (shouldResize) {
      if (!originalContentType.startsWith("image/")) {
        return respond(415, {
          ok: false,
          error: "unsupported_media_type",
          message: "resize=true is only supported for image payloads"
        });
      }

      if (!bottleKey) {
        return respond(400, {
          ok: false,
          error: "missing_bottle",
          message: "resize=true requires a bottle query parameter"
        });
      }

      const sideKey = designSide.toLowerCase();
      const dims = labelDimensions[bottleKey]?.[sideKey];
      if (!dims) {
        return respond(400, {
          ok: false,
          error: "missing_dimensions",
          message: `No dimensions configured for bottle='${bottleKey}' and design_side='${sideKey}'`
        });
      }

      const resized = sharp(bodyBuffer)
        .rotate()
        .resize(dims.width, dims.height, { fit: "cover" });

      if (originalContentType.includes("png")) {
        bodyBuffer = await resized.png().toBuffer();
        contentType = "image/png";
      } else if (originalContentType.includes("webp")) {
        bodyBuffer = await resized.webp().toBuffer();
        contentType = "image/webp";
      } else if (originalContentType.includes("gif")) {
        bodyBuffer = await resized.gif().toBuffer();
        contentType = "image/gif";
      } else {
        bodyBuffer = await resized.jpeg({ quality: 90 }).toBuffer();
        contentType = "image/jpeg";
      }

      meta.resize = { bottle: bottleKey, designSide: sideKey, ...dims, contentType };
    }

    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: bodyBuffer,
        ContentType: contentType,
        ACL: process.env.S3_ACL || process.env.VISTACREATE_S3_ACL || "public-read"
      })
    );

    const regionParam =
      process.env.S3_REGION ||
      process.env.AWS_REGION ||
      process.env.AWS_DEFAULT_REGION ||
      DEFAULT_REGION;
    const publicUrl = `https://${bucket}.s3.${regionParam}.amazonaws.com/${encodeURI(key)}`;
    console.log("upload-s3-image: uploaded to S3", { ...meta, region: regionParam });

    return respond(200, {
      ok: true,
      bucket,
      key,
      imageUrl: publicUrl,
      resized: Boolean(meta.resize),
      contentType
    });
  } catch (error) {
    console.error("upload-s3-image failed", error);
    return respond(502, {
      ok: false,
      error: "upload_failed",
      message: error.message || "Failed to upload asset to S3"
    });
  }
};

export default withShopifyProxy(handler, {
  methods: ["POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true
});