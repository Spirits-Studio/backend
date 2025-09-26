import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const DEFAULT_BUCKET = "barrel-n-bond";
const DEFAULT_REGION = "eu-west-2";
const DEFAULT_PREFIX = "orders";

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

const sanitizeOrderId = (value) => {
  if (!value) return "";
  return String(value).replace(/\?.*$/, "").trim();
};

const normalizeStage = (value) => {
  if (value === undefined || value === null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const stripped = trimmed.replace(/^\/+|\/+$/g, "");
  const parts = stripped
    .split('/')
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean);
  return parts.length ? parts.join('/') : null;
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

    const designSideRaw =
      body.design_side ||
      body.designSide ||
      body.design ||
      body.side;

    console.log("upload-s3-image: request validated", { method });

    if (!downloadLink || typeof downloadLink !== "string") {
      return respond(400, {
        ok: false,
        error: "missing_download_link",
        message: "Expected download_link in the request payload"
      });
    }

    if (!orderIdRaw || typeof orderIdRaw !== "string") {
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

    const orderId = sanitizeOrderId(orderIdRaw);
    if (!orderId) {
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
      process.env.ASSETS_BUCKET ||
      DEFAULT_BUCKET;

    const stage = normalizeStage(qs.stage);

    let key = ''

    if(stage === session) {
      key = `${stage}s/${sessionId}/${designSide}_label`;

    } else if(stage === order) {
      key = `${stage}s/${orderId}/${designSide}_label`;
    }
    

    console.log("upload-s3-image: resolved s3 key", {
      bucket,
      prefixOverride,
      envPrefix,
      basePrefix: prefixPath,
      orderId,
      designSide,
      key
    });

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
    const bodyBuffer = Buffer.from(arrayBuffer);
    console.log("upload-s3-image: fetched asset", {
      downloadLink,
      bytes: bodyBuffer.length,
      contentType: assetResponse.headers.get("content-type")
    });

    const contentType = assetResponse.headers.get("content-type") || "image/png";

    await s3Client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: bodyBuffer,
        ContentType: contentType,
        ACL: process.env.S3_ACL || process.env.S3_ACL || "public-read"
      })
    );

    const regionParam =
      process.env.S3_REGION ||
      process.env.S3_REGION ||
      process.env.AWS_REGION ||
      process.env.AWS_DEFAULT_REGION ||
      DEFAULT_REGION;
    const publicUrl = `https://${bucket}.s3.${regionParam}.amazonaws.com/${encodeURI(key)}`;
    console.log("upload-s3-image: uploaded to S3", { bucket, key, region: regionParam });

    return respond(200, {
      ok: true,
      bucket,
      key,
      imageUrl: publicUrl
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
