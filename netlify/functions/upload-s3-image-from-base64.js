import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { PDFDocument } from "pdf-lib";
import sharp from "sharp";

const DEFAULT_BUCKET = "spirits-studio";
const DEFAULT_REGION = "eu-west-2";
const TARGET_DPI = Number(process.env.LABEL_EXPORT_DPI ?? 300);

const labelDimensions = {
  polo: {
    front: { width: 110, height: 65 },
    back: { width: 110, height: 65 }
  },
  outlaw: {
    front: { width: 55, height: 95 },
    back: { width: 55, height: 95 }
  },
  antica: {
    front: { width: 110, height: 110 },
    back: { width: 80, height: 100 }
  },
  manila: {
    front: { width: 135, height: 50 },
    back: { width: 115, height: 40 }
  },
  origin: {
    front: { width: 115, height: 45 },
    back: { width: 100, height: 45 }
  }
};

const MM_PER_INCH = 25.4;
const PDF_POINTS_PER_INCH = 72;
const BLEED_PER_SIDE_MM = 2;
const BLEED_TOTAL_MM = BLEED_PER_SIDE_MM * 2;
const DIMENSION_TOLERANCE_MM = Number(process.env.LABEL_DIMENSION_TOLERANCE_MM ?? 5);
const PX_TOLERANCE = Math.round((DIMENSION_TOLERANCE_MM / MM_PER_INCH) * TARGET_DPI);

const respond = (status, body) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    }
  });

const resolveS3Credentials = () => {
  const accessKeyId = process.env.SS_AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.SS_AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) return undefined;
  const sessionToken = process.env.SS_AWS_SESSION_TOKEN;
  return sessionToken ? { accessKeyId, secretAccessKey, sessionToken } : { accessKeyId, secretAccessKey };
};

const regionParam =
  process.env.S3_REGION ||
  process.env.AWS_REGION ||
  process.env.AWS_DEFAULT_REGION ||
  DEFAULT_REGION;

const s3Client = new S3Client({
  region: regionParam,
  credentials: resolveS3Credentials()
});

const shouldAllowAcl = () => {
  const allow = String(process.env.ALLOW_S3_ACL || "").toLowerCase() === "true";
  const ownership = (process.env.S3_OBJECT_OWNERSHIP || "BucketOwnerEnforced").trim();
  return allow && ownership !== "BucketOwnerEnforced";
};

const getAclIfAllowed = () => {
  if (!shouldAllowAcl()) return null;
  const acl =
    process.env.S3_ACL ||
    process.env.VISTACREATE_S3_ACL ||
    process.env.DEFAULT_S3_ACL ||
    "none";
  if (String(acl).toLowerCase() === "none") return null;
  return acl;
};

const isAclNotSupportedError = (error) => {
  if (!error || typeof error !== "object") return false;
  const code = error.Code || error.code || error.name || "";
  const msg = String(error.message || "");
  return code === "AccessControlListNotSupported" || /AccessControlListNotSupported/i.test(msg);
};

const sanitizeIdentifier = (value) => {
  if (!value) return "";
  return String(value).replace(/\?.*$/, "").trim();
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
      return JSON.parse(text);
    } catch {
      return {};
    }
  }

  const headers = arg.headers || {};
  const rawCt = (headers["content-type"] || headers["Content-Type"] || "").toLowerCase();
  let body = arg.body || "";

  if (arg.isBase64Encoded && body) {
    try {
      body = Buffer.from(body, "base64").toString("utf8");
    } catch {
      body = "";
    }
  }

  if (!body) return {};

  try {
    if (rawCt.includes("application/json")) return JSON.parse(body);
    if (rawCt.includes("application/x-www-form-urlencoded")) {
      return Object.fromEntries(new URLSearchParams(body));
    }
    return JSON.parse(body);
  } catch {
    return {};
  }
};

const toArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null) return [];
  return [value];
};

const mmToPixels = (mm) => Math.max(1, Math.round((mm / MM_PER_INCH) * TARGET_DPI));
const mmToPoints = (mm) => (mm / MM_PER_INCH) * PDF_POINTS_PER_INCH;

const getExpectedDimensionsMm = (bottleKey, sideKey) => {
  const dims = labelDimensions[bottleKey]?.[sideKey];
  if (!dims) return null;
  const widthMm = Number(dims.width);
  const heightMm = Number(dims.height);
  if (!Number.isFinite(widthMm) || !Number.isFinite(heightMm) || widthMm <= 0 || heightMm <= 0) return null;

  return {
    widthMm: widthMm + BLEED_TOTAL_MM,
    heightMm: heightMm + BLEED_TOTAL_MM
  };
};

const parseDataUrl = async (input, index) => {
  if (typeof input !== "string" || !input.trim()) {
    throw new Error(`data entry ${index + 1} is empty or not a string`);
  }

  const trimmed = input.trim();
  const dataUrlPattern = /^data:(.*?);base64,(.*)$/i;
  const match = trimmed.match(dataUrlPattern);

  let mimeType = "image/png";
  let base64 = trimmed;

  if (match) {
    mimeType = match[1] && match[1].includes("/") ? match[1].toLowerCase() : "image/png";
    base64 = match[2];
  }

  try {
    const buffer = Buffer.from(base64, "base64");
    if (!buffer.length) throw new Error("decoded buffer is empty");
    return { buffer, mimeType };
  } catch (err) {
    throw new Error(`data entry ${index + 1} is not valid base64 (${err?.message || err})`);
  }
};

const mimeToExtension = (mime) => {
  if (!mime) return { extension: "bin", contentType: "application/octet-stream" };
  if (mime.includes("png")) return { extension: "png", contentType: "image/png" };
  if (mime.includes("jpeg") || mime.includes("jpg")) return { extension: "jpg", contentType: "image/jpeg" };
  if (mime.includes("webp")) return { extension: "webp", contentType: "image/webp" };
  if (mime.includes("gif")) return { extension: "gif", contentType: "image/gif" };
  if (mime.includes("svg")) return { extension: "svg", contentType: "image/svg+xml" };
  return { extension: "bin", contentType: mime };
};

const embedImageInPdf = async (pdfDoc, buffer, mimeType) => {
  if (mimeType.includes("png")) return pdfDoc.embedPng(buffer);
  if (mimeType.includes("jpeg") || mimeType.includes("jpg")) return pdfDoc.embedJpg(buffer);
  if (mimeType.includes("webp") || mimeType.includes("gif") || mimeType.includes("svg")) {
    const pngBuffer = await sharp(buffer).png().toBuffer();
    return pdfDoc.embedPng(pngBuffer);
  }
  try {
    const fallback = await sharp(buffer).png().toBuffer();
    return pdfDoc.embedPng(fallback);
  } catch (err) {
    throw new Error(`Unsupported image format for PDF embedding (${err?.message || err})`);
  }
};

const createPdfFromImage = async ({ imageBuffer, mimeType, widthMm, heightMm }) => {
  const pdfDoc = await PDFDocument.create();
  const pageWidthPt = mmToPoints(widthMm);
  const pageHeightPt = mmToPoints(heightMm);
  const page = pdfDoc.addPage([pageWidthPt, pageHeightPt]);

  const embeddedImage = await embedImageInPdf(pdfDoc, imageBuffer, mimeType);
  const { width: drawWidth, height: drawHeight } = embeddedImage.scaleToFit(pageWidthPt, pageHeightPt);

  page.drawImage(embeddedImage, {
    x: (pageWidthPt - drawWidth) / 2,
    y: (pageHeightPt - drawHeight) / 2,
    width: drawWidth,
    height: drawHeight
  });

  const pdfBytes = await pdfDoc.save();
  return Buffer.from(pdfBytes);
};

const uploadToS3 = async ({ bucket, key, body, contentType, metadata }) => {
  const params = {
    Bucket: bucket,
    Key: key,
    Body: body,
    ContentType: contentType,
    Metadata: metadata
  };

  const acl = getAclIfAllowed();
  if (acl) params.ACL = acl;

  try {
    await s3Client.send(new PutObjectCommand(params));
  } catch (error) {
    if (params.ACL && isAclNotSupportedError(error)) {
      delete params.ACL;
      await s3Client.send(new PutObjectCommand(params));
    } else {
      throw error;
    }
  }
};

const roundMm = (value) => Math.round(value * 100) / 100;

const handler = async (event, { qs = {}, isV2, method }) => {
  if (method !== "POST") {
    return respond(405, { ok: false, error: "method_not_allowed" });
  }

  try {
    const body = await parseBody(event, isV2);

    const dataEntries = toArray(body.data ?? body.images ?? body.base64 ?? qs.data).filter(Boolean);
    if (!dataEntries.length) {
      return respond(400, { ok: false, error: "missing_data", message: "Expected a data field containing base64 image data." });
    }

    const bottleKey = normalizeBottle(body.bottle || body.bottleName || qs.bottle || qs.bottleName);
    if (!bottleKey) {
      return respond(400, { ok: false, error: "missing_bottle", message: "bottle is required." });
    }

    const designSideRaw =
      body.design_side ||
      body.designSide ||
      body.design ||
      body.side ||
      qs.design_side ||
      qs.designSide;

    if (!designSideRaw || typeof designSideRaw !== "string") {
      return respond(400, { ok: false, error: "missing_design_side", message: "design_side is required." });
    }

    const designSide = designSideRaw.trim();
    if (!designSide) {
      return respond(400, { ok: false, error: "invalid_design_side", message: "design_side must contain at least one character." });
    }

    const designSideKey = designSide.toLowerCase();
    const expectedDims = getExpectedDimensionsMm(bottleKey, designSideKey);
    if (!expectedDims) {
      return respond(400, { ok: false, error: "missing_dimensions", message: `No label dimensions configured for bottle='${bottleKey}' and design_side='${designSideKey}'.` });
    }

    const orderIdRaw =
      body.order_id ||
      body.orderId ||
      qs.order_id ||
      qs.orderId;

    const sessionIdRaw =
      body.session_id ||
      body.sessionId ||
      qs.session_id ||
      qs.sessionId;

    let stage = normalizeStage(body.stage || qs.stage);
    if (!stage) stage = sessionIdRaw ? "session" : null;
    if (!stage) stage = orderIdRaw ? "order" : null;

    if (!stage) {
      return respond(400, { ok: false, error: "missing_stage", message: "stage must be 'session' or 'order'." });
    }

    const orderId = sanitizeIdentifier(orderIdRaw);
    const sessionId = sanitizeIdentifier(sessionIdRaw);

    if (stage === "session" && !sessionId) {
      return respond(400, { ok: false, error: "missing_session_id", message: "stage=session requires session_id." });
    }
    if (stage === "order" && !orderId) {
      return respond(400, { ok: false, error: "missing_order_id", message: "stage=order requires order_id." });
    }

    const customName = sanitizeIdentifier(body.filename || body.fileName || body.name || "");
    const bucket = process.env.S3_BUCKET || DEFAULT_BUCKET;
    const designKey = designSideKey.replace(/\s+/g, "_");
    const keyBase = stage === "session" ? `sessions/${sessionId}` : `orders/${orderId}`;

    const results = [];

    for (let idx = 0; idx < dataEntries.length; idx++) {
      const parsed = await parseDataUrl(dataEntries[idx], idx);
      const { buffer, mimeType } = parsed;
      const { extension, contentType } = mimeToExtension(mimeType);

      const sharpInstance = sharp(buffer);
      let metadata;
      try {
        metadata = await sharpInstance.metadata();
      } catch (err) {
        return respond(415, { ok: false, error: "invalid_image", message: `Unable to read image metadata (${err?.message || err}).` });
      }

      if (!metadata?.width || !metadata?.height) {
        return respond(415, { ok: false, error: "invalid_image", message: "Uploaded image is missing width/height metadata." });
      }

      const widthPx = metadata.width;
      const heightPx = metadata.height;

      const expectedWidthPx = mmToPixels(expectedDims.widthMm);
      const expectedHeightPx = mmToPixels(expectedDims.heightMm);
      const diffNormal = Math.max(Math.abs(widthPx - expectedWidthPx), Math.abs(heightPx - expectedHeightPx));
      const diffRotated = Math.max(Math.abs(widthPx - expectedHeightPx), Math.abs(heightPx - expectedWidthPx));
      const orientation = diffNormal <= PX_TOLERANCE ? "normal" : diffRotated <= PX_TOLERANCE ? "rotated" : "unknown";

      const pdfWidthMm = orientation === "rotated" ? expectedDims.heightMm : expectedDims.widthMm;
      const pdfHeightMm = orientation === "rotated" ? expectedDims.widthMm : expectedDims.heightMm;

      const variantSuffix = dataEntries.length > 1 ? `_${String(idx + 1).padStart(2, "0")}` : "";
      const fallbackName = `${designKey}_label${variantSuffix}`;
      const baseFileName = (customName || fallbackName).replace(/\s+/g, "_").replace(/_{2,}/g, "_");
      const originalKey = `${keyBase}/${baseFileName}.${extension}`;
      const pdfKey = `${keyBase}/${baseFileName}.pdf`;

      const metadataBase = {
        "ss-stage": stage,
        "ss-design-side": designKey,
        "ss-bottle": bottleKey,
        "ss-expected-width-mm": String(roundMm(expectedDims.widthMm)),
        "ss-expected-height-mm": String(roundMm(expectedDims.heightMm)),
        "ss-actual-width-px": String(widthPx),
        "ss-actual-height-px": String(heightPx),
        "ss-bleed-per-side-mm": String(BLEED_PER_SIDE_MM),
        "ss-orientation": orientation,
        "ss-source": "base64"
      };

      if (stage === "session") {
        metadataBase["ss-session-id"] = sessionId;
      } else {
        metadataBase["ss-order-id"] = orderId;
      }

      await uploadToS3({
        bucket,
        key: originalKey,
        body: buffer,
        contentType,
        metadata: metadataBase
      });

      let pdfBuffer;
      try {
        pdfBuffer = await createPdfFromImage({
          imageBuffer: buffer,
          mimeType: contentType,
          widthMm: pdfWidthMm,
          heightMm: pdfHeightMm
        });
      } catch (err) {
        return respond(500, { ok: false, error: "pdf_generation_failed", message: err?.message || "Failed to convert image to PDF." });
      }

      await uploadToS3({
        bucket,
        key: pdfKey,
        body: pdfBuffer,
        contentType: "application/pdf",
        metadata: {
          ...metadataBase,
          "ss-original-key": originalKey
        }
      });

      const originalUrl = `https://${bucket}.s3.${regionParam}.amazonaws.com/${encodeURI(originalKey)}`;
      const pdfUrl = `https://${bucket}.s3.${regionParam}.amazonaws.com/${encodeURI(pdfKey)}`;

      results.push({
        index: idx,
        original: {
          key: originalKey,
          url: originalUrl,
          contentType,
          sizeBytes: buffer.length
        },
        pdf: {
          key: pdfKey,
          url: pdfUrl,
          contentType: "application/pdf",
          sizeBytes: pdfBuffer.length
        },
        orientation,
        widthPx,
        heightPx
      });
    }

    return respond(200, {
      ok: true,
      bucket,
      stage,
      bottle: bottleKey,
      designSide: designSideKey,
      expectedDimensionsMm: {
        width: roundMm(expectedDims.widthMm),
        height: roundMm(expectedDims.heightMm)
      },
      bleedPerSideMm: BLEED_PER_SIDE_MM,
      entries: results
    });
  } catch (error) {
    return respond(502, {
      ok: false,
      error: "upload_failed",
      message: error?.message || "Failed to upload asset to S3"
    });
  }
};

export default withShopifyProxy(handler, {
  methods: ["POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true
});
