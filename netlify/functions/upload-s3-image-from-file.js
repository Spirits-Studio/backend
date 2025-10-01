import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { PDFDocument } from "pdf-lib";

const DEFAULT_BUCKET = "barrel-n-bond";
const DEFAULT_REGION = "eu-west-2";

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
const DIMENSION_TOLERANCE_MM = Number(process.env.LABEL_DIMENSION_TOLERANCE_MM ?? 0.3);

const respond = (status, body) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    }
  });

const isAclNotSupportedError = (error) => {
  if (!error || typeof error !== "object") return false;
  const code = error.Code || error.code || error.name || "";
  const msg = String(error.message || "");
  return code === "AccessControlListNotSupported" || /AccessControlListNotSupported/i.test(msg);
};

const resolveS3Credentials = () => {
  const accessKeyId = process.env.BNB_AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.BNB_AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) return undefined;
  const sessionToken = process.env.BNB_AWS_SESSION_TOKEN;
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

const ensureRequest = (arg, isV2) => {
  if (isV2) return arg;

  const headers = new Headers();
  Object.entries(arg.headers || {}).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.filter(Boolean).forEach((v) => headers.append(key, v));
    } else if (value !== undefined && value !== null) {
      headers.append(key, String(value));
    }
  });

  const bodyInit = arg.body
    ? Buffer.from(arg.body, arg.isBase64Encoded ? "base64" : "utf8")
    : undefined;

  const url = arg.rawUrl || `https://placeholder${arg.path || "/"}`;
  return new Request(url, {
    method: arg.httpMethod || "POST",
    headers,
    body: bodyInit
  });
};

const convertToMillimetres = (value, unit = "mm") => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  const normalized = unit.trim().toLowerCase();
  if (["mm", "millimeter", "millimeters", "millimetre", "millimetres"].includes(normalized)) {
    return numeric;
  }
  if (["cm", "centimeter", "centimeters", "centimetre", "centimetres"].includes(normalized)) {
    return numeric * 10;
  }
  if (["in", "inch", "inches"].includes(normalized)) {
    return numeric * MM_PER_INCH;
  }
  return null;
};

const getExpectedDimensionsMm = (bottleKey, sideKey) => {
  const dims = labelDimensions[bottleKey]?.[sideKey];
  if (!dims) return null;

  const unit = typeof dims.unit === "string" && dims.unit.trim() ? dims.unit : "mm";
  const widthMm = convertToMillimetres(dims.width, unit);
  const heightMm = convertToMillimetres(dims.height, unit);
  if (!widthMm || !heightMm) return null;

  return {
    widthMm: widthMm + BLEED_TOTAL_MM,
    heightMm: heightMm + BLEED_TOTAL_MM
  };
};

const pointsToMillimetres = (points) => (points * MM_PER_INCH) / PDF_POINTS_PER_INCH;

const roundMm = (value) => Math.round(value * 100) / 100;

const getStringField = (formData, ...keys) => {
  for (const key of keys) {
    const raw = formData.get(key);
    if (typeof raw === "string") {
      const trimmed = raw.trim();
      if (trimmed) return trimmed;
    }
  }
  return null;
};

const collectFiles = (formData) => {
  const files = [];
  for (const [key, value] of formData.entries()) {
    if (typeof File !== "undefined" && value instanceof File) {
      files.push({ key, file: value });
    }
  }
  return files;
};

const handler = async (arg, { qs = {}, isV2, method }) => {
  if (method !== "POST") {
    return respond(405, { ok: false, error: "method_not_allowed" });
  }

  try {
    const request = ensureRequest(arg, isV2);

    let formData;
    try {
      formData = await request.formData();
    } catch (error) {
      console.warn("upload-s3-image-from-file: failed to parse multipart form", error);
      return respond(415, {
        ok: false,
        error: "invalid_payload",
        message: "Expected multipart/form-data payload with a PDF file"
      });
    }

    const files = collectFiles(formData);
    if (!files.length) {
      return respond(400, {
        ok: false,
        error: "missing_file",
        message: "No file found in the upload payload"
      });
    }

    if (files.length > 1) {
      console.warn("upload-s3-image-from-file: multiple files received, using first", {
        count: files.length,
        keys: files.map((entry) => entry.key)
      });
    }

    const { file } = files[0];
    const originalFilename = file.name || "upload.pdf";
    const providedType = (file.type || "").toLowerCase();
    const isPdf = providedType.includes("pdf") || originalFilename.toLowerCase().endsWith(".pdf");
    if (!isPdf) {
      return respond(415, {
        ok: false,
        error: "unsupported_media_type",
        message: "Only PDF uploads are supported"
      });
    }

    const bottleKey = normalizeBottle(
      qs.bottle ||
        qs.bottleName ||
        getStringField(formData, "bottle", "bottleName")
    );

    const designSideRaw =
      getStringField(formData, "design_side", "designSide", "design", "side") ||
      qs.design_side ||
      qs.designSide;

    const stageRaw = qs.stage || getStringField(formData, "stage");
    let stage = normalizeStage(stageRaw);

    const orderIdRaw =
      getStringField(formData, "order_id", "orderId", "orderID") ||
      qs.order_id ||
      qs.orderId;

    const sessionIdRaw =
      getStringField(formData, "session_id", "sessionId", "sessionID") ||
      qs.session_id ||
      qs.sessionId;

    if (!stage) {
      stage = sessionIdRaw ? "session" : normalizeStage(qs.default_stage || "");
    }

    if (!stage) {
      return respond(400, {
        ok: false,
        error: "missing_stage",
        message: "stage query parameter or form field is required"
      });
    }

    if (!designSideRaw) {
      return respond(400, {
        ok: false,
        error: "missing_design_side",
        message: "design_side form field is required"
      });
    }

    if (!bottleKey) {
      return respond(400, {
        ok: false,
        error: "missing_bottle",
        message: "bottle form field is required for PDF dimension validation"
      });
    }

    const designSide = designSideRaw.trim();
    if (!designSide) {
      return respond(400, {
        ok: false,
        error: "invalid_design_side",
        message: "design_side must contain at least one character"
      });
    }

    const designSideKey = designSide.toLowerCase();
    const expectedDims = getExpectedDimensionsMm(bottleKey, designSideKey);
    if (!expectedDims) {
      return respond(400, {
        ok: false,
        error: "missing_dimensions",
        message: `No label dimensions configured for bottle='${bottleKey}' and design_side='${designSideKey}'`
      });
    }

    const orderId = sanitizeIdentifier(orderIdRaw);
    const sessionId = sanitizeIdentifier(sessionIdRaw);

    if (stage === "session" && !sessionId) {
      return respond(400, {
        ok: false,
        error: "missing_session_id",
        message: "stage=session requires session_id in the request"
      });
    }

    if (stage === "order" && !orderId) {
      return respond(400, {
        ok: false,
        error: "missing_order_id",
        message: "stage=order requires order_id in the request"
      });
    }

    const fileArrayBuffer = await file.arrayBuffer();
    const fileBuffer = Buffer.from(fileArrayBuffer);
    if (!fileBuffer.length) {
      return respond(400, {
        ok: false,
        error: "empty_file",
        message: "Uploaded file is empty"
      });
    }

    let pdfDoc;
    try {
      pdfDoc = await PDFDocument.load(fileBuffer, { ignoreEncryption: true });
    } catch (error) {
      console.warn("upload-s3-image-from-file: failed to parse PDF", error);
      return respond(415, {
        ok: false,
        error: "invalid_pdf",
        message: "Uploaded file is not a readable PDF"
      });
    }

    const pageCount = pdfDoc.getPageCount();
    if (pageCount !== 1) {
      return respond(422, {
        ok: false,
        error: "invalid_page_count",
        message: "PDF must contain exactly one page"
      });
    }

    const [firstPage] = pdfDoc.getPages();
    const { width: widthPts, height: heightPts } = firstPage.getSize();

    const actualWidthMm = pointsToMillimetres(widthPts);
    const actualHeightMm = pointsToMillimetres(heightPts);

    const diffWidth = Math.abs(actualWidthMm - expectedDims.widthMm);
    const diffHeight = Math.abs(actualHeightMm - expectedDims.heightMm);

    const diffWidthRotated = Math.abs(actualWidthMm - expectedDims.heightMm);
    const diffHeightRotated = Math.abs(actualHeightMm - expectedDims.widthMm);

    const matchesNormal = diffWidth <= DIMENSION_TOLERANCE_MM && diffHeight <= DIMENSION_TOLERANCE_MM;
    const matchesRotated = diffWidthRotated <= DIMENSION_TOLERANCE_MM && diffHeightRotated <= DIMENSION_TOLERANCE_MM;

    if (!matchesNormal && !matchesRotated) {
      return respond(422, {
        ok: false,
        error: "invalid_pdf_dimensions",
        message: `Expected ${roundMm(expectedDims.widthMm)}mm x ${roundMm(expectedDims.heightMm)}mm (including 2mm bleed per side) but received ${roundMm(actualWidthMm)}mm x ${roundMm(actualHeightMm)}mm`
      });
    }

    const orientation = matchesRotated ? "rotated" : "normal";
    const contentType = providedType || "application/pdf";

    const bucket = process.env.S3_BUCKET || DEFAULT_BUCKET;
    const designKey = designSideKey.replace(/\s+/g, "_");
    const keyBase = stage === "session" ? `sessions/${sessionId}` : `orders/${orderId}`;
    const key = `${keyBase}/${designKey}_label.pdf`;

    const metadata = {
      "bnb-stage": stage,
      "bnb-design-side": designKey,
      "bnb-bottle": bottleKey,
      "bnb-expected-width-mm": String(roundMm(expectedDims.widthMm)),
      "bnb-expected-height-mm": String(roundMm(expectedDims.heightMm)),
      "bnb-actual-width-mm": String(roundMm(actualWidthMm)),
      "bnb-actual-height-mm": String(roundMm(actualHeightMm)),
      "bnb-bleed-per-side-mm": String(BLEED_PER_SIDE_MM),
      "bnb-page-count": String(pageCount)
    };

    if (originalFilename) {
      metadata["bnb-original-filename"] = originalFilename.slice(-1024);
    }

    if (stage === "session") {
      metadata["bnb-session-id"] = sessionId;
    } else {
      metadata["bnb-order-id"] = orderId;
    }

    const putParams = {
      Bucket: bucket,
      Key: key,
      Body: fileBuffer,
      ContentType: contentType,
      Metadata: metadata
    };

    const maybeAcl = getAclIfAllowed();
    if (maybeAcl) putParams.ACL = maybeAcl;

    try {
      await s3Client.send(new PutObjectCommand(putParams));
    } catch (error) {
      if (putParams.ACL && isAclNotSupportedError(error)) {
        console.warn("upload-s3-image-from-file: bucket does not support ACLs, retrying without ACL", {
          bucket,
          key
        });
        delete putParams.ACL;
        await s3Client.send(new PutObjectCommand(putParams));
      } else {
        throw error;
      }
    }

    const publicUrl = `https://${bucket}.s3.${regionParam}.amazonaws.com/${encodeURI(key)}`;
    console.log("upload-s3-image-from-file: uploaded PDF", {
      bucket,
      key,
      stage,
      orderId,
      sessionId,
      bottle: bottleKey,
      designSide: designKey,
      orientation,
      expected: expectedDims,
      actual: {
        widthMm: roundMm(actualWidthMm),
        heightMm: roundMm(actualHeightMm)
      }
    });

    return respond(200, {
      ok: true,
      bucket,
      key,
      url: publicUrl,
      imageUrl: publicUrl,
      contentType,
      sizeBytes: fileBuffer.length,
      orientation,
      expectedDimensionsMm: {
        width: roundMm(expectedDims.widthMm),
        height: roundMm(expectedDims.heightMm)
      },
      actualDimensionsMm: {
        width: roundMm(actualWidthMm),
        height: roundMm(actualHeightMm)
      },
      bleedPerSideMm: BLEED_PER_SIDE_MM
    });
  } catch (error) {
    console.error("upload-s3-image-from-file failed", error);
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
