// apps/ss/finalize-upload.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { PDFDocument } from "pdf-lib";
import sharp from "sharp";

const DEFAULT_BUCKET = "spirits-studio";
const DEFAULT_REGION = "eu-west-2";

// --- copy helpers from your upload function ---
const MM_PER_INCH = 25.4;
const PDF_POINTS_PER_INCH = 72;
const mmToPoints = (mm) => (mm / MM_PER_INCH) * PDF_POINTS_PER_INCH;

const respond = (status, body) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });

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

const s3 = new S3Client({
  region: regionParam,
  credentials: resolveS3Credentials()
});

const embedImageInPdf = async (pdfDoc, buffer, mimeType) => {
  if ((mimeType || "").includes("png")) return pdfDoc.embedPng(buffer);
  if ((mimeType || "").includes("jpeg") || (mimeType || "").includes("jpg")) return pdfDoc.embedJpg(buffer);
  // convert unknowns to PNG via sharp
  const pngBuffer = await sharp(buffer).png().toBuffer();
  return pdfDoc.embedPng(pngBuffer);
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

const shouldAllowAcl = () => {
  const allow = String(process.env.ALLOW_S3_ACL || "").toLowerCase() === "true";
  const ownership = (process.env.S3_OBJECT_OWNERSHIP || "BucketOwnerEnforced").trim();
  return allow && ownership !== "BucketOwnerEnforced";
};

export default withShopifyProxy(async (event, { method }) => {
  if (method !== "POST") return respond(405, { ok:false, error:"method_not_allowed" });

  // Expect a tiny JSON body: { key, designSide, expectedWidthMm, expectedHeightMm }
  const ct = (event.headers.get?.("content-type") || "").toLowerCase();
  let body = {};
  try {
    body = ct.includes("json") ? await event.json() : JSON.parse(await event.text());
  } catch {}

  const { key, designSide, expectedWidthMm, expectedHeightMm } = body || {};
  if (!key || !expectedWidthMm || !expectedHeightMm) {
    return respond(400, { ok:false, error:"bad_request", message:"key, expectedWidthMm, expectedHeightMm are required" });
  }

  const bucket = process.env.S3_BUCKET || DEFAULT_BUCKET;

  // 1) Head object to grab metadata + content-type
  const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
  const contentType = head.ContentType || "image/png";
  const baseMeta = head.Metadata || {};

  // 2) Get the image bytes
  const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const buf = Buffer.from(await obj.Body.arrayBuffer());

  // 3) Create PDF (respect orientation against expected dims if you like)
  // (Simple: use provided expected dims as-is)
  const pdfBuffer = await createPdfFromImage({
    imageBuffer: buf,
    mimeType: contentType,
    widthMm: Number(expectedWidthMm),
    heightMm: Number(expectedHeightMm)
  });

  // 4) Write the PDF next to the original
  const pdfKey = key.replace(/\.[^.]+$/, "") + ".pdf";
  await s3.send(new PutObjectCommand({
    Bucket: bucket,
    Key: pdfKey,
    Body: pdfBuffer,
    ContentType: "application/pdf",
    Metadata: {
      ...baseMeta,
      "bnb-original-key": key
    },
    ...(shouldAllowAcl() ? { ACL: (process.env.S3_ACL || "private") } : {})
  }));

  const baseUrl = `https://${bucket}.s3.${regionParam}.amazonaws.com/`;
  return respond(200, {
    ok: true,
    original: { key, url: baseUrl + encodeURI(key) },
    pdf: { key: pdfKey, url: baseUrl + encodeURI(pdfKey) }
  });
}, {
  methods: ["POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true
});