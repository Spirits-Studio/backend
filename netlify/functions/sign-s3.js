// apps/ss/sign-s3.js
import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const DEFAULT_BUCKET = "spirits-studio";
const DEFAULT_REGION = "eu-west-2";
const EXPIRES_SECONDS = 300; // 5 minutes, adjust if needed

const regionParam =
  process.env.S3_REGION ||
  process.env.AWS_REGION ||
  process.env.AWS_DEFAULT_REGION ||
  DEFAULT_REGION;

const resolveS3Credentials = () => {
  const accessKeyId = process.env.SS_AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.SS_AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) return undefined;
  const sessionToken = process.env.SS_AWS_SESSION_TOKEN;
  return sessionToken ? { accessKeyId, secretAccessKey, sessionToken } : { accessKeyId, secretAccessKey };
};

const s3 = new S3Client({
  region: regionParam,
  credentials: resolveS3Credentials()
});

const respond = (status, body) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    }
  });

// mirrors your ACL toggle
const shouldAllowAcl = () => {
  const allow = String(process.env.ALLOW_S3_ACL || "").toLowerCase() === "true";
  const ownership = (process.env.S3_OBJECT_OWNERSHIP || "BucketOwnerEnforced").trim();
  return allow && ownership !== "BucketOwnerEnforced";
};

export default withShopifyProxy(async (event, { isV2, method }) => {
  if (method !== "POST") return respond(405, { ok:false, error:"method_not_allowed" });

  const ct = (event.headers.get?.("content-type") || "").toLowerCase();
  let body = {};
  try {
    body = ct.includes("json") ? await event.json() : JSON.parse(await event.text());
  } catch {}

  const {
    // required client-provided inputs
    key,               // e.g. 'sessions/<id>/front_label.png'
    contentType,       // e.g. 'image/png'
    // optional metadata fields we’ll embed as x-amz-meta-*:
    stage,             // 'session' | 'order'
    bottle,            // lowercased bottle key
    design_side,       // 'front' | 'back'
    sessionId,
    orderId,
    expectedWidthMm,
    expectedHeightMm,
    bleedPerSideMm
  } = body;

  if (!key || !contentType) {
    return respond(400, { ok:false, error:"bad_request", message:"key and contentType are required" });
  }

  const bucket = process.env.S3_BUCKET || DEFAULT_BUCKET;

  // Build metadata like your upload function does
  const meta = {
    "ss-stage": stage || (sessionId ? "session" : orderId ? "order" : ""),
    "ss-design-side": (design_side || "").toLowerCase(),
    "ss-bottle": (bottle || "").toLowerCase(),
    "ss-source": "browser-presigned",
  };
  if (sessionId) meta["ss-session-id"] = String(sessionId);
  if (orderId)   meta["ss-order-id"]   = String(orderId);
  if (expectedWidthMm)  meta["ss-expected-width-mm"]  = String(expectedWidthMm);
  if (expectedHeightMm) meta["ss-expected-height-mm"] = String(expectedHeightMm);
  if (bleedPerSideMm)   meta["ss-bleed-per-side-mm"]  = String(bleedPerSideMm);

  // Prepare command with headers we want the browser to also send
  const cmd = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    ContentType: contentType,
    // ACL can be added only if your bucket allows it
    ...(shouldAllowAcl() ? { ACL: (process.env.S3_ACL || "private") } : {}),
    Metadata: meta
  });

  const url = await getSignedUrl(s3, cmd, { expiresIn: EXPIRES_SECONDS });

  return respond(200, {
    ok: true,
    bucket,
    region: regionParam,
    key,
    url,
    // these must be mirrored in the browser PUT request
    requiredHeaders: {
      "Content-Type": contentType
      // ACL cannot be set by browsers in a signed URL PUT unless included in the signature.
      // If you included ACL above, the signature already accounts for it even if you can’t set it explicitly here.
    },
  });
}, {
  methods: ["POST"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true
});