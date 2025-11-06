import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { GoogleGenAI } from "@google/genai";
import sharp from "sharp";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const debugImages = [];
let debugOn = false; // toggled per-request inside main()

// --- Helper: read API key from canonical env names (with your legacy fallback) ---
function getGeminiKey() {
  return (
    process.env.GOOGLE_API_KEY ||
    process.env.GEMINI_API_KEY ||
    process.env.GOOGLE_AI_STUDIO_KEY || // legacy fallback
    ""
  );
}

// --- Helper: safe JSON parse ---
function safeParseJSON(input) {
  if (!input) return {};
  if (typeof input === "object") return input;
  try { return JSON.parse(input); } catch { return {}; }
}

// --- Helper: read Request body supporting JSON, form-data, and urlencoded
async function readBody(req) {
  try {
    const ct = req?.headers?.get?.('content-type') || '';

    // JSON
    if (ct.includes('application/json')) {
      const text = await req.text();
      return safeParseJSON(text);
    }

    // Form submissions (Site/Browser -> Netlify function)
    if (ct.includes('multipart/form-data') || ct.includes('application/x-www-form-urlencoded')) {
      const fd = await req.formData();
      const obj = {};
      for (const [key, value] of fd.entries()) {
        if (typeof value === 'string') {
          obj[key] = value;
        } else if (value && typeof value.arrayBuffer === 'function') {
          // File/Blob – convert to data URL if it's the logo
          const mime = value.type || 'application/octet-stream';
          const ab = await value.arrayBuffer();
          const b64 = Buffer.from(ab).toString('base64');
          if (key === 'logo') {
            obj.logoDataUrl = `data:${mime};base64,${b64}`;
            obj.logoName = value.name || 'logo';
          } else {
            // keep as generic attachment if needed later
            obj[key] = { name: value.name || 'file', mime, size: ab.byteLength, base64: b64 };
          }
        }
      }
      return obj;
    }

    // Fallback: try text->JSON
    const text = await req.text();
    return safeParseJSON(text);
  } catch (e) {
    console.error('readBody error:', e?.message || e);
    return {};
  }
}

function pickDataUrl(obj, keys = []) {
  for (const k of keys) {
    const v = obj?.[k];
    if (typeof v === 'string' && v.startsWith('data:')) return v;
  }
  return '';
}

function normalizeHex(hex = '') {
  const s = String(hex).trim();
  if (!s) return '';
  const m = s.match(/^#?[0-9a-fA-F]{6}$/);
  return m ? (s.startsWith('#') ? s : `#${s}`) : '';
}

function dataUrlToInlineData(dataUrl) {
  try {
    const [, meta, b64] = dataUrl.match(/^data:(.*?);base64,(.*)$/) || [];
    if (!b64) return null;
    const mime = meta && meta.includes('/') ? meta : 'image/png';
    return { mimeType: mime, data: b64 };
  } catch { return null; }
}

// Fetch a template image (by URL) and convert it to Gemini inlineData
async function fetchTemplateInlineData(url) {
  if (!url) return null;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const ct = res.headers.get('content-type') || 'image/png';
    const ab = await res.arrayBuffer();
    const b64 = Buffer.from(ab).toString('base64');
    return { mimeType: ct, data: b64 };
  } catch (e) {
    console.error('fetchTemplateInlineData error:', e?.message || e);
    return null;
  }
}

async function trimWhiteBorder(input, opts = {}) {
  // Support both number and object forms: trimWhiteBorder(buf, 15) or trimWhiteBorder(buf, { threshold: 15 })
  const threshold = typeof opts === 'number' ? opts : (opts && typeof opts.threshold === 'number' ? opts.threshold : 12);

  // Flatten transparency onto white so trim works properly
  const flattened = await sharp(input)
    .flatten({ background: { r: 255, g: 255, b: 255 } })
    .toBuffer();

  const originalMeta = await sharp(flattened).metadata();

  // Sharp >=0.33 expects an object; passing `{ threshold }` works across versions
  const trimmed = sharp(flattened).trim({ threshold });
  const outputBuffer = await trimmed.toBuffer();
  const croppedMeta = await sharp(outputBuffer).metadata();

  return {
    buffer: outputBuffer,
    original: { width: originalMeta.width, height: originalMeta.height },
    cropped: { width: croppedMeta.width, height: croppedMeta.height },
    removed: {
      width: originalMeta.width - croppedMeta.width,
      height: originalMeta.height - croppedMeta.height
    }
  };
}

// --- Unit & colour helpers ---
function mmToPx(mm, dpi = 300) {
  return Math.max(1, Math.round((mm / 25.4) * dpi));
}

function hexToRgb(hex) {
  const h = hex.replace('#','');
  return {
    r: parseInt(h.substring(0,2),16),
    g: parseInt(h.substring(2,4),16),
    b: parseInt(h.substring(4,6),16)
  };
}

// Detect dominant colour on the outermost ring of a trimmed image (first content colour)
async function detectFirstContentColour(inputBuffer, nearWhite = 245, ring = 1) {
  const im = sharp(inputBuffer).ensureAlpha();
  const { data, info } = await im.raw().toBuffer({ resolveWithObject: true });
  const W = info.width, H = info.height;
  const isWhiteish = (r,g,b) => r>=nearWhite && g>=nearWhite && b>=nearWhite;
  const clamp = (v,lo,hi)=>Math.max(lo,Math.min(hi,v));
  const sample=[];
  for (let k=0;k<ring;k++){
    const left=clamp(0+k,0,W-1);
    const right=clamp(W-1-k,0,W-1);
    const top=clamp(0+k,0,H-1);
    const bottom=clamp(H-1-k,0,H-1);
    for(let x=left;x<=right;x++){
      for(const y of [top,bottom]){
        const i=(y*W+x)*4; const r=data[i], g=data[i+1], b=data[i+2];
        if(!isWhiteish(r,g,b)) sample.push([r,g,b]);
      }
    }
    for(let y=top+1;y<bottom;y++){
      for(const x of [left,right]){
        const i=(y*W+x)*4; const r=data[i], g=data[i+1], b=data[i+2];
        if(!isWhiteish(r,g,b)) sample.push([r,g,b]);
      }
    }
  }
  if (!sample.length) return '#FFFFFF';
  const med = arr=>{const a=arr.slice().sort((a,b)=>a-b);const m=Math.floor(a.length/2);return a.length%2?a[m]:Math.round((a[m-1]+a[m])/2)};
  const rs=sample.map(p=>p[0]), gs=sample.map(p=>p[1]), bs=sample.map(p=>p[2]);
  const r=med(rs), g=med(gs), b=med(bs);
  return '#' + [r,g,b].map(v=>v.toString(16).padStart(2,'0')).join('').toUpperCase();
}

// Compose final label with exact mm dimensions (converted to px via DPI) and center the cropped image
async function composeLabel(croppedBuffer, widthMm, heightMm, bgHex, dpi = 300) {
  const targetW = mmToPx(widthMm, dpi);
  const targetH = mmToPx(heightMm, dpi);
  const bg = hexToRgb(bgHex);

  const meta = await sharp(croppedBuffer).metadata();
  const cw = meta.width || 1;
  const ch = meta.height || 1;

  // Fit the cropped image entirely inside the target while preserving aspect
  const scale = Math.min(targetW / cw, targetH / ch);
  const rw = Math.max(1, Math.floor(cw * scale));
  const rh = Math.max(1, Math.floor(ch * scale));

  const resized = await sharp(croppedBuffer).resize(rw, rh, { fit: 'contain' }).toBuffer();

  const left = Math.floor((targetW - rw) / 2);
  const top  = Math.floor((targetH - rh) / 2);

  const composite = await sharp({
    create: { width: targetW, height: targetH, channels: 3, background: { r: bg.r, g: bg.g, b: bg.b } }
  })
  .composite([{ input: resized, left, top }])
  .png()
  .toBuffer();

  return composite;
}

// --- S3 upload helpers (mirrors create-front-ai-label) ---
const DEFAULT_BUCKET = "spirits-studio";
const DEFAULT_REGION = "eu-west-2";
const regionParam =
  process.env.S3_REGION ||
  process.env.AWS_REGION ||
  process.env.AWS_DEFAULT_REGION ||
  DEFAULT_REGION;

function resolveS3Credentials() {
  const accessKeyId = process.env.BNB_AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.BNB_AWS_SECRET_ACCESS_KEY;
  if (!accessKeyId || !secretAccessKey) return undefined;
  const sessionToken = process.env.BNB_AWS_SESSION_TOKEN;
  return sessionToken
    ? { accessKeyId, secretAccessKey, sessionToken }
    : { accessKeyId, secretAccessKey };
}

const s3Client = new S3Client({
  region: regionParam,
  credentials: resolveS3Credentials()
});

const sanitizeId = (v) => String(v || "").trim().replace(/[^a-zA-Z0-9_-]/g, "");
const mimeToExt = (mime) => {
  const m = (mime || "").toLowerCase();
  if (m.includes("png")) return "png";
  if (m.includes("jpeg") || m.includes("jpg")) return "jpg";
  if (m.includes("webp")) return "webp";
  if (m.includes("gif")) return "gif";
  return "bin";
};

async function uploadBufferToS3({ buffer, contentType, key, metadata = {} }) {
  const Bucket = process.env.S3_BUCKET || DEFAULT_BUCKET;
  await s3Client.send(new PutObjectCommand({
    Bucket,
    Key: key,
    Body: buffer,
    ContentType: contentType || "application/octet-stream",
    Metadata: metadata
  }));
  return `https://${Bucket}.s3.${regionParam}.amazonaws.com/${encodeURI(key)}`;
}

function parseDataUrlToBuffer(dataUrl) {
  const m = /^data:(.*?);base64,(.*)$/i.exec(String(dataUrl || ""));
  if (!m) return { buffer: null, mimeType: "" };
  const mimeType = m[1] || "image/png";
  const b64 = m[2] || "";
  const buffer = Buffer.from(b64, "base64");
  return { buffer, mimeType };
}


// --- Label dimension map (mm) ---
const labelDimensions = {
  Polo:   { front: { width: 110, height: 65 },  back: { width: 110, height: 65 } },
  Outlaw: { front: { width: 55,  height: 95 },  back: { width: 55,  height: 95 } },
  Antica: { front: { width: 110, height: 110 }, back: { width: 80,  height: 100 } },
  Manila: { front: { width: 135, height: 50 },  back: { width: 115, height: 40 } },
  Origin: { front: { width: 115, height: 45 },  back: { width: 100, height: 45 } },
};

// --- Normalise bottle & side casing to match keys above ---
function normaliseBottle(name = "") {
  if (!name) return "";
  return name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
}
function normaliseSide(side = "") {
  const s = String(side || "").toLowerCase();
  return s === "back" ? "back" : "front";
}

// Find closest aspect ratio from dimensions map (if needed)
function getClosestAspectRatio(width, height) {
  const ratios = [
    "1:1",
    "2:3",
    "3:2",
    "3:4",
    "4:3",
    "4:5",
    "5:4",
    "9:16",
    "16:9",
    "21:9"
  ];

  const target = width / height;

  let closest = ratios[0];
  let smallestDiff = Infinity;

  for (const ratio of ratios) {
    const [w, h] = ratio.split(":").map(Number);
    const r = w / h;
    const diff = Math.abs(r - target);
    if (diff < smallestDiff) {
      smallestDiff = diff;
      closest = ratio;
    }
  }

  return closest;
}

function buildRevisionPrompt(alcoholName, dims, critique, musts, designSide, primaryHex, secondaryHex) {
  const orientation =
    dims.width === dims.height ? 'square' :
    dims.width >= dims.height ? 'landscape' : 'portrait';

  const lines = [];
  if (designSide === 'back') {
    lines.push(`You are REVISING the BACK label background for this bottle (${orientation}).`);
    lines.push(`Start from the PRIOR OUTPUT image and address ONLY these revision notes:`);
    if (critique) {
      for (const ln of String(critique).split(/\r?\n/).filter(Boolean)) {
        lines.push(`- ${ln}`);
      }
    }
    if (Array.isArray(musts) && musts.length) {
      lines.push(`Hard requirements (must keep):`);
      for (const m of musts) lines.push(`- ${m}`);
    }
    lines.push(`ABSOLUTELY NO figurative content: no characters, creatures, people, animals, mascots, faces, hands, tentacles, crabs, fish, silhouettes; and no objects such as bottles, logos, icons, barcodes, or recognizable items.`);
    lines.push(`Use only non-figurative background elements: gradients, soft light rays, gentle bokeh, low-detail textures, abstract patterns. If using plants/seaweed motifs, keep them highly abstract and non-recognizable.`);
    lines.push(`If the FRONT inspiration shows any characters or objects, treat them as forbidden elements — carry over only palette, lighting mood, and texture.`);
    lines.push(`Reserve a calm, low-detail zone across at least 60% of the canvas to keep future text readable.`);
    if (primaryHex || secondaryHex) {
      lines.push(`Palette lock: primary ${primaryHex || '—'}, secondary ${secondaryHex || '—'}.`);
    }
    lines.push(`Constraints: Use the TEMPLATE canvas exactly; preserve pixel dimensions; fill edge-to-edge; square corners; no borders.`);
    return lines.join('\n');
  }

  // FRONT
  lines.push(`You are REVISING the FRONT label for a bottle of ${alcoholName} (${orientation}).`);
  lines.push(`Start from the PRIOR OUTPUT image and address ONLY these revision notes:`);
  if (critique) {
    for (const ln of String(critique).split(/\r?\n/).filter(Boolean)) {
      lines.push(`- ${ln}`);
    }
  }
  if (Array.isArray(musts) && musts.length) {
    lines.push(`Hard requirements (must keep):`);
    for (const m of musts) lines.push(`- ${m}`);
  }
  if (primaryHex || secondaryHex) {
    lines.push(`Palette lock: primary ${primaryHex || '—'}, secondary ${secondaryHex || '—'}.`);
  }
  lines.push(`Constraints: Use the TEMPLATE canvas exactly; preserve pixel dimensions; fill edge-to-edge; square corners; no borders. Keep text highly readable with adequate contrast.`);
  return lines.join('\n');
}

// Build Gemini contents (parts array) in one place for legibility
async function buildContents({ templateUrl, logoInline, designSide, inspirationInline, finalPrompt }) {
  const parts = [];

  // Template canvas
  if (templateUrl) {
    try {
      const templateInline = await fetchTemplateInlineData(templateUrl);
      if (templateInline) {
        parts.push({ text: "TEMPLATE CANVAS — Use this as the exact base. Preserve its pixel dimensions and aspect ratio. Fill edge-to-edge with no borders; corners must be square." });
        parts.push({ inlineData: templateInline });
      }
    } catch (e) {
      console.warn("buildContents: template fetch failed:", e?.message || e);
    }
  }

  // Logo (front only, when provided)
  if (logoInline && designSide === "front") {
    parts.push({ text: "LOGO — Include this logo exactly as provided. Do not alter its colours or proportions." });
    parts.push({ inlineData: logoInline });
  }

  // Inspiration (front label, for back designs)
  if (designSide === "back" && inspirationInline) {
    parts.push({ text: "FRONT LABEL — Use this image strictly as style inspiration for the BACK label background. Match palette, texture, motifs, and visual weight. Do NOT copy any text, logos, characters, creatures, people, animals, bottles, or any identifiable objects from it." });
    parts.push({ inlineData: inspirationInline });
  }

  // Final textual brief (kept as a single block for readability)
  parts.push({ text: `${finalPrompt}\n\nRe-state constraints: Use the template as the base. Preserve its pixel dimensions. No white borders; square corners. If BACK, no text or icons at all.` });

  return [{ role: "user", parts }];
}

// Build Gemini contents for a REVISION request (includes PRIOR OUTPUT)
async function buildRevisionContents({ templateUrl, designSide, inspirationInline, priorInline, finalPrompt }) {
  const parts = [];

  // Template canvas (always first)
  if (templateUrl) {
    try {
      const templateInline = await fetchTemplateInlineData(templateUrl);
      if (templateInline) {
        parts.push({ text: "TEMPLATE CANVAS — Use this as the exact base. Preserve its pixel dimensions and aspect ratio. Fill edge-to-edge with no borders; corners must be square." });
        parts.push({ inlineData: templateInline });
      }
    } catch (e) {
      console.warn("buildRevisionContents: template fetch failed:", e?.message || e);
    }
  }

  // For BACK revisions, optionally include the accepted FRONT label for style cohesion
  if (designSide === 'back' && inspirationInline) {
    parts.push({ text: "FRONT LABEL — Style inspiration only for the BACK label background. Match palette, texture, motifs, and visual weight. Do NOT copy any text, logos, characters, creatures, people, animals, bottles, or identifiable objects." });
    parts.push({ inlineData: inspirationInline });
  }

  // Prior output to revise (critical)
  if (priorInline) {
    parts.push({ text: "PRIOR OUTPUT — Revise THIS image to satisfy the revision notes. Do not discard it or start a new concept." });
    parts.push({ inlineData: priorInline });
  }

  // Final textual brief
  parts.push({ text: finalPrompt });

  return [{ role: "user", parts }];
}



// --- Improved checkAcceptableDimensions for 25% tolerance, returns trimmed image and ratio info ---
async function checkAcceptableDimensions(attempt, dataUrl, targetWidthMm, targetHeightMm, opts = {}) {
  const { tolerance = 0.25, trimThreshold = 10 } = opts; // 25% tolerance
  try {
    const base64 = dataUrl.split(',')[1];
    const inputBuffer = Buffer.from(base64, 'base64');
    addDebugImage(inputBuffer, `attempt${attempt}-raw`);

    // 1) Trim white borders first
    const { buffer: trimmedBuffer, original, cropped, removed } = await trimWhiteBorder(inputBuffer, { threshold: trimThreshold });
    addDebugImage(trimmedBuffer, `attempt${attempt}-trimmed`);

    // 2) Check aspect ratio tolerance
    const targetRatio = targetWidthMm / targetHeightMm;
    const pixelRatio = (cropped.width || 1) / (cropped.height || 1);
    const ratioDiff = Math.abs(pixelRatio - targetRatio) / targetRatio;
    const acceptable = ratioDiff <= tolerance;

    const trimmedDataUrl = `data:image/png;base64,${trimmedBuffer.toString('base64')}`;

    return {
      acceptable,
      ratioDiff,
      targetRatio,
      pixelRatio,
      trimmedBuffer,
      trimmedDataUrl,
      original,
      cropped,
      removed,
    };
  } catch (e) {
    console.error('checkAcceptableDimensions error:', e?.message || e);
    return { acceptable: false, error: String(e?.message || e) };
  }
}

function addDebugImage(buffer, label) {
  if (!debugOn) return;
  // Keep as data URL for easy viewing in dev; no console logging here
  debugImages.push({
    label,
    dataUrl: `data:image/png;base64,${buffer.toString('base64')}`,
  });
}

async function main(arg, { qs, method }) {
  try {
    // Read incoming payload from Shopify App Proxy (qs) and JSON body
    const body = await readBody(arg);
    // Debug toggle: /function?debug=1 or body.debug=true
    const qsDebug = qs?.debug;
    debugOn = qsDebug === '1' || qsDebug === 'true' || body.debug === true;
    // Reset collector per-invocation
    debugImages.length = 0;

    // Accept both JSON keys and qs aliases
    const alcoholName = body.alcoholName ?? qs.alcoholName ?? "";
    const rawBottle = body.bottleName ?? qs.bottleName ?? "";
    const sessionId = body.sessionId ?? qs.sessionId ?? "";
    const primaryIn = body.primaryColor ?? qs.primaryColor ?? '';
    const secondaryIn = body.secondaryColor ?? qs.secondaryColor ?? '';

    // Revision-specific fields
    const revisionSidesIn = body.revisionSides ?? qs.revisionSides ?? body.side ?? qs.side ?? "";
    const critique = body.critique ?? qs.critique ?? "";
    // Allow musts as array or newline-separated string
    let musts = body.musts ?? qs.musts ?? [];
    if (typeof musts === 'string') {
      musts = musts.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    } else if (!Array.isArray(musts)) {
      musts = [];
    }

    // Prior images (data URLs)
    const priorFrontDataUrl =
      pickDataUrl(body, ['priorFrontImage', 'frontImage']) ||
      pickDataUrl(qs, ['priorFrontImage', 'frontImage']) || "";
    const priorBackDataUrl =
      pickDataUrl(body, ['priorBackImage', 'backImage']) ||
      pickDataUrl(qs, ['priorBackImage', 'backImage']) || "";

    // Optional front inspiration to keep cohesion for back revisions
    const frontInspirationDataUrl =
      pickDataUrl(body, ['frontInspiration', 'inspirationDataUrl']) ||
      pickDataUrl(qs, ['frontInspiration', 'inspirationDataUrl']) || "";

    console.log('review-ai-label incoming:', {
      method,
      alcoholName,
      bottleName: rawBottle,
      revisionSides: Array.isArray(revisionSidesIn) ? revisionSidesIn : String(revisionSidesIn || '').split(',').map(s => s.trim()).filter(Boolean),
      primaryHex: normalizeHex(primaryIn),
      secondaryHex: normalizeHex(secondaryIn),
      hasCritique: Boolean(critique),
      mustsCount: Array.isArray(musts) ? musts.length : 0,
      hasPriorFront: Boolean(priorFrontDataUrl),
      hasPriorBack: Boolean(priorBackDataUrl),
      hasFrontInspiration: Boolean(frontInspirationDataUrl),
      sessionId: sessionId ? String(sessionId).slice(0, 6) + '…' : '',
    });

    // Normalize and validate sides
    let revisionSides = Array.isArray(revisionSidesIn)
      ? revisionSidesIn
      : String(revisionSidesIn || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
    if (revisionSides.length === 0) revisionSides = ['front']; // default

    const bottleName = normaliseBottle(rawBottle);
    if (!bottleName) {
      return { statusCode: 400, body: JSON.stringify({ message: "Bottle name is required" }) };
    }
    if (!critique) {
      return { statusCode: 400, body: JSON.stringify({ message: "Critique (revision notes) is required" }) };
    }

    const sIdSafe = sanitizeId(sessionId) || String(Date.now());
    const bottleSafe = sanitizeId(bottleName.toLowerCase() || "unknown");

    const apiKey = getGeminiKey();
    if (!apiKey) {
      console.error("Gemini API key is missing (set GOOGLE_API_KEY or GEMINI_API_KEY).");
      return { statusCode: 500, body: "Gemini API key is missing" };
    }
    const ai = new GoogleGenAI({ apiKey });
    const modelId = "gemini-2.5-flash-image";

    const primaryHex = normalizeHex(primaryIn);
    const secondaryHex = normalizeHex(secondaryIn);

    const resultImages = { front: null, back: null };
    const sideMessages = {};
    const uploadsBySide = { front: [], back: [] };
    const sidePrimaryUrls = { front: "", back: "" };

    for (const sideRaw of revisionSides) {
      const designSide = normaliseSide(sideRaw);
      const dims = labelDimensions[bottleName]?.[designSide];
      if (!dims) {
        return { statusCode: 400, body: JSON.stringify({ message: `Label dimensions not identified for ${bottleName} ${designSide}` }) };
      }

      // Choose prior image and inspiration as needed per side
      const priorDataUrl = designSide === 'front' ? priorFrontDataUrl : priorBackDataUrl;
      if (!priorDataUrl) {
        return { statusCode: 400, body: JSON.stringify({ message: `Missing prior ${designSide} image (data URL)` }) };
      }
      const priorInline = dataUrlToInlineData(priorDataUrl);

      // For back cohesion, allow an explicit front inspiration; fall back to prior front if provided
      let inspirationInline = null;
      if (designSide === 'back') {
        const inspDataUrl = frontInspirationDataUrl || priorFrontDataUrl;
        inspirationInline = inspDataUrl ? dataUrlToInlineData(inspDataUrl) : null;
      }

      // Template URL derived from bottle & side
      const templateUrl = `https://spirits-studio.s3.eu-west-2.amazonaws.com/templates/${bottleName.toLowerCase()}/${designSide}.png`;

      const finalPrompt = buildRevisionPrompt(alcoholName, dims, critique, musts, designSide, primaryHex, secondaryHex);
      console.log(`[review] ${designSide} prompt:`, finalPrompt.replace(/\n/g, ' | '));
      console.log(`[review] ${designSide} template:`, templateUrl || '(none)');

      const contents = await buildRevisionContents({
        templateUrl,
        designSide,
        inspirationInline,
        priorInline,
        finalPrompt
      });

      // Generate revision
      let images = [];
      let modelMessage = "";
      let response;
      try {
        response = await ai.models.generateContent({
          model: modelId,
          contents,
        });
      } catch (err) {
        console.error(`[review] generateContent error (${designSide}):`, err?.message || err);
        return { statusCode: 502, body: JSON.stringify({ message: "Gemini API error", error: String(err?.message || err) }) };
      }

      const parts = response?.candidates?.[0]?.content?.parts || [];
      for (const part of parts) {
        if (part.text) {
          modelMessage += (modelMessage ? "\n" : "") + part.text;
        } else if (part.inlineData?.data) {
          const mime = part.inlineData?.mimeType || part.inlineData?.mime || 'image/png';
          const dataUrl = `data:${mime};base64,${part.inlineData.data}`;
          images.push(dataUrl);
          try { addDebugImage(Buffer.from(part.inlineData.data, 'base64'), `review-${designSide}-raw-${images.length}`); } catch (_) {}
        }
      }

      // Post-trim white borders on all returned images
      try {
        const trimmedImages = [];
        for (let idx = 0; idx < images.length; idx++) {
          const dataUrl = images[idx];
          const b64 = (dataUrl.split(',')[1] || '').trim();
          if (!b64) { trimmedImages.push(dataUrl); continue; }
          const inputBuf = Buffer.from(b64, 'base64');
          const { buffer: trimmedBuf } = await trimWhiteBorder(inputBuf, { threshold: 12 });
          addDebugImage(trimmedBuf, `review-${designSide}-trimmed-${idx + 1}`);
          const trimmedDataUrl = `data:image/png;base64,${trimmedBuf.toString('base64')}`;
          trimmedImages.push(trimmedDataUrl);
        }
        images = trimmedImages;
      } catch (e) {
        console.warn(`[review] Post-trim step failed (${designSide}); returning untrimmed images.`, e?.message || e);
      }

      const firstImage = images[0] || null;
      resultImages[designSide] = firstImage;
      sideMessages[designSide] = modelMessage;

      // Upload to S3 with same structure as create-front-ai-label
      try {
        const basePrefix = `sessions/${sIdSafe}/${bottleSafe}/review/${designSide}`;
        const uploaded = [];
        for (let i = 0; i < images.length; i++) {
          const { buffer, mimeType } = parseDataUrlToBuffer(images[i]);
          if (!buffer || !buffer.length) continue;
          const ext = mimeToExt(mimeType);
          const index = images.length > 1 ? `_${String(i + 1).padStart(2, "0")}` : "";
          const key = `${basePrefix}_label${index}.${ext}`;
          const url = await uploadBufferToS3({
            buffer,
            contentType: mimeType || "image/png",
            key,
            metadata: {
              "bnb-stage": "review",
              "bnb-design-side": designSide,
              "bnb-bottle": bottleSafe,
              "bnb-source": "ai-review",
              "bnb-session-id": sIdSafe
            }
          });
          uploaded.push({ key, url, contentType: mimeType });
        }
        uploadsBySide[designSide] = uploaded;
        sidePrimaryUrls[designSide] = uploaded[0]?.url || "";
      } catch (uploadErr) {
        console.error(`[review] S3 upload failed (${designSide}):`, uploadErr?.message || uploadErr);
        return {
          statusCode: 502,
          body: JSON.stringify({ message: "S3 upload failed", error: String(uploadErr?.message || uploadErr) })
        };
      }
    }

    const allUploads = [
      ...uploadsBySide.front,
      ...uploadsBySide.back,
    ];

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "AI revision generated successfully",
        model: "gemini-2.5-flash-image",
        bottleName,
        revisionSides,
        width_mm: labelDimensions[bottleName]?.front?.width,
        height_mm: labelDimensions[bottleName]?.front?.height,
        images: resultImages,
        modelMessages: sideMessages,
        s3Uploads: allUploads,
        frontS3Url: sidePrimaryUrls.front,
        backS3Url: sidePrimaryUrls.back,
        ...(debugOn ? { debugImages } : {}),
      }),
    };
  } catch (err) {
    console.error("review-ai-label unhandled error:", err);
    return { statusCode: 500, body: String(err?.message || err), debugImages };
  }
}

export default withShopifyProxy(main, {
  methods: ["POST"],                   // Expect POST from your frontend
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});
