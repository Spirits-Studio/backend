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
          // File/Blob – convert supported image inputs to data URLs
          const mime = value.type || 'application/octet-stream';
          const ab = await value.arrayBuffer();
          const b64 = Buffer.from(ab).toString('base64');
          if (key === 'logo') {
            obj.logoDataUrl = `data:${mime};base64,${b64}`;
            obj.logoName = value.name || 'logo';
          } else if (key === "character") {
            obj.characterDataUrl = `data:${mime};base64,${b64}`;
            obj.characterName = value.name || "character";
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

function pickHttpUrl(obj, keys = []) {
  for (const k of keys) {
    const v = obj?.[k];
    if (typeof v !== "string") continue;
    const trimmed = v.trim();
    if (/^https?:\/\//i.test(trimmed)) return trimmed;
  }
  return "";
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

async function urlToInlineData(url) {
  try {
    const href = String(url || "").trim();
    if (!/^https?:\/\//i.test(href)) return null;
    const res = await fetch(href);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const mime = res.headers.get("content-type") || "image/png";
    const ab = await res.arrayBuffer();
    if (!ab || !ab.byteLength) return null;
    return { mimeType: mime, data: Buffer.from(ab).toString("base64") };
  } catch (error) {
    console.warn("urlToInlineData failed:", error?.message || error);
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


// --- Label dimension map (mm) ---
const labelDimensions = {
  Polo:   { width: 110, height: 65 },
  Outlaw: { width: 55,  height: 95 },
  Antica: { width: 110, height: 110 },
  Manila: { width: 135, height: 50 },
  Origin: { width: 115, height: 45 },
};

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

async function resolveInputAsset({
  body,
  qs,
  dataKeys = [],
  urlKeys = [],
  s3Prefix,
  assetName,
  metadata = {},
}) {
  const dataUrl = pickDataUrl(body, dataKeys) || pickDataUrl(qs, dataKeys);
  const directUrl = pickHttpUrl(body, urlKeys) || pickHttpUrl(qs, urlKeys);

  let inlineData = null;
  let inputUrl = directUrl || "";

  if (dataUrl) {
    inlineData = dataUrlToInlineData(dataUrl);
    const { buffer, mimeType } = parseDataUrlToBuffer(dataUrl);
    if (!inlineData || !buffer || !buffer.length) {
      const err = new Error(`${assetName} must be a valid data URL when provided.`);
      err.status = 400;
      throw err;
    }

    const ext = mimeToExt(mimeType);
    const key = `${s3Prefix}_${assetName}_${Date.now()}.${ext}`;
    inputUrl = await uploadBufferToS3({
      buffer,
      contentType: mimeType || "application/octet-stream",
      key,
      metadata,
    });
    return { inlineData, inputUrl, hasDataUrl: true };
  }

  if (directUrl) {
    inlineData = await urlToInlineData(directUrl);
  }

  return { inlineData, inputUrl, hasDataUrl: false };
}

// --- Normalise bottle & side casing to match keys above ---
function normaliseBottle(name = "") {
  if (!name) return "";
  return name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
}
function buildCreatePrompt({ alcoholName, dims, promptIn, logoInline, characterInline, titleIn, subtitleIn, primaryHex, secondaryHex, includeHexes }) {
  const orientation =
    dims.width === dims.height ? 'square' :
    dims.width >= dims.height ? 'landscape' : 'portrait';

  const promptLines = [];
  const regulatoryInfo = [];

  promptLines.push(`You are designing the FRONT label for a bottle of ${alcoholName} (${orientation}).`);
  promptLines.push(`The final label must be ${orientation}, with width of ${dims.width}mm and height of ${dims.height}mm.`);
  promptLines.push(`Fill the canvas completely completely (no borders, no transparent elements), and ensure that the corners are square. Do not include any rulers or measurements - this is the final print file with no bleed.`);

  if (promptIn) promptLines.push(`Creative direction: ${promptIn}`);
  if (logoInline) promptLines.push(`Include the provided LOGO exactly as given (do not alter).`);
  if (characterInline) promptLines.push(`Include the provided CHARACTER/REFERENCE image in the composition and keep it recognisable.`);
  if (titleIn) promptLines.push(`Bottle title text: "${titleIn}".`);
  subtitleIn ? promptLines.push(`Bottle subtitle text: "${subtitleIn}".`) : promptLines.push(`No subtitle text is needed.`);
  if (includeHexes) {
    promptLines.push(`Palette: ${primaryHex || '—'} (primary), ${secondaryHex || '—'} (secondary).`);
  }

  regulatoryInfo.push("40% ABV");
  promptLines.push(`Include the following regulatory information clearly and legibly: ${regulatoryInfo.join(', ')}.`);
  promptLines.push(`Ensure all text is highly readable with adequate contrast.`);

  return promptLines.join('\n');
}

function buildRevisePrompt({
  alcoholName,
  dims,
  promptIn,
  logoInline,
  characterInline,
  titleIn,
  subtitleIn,
  primaryHex,
  secondaryHex,
  includeHexes,
  critique,
}) {
  const orientation =
    dims.width === dims.height ? 'square' :
    dims.width >= dims.height ? 'landscape' : 'portrait';

  const promptLines = [];
  const regulatoryInfo = [];

  promptLines.push(`You are updating the existing FRONT label for a bottle of ${alcoholName} (${orientation}).`);
  promptLines.push(`You are given the current label design as an image. Start from that design and MODIFY it according to the revision notes, rather than creating a totally new concept. DO NOT change any elements that are not mentioned in the revision notes.`);
  promptLines.push(`Keep the overall layout, style, and key visual identity unless the revision notes explicitly say otherwise.`);

  if (critique) promptLines.push(`Revision notes (these are the most important instructions): ${critique}`);
  if (logoInline) promptLines.push(`If a LOGO is provided, keep it consistent and unaltered.`);
  if (characterInline) promptLines.push(`If a CHARACTER/REFERENCE image is provided, preserve recognisable features while applying the revision notes.`);
  if (includeHexes) {
    promptLines.push(`Palette: ${primaryHex || '—'} (primary), ${secondaryHex || '—'} (secondary).`);
  }

  regulatoryInfo.push("40% ABV");
  promptLines.push(`Ensure the regulatory information remains clearly readable: ${regulatoryInfo.join(', ')}.`);
  promptLines.push(`Make sure all text is highly readable with adequate contrast, and avoid adding extra text that is not requested in the revision notes.`);

  return promptLines.join('\n');
}



// Build Gemini contents (parts array) in one place for legibility
async function buildContents({
  logoInline,
  characterInline,
  finalPrompt,
  previousInline,
  isRevision,
}) {
  const parts = [];

  if(finalPrompt) {
    parts.push({ text: finalPrompt });
  }

  // If this is a revision, start by giving the existing label image
  if (isRevision && previousInline) {
    parts.push({
      text: "EXISTING LABEL — This is the current label design. Start from this and update it according to the revision notes, instead of creating a totally new design.",
    });
    parts.push({ inlineData: previousInline });
  }

  // Logo (front only, when provided)
  if (logoInline) {
    parts.push({
      text: "LOGO — Include this logo exactly as provided. Do not alter its colours or proportions.",
    });
    parts.push({ inlineData: logoInline });
  }

  if (characterInline) {
    parts.push({
      text: "CHARACTER/REFERENCE IMAGE — Incorporate this image in the label and keep key traits recognisable.",
    });
    parts.push({ inlineData: characterInline });
  }

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
    
    // Accept both JSON keys and qs aliases
    const alcoholName = body.alcoholName ?? qs.alcoholName ?? "";
    const rawBottle = body.bottleName ?? qs.bottleName ?? "";
    const sessionId = body.sessionId ?? qs.sessionId ?? "";
    const titleIn = body.title ?? qs.title ?? '';
    const subtitleIn = body.subtitle ?? qs.subtitle ?? '';
    const promptIn = body.prompt ?? qs.prompt ?? "";
    const primaryIn = body.primaryColor ?? qs.primaryColor ?? '';
    const secondaryIn = body.secondaryColor ?? qs.secondaryColor ?? '';
    const includeHexes = body.includeHexes ?? qs.includeHexes ?? '';
    
    const primaryHex = normalizeHex(primaryIn);
    const secondaryHex = normalizeHex(secondaryIn);
    
    const critique = body.critique ?? qs.critique ?? "";
    const previousImage = body.previousImage ?? qs.previousImage ?? "";
    const previousImageIsDataUrl =
      typeof previousImage === "string" && previousImage.startsWith("data:");
    const previousImageIsUrl =
      typeof previousImage === "string" && /^https?:\/\//i.test(previousImage);
    const previousImageInline = previousImageIsDataUrl
      ? dataUrlToInlineData(previousImage)
      : (previousImageIsUrl ? await urlToInlineData(previousImage) : null);
    const hasPreviousImage = Boolean(previousImageInline);
    const qsDebug = qs?.debug;
    debugOn = qsDebug === '1' || qsDebug === 'true' || body.debug === true;
    
    // Reset collector per-invocation
    debugImages.length = 0;

    const logoDataUrl = pickDataUrl(body, ["logoDataUrl", "logo"]) || pickDataUrl(qs, ["logoDataUrl", "logo"]);
    const logoUrlInput =
      pickHttpUrl(body, ["logoUrl", "input_logo_url", "inputLogoUrl"]) ||
      pickHttpUrl(qs, ["logoUrl", "input_logo_url", "inputLogoUrl"]);
    const characterDataUrl =
      pickDataUrl(body, ["characterDataUrl", "character"]) || pickDataUrl(qs, ["characterDataUrl", "character"]);
    const characterUrlInput =
      pickHttpUrl(body, ["characterUrl", "input_character_url", "inputCharacterUrl"]) ||
      pickHttpUrl(qs, ["characterUrl", "input_character_url", "inputCharacterUrl"]);

    const bottleName = normaliseBottle(rawBottle);
    const dims = labelDimensions[bottleName] || null;
    const sIdSafe = sanitizeId(sessionId) || String(Date.now());
    const bottleSafe = sanitizeId(bottleName.toLowerCase() || "unknown");

        console.log('create-front-ai-label incoming:', {
          method,
          alcoholName,
          bottleName,
          designSide: 'front',
          width: dims?.width,
          height: dims?.height,
          hasPrompt: Boolean(promptIn),
          hasTitle: Boolean(titleIn),
          hasSubtitle: Boolean(subtitleIn),
          includeHexes: includeHexes,
          primaryHex,
          secondaryHex,
          hasLogoDataUrl: Boolean(logoDataUrl),
          hasLogoUrlInput: Boolean(logoUrlInput),
          hasCharacterDataUrl: Boolean(characterDataUrl),
          hasCharacterUrlInput: Boolean(characterUrlInput),
          sessionId: sessionId ? String(sessionId).slice(0, 6) + '…' : '',
          hasCritique: Boolean(critique),
          hasPreviousImage,
          critiquePreview: critique ? String(critique).slice(0, 160) : "",
          critique,
          debug: false,
        });

    if (!dims) {
      console.error("Label Dimensions not identified:", { bottleName });
      return {
        statusCode: 400,
        body: JSON.stringify({ message: "Label Dimensions not identified", bottleName }),
      };
    }
    if (!promptIn && !critique) {
      console.error("Prompt/Critique not provided");
      return { statusCode: 400, body: JSON.stringify({ message: "Prompt not provided" }) };
    }
    if (critique && previousImage && !hasPreviousImage) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          message: "previousImage must be a valid data URL or publicly fetchable HTTP(S) URL"
        }),
      };
    }

    let logoInline = null;
    let characterInline = null;
    let inputLogoUrl = logoUrlInput || "";
    let inputCharacterUrl = characterUrlInput || "";
    try {
      const inputPrefix = `sessions/${sIdSafe}/${bottleSafe}/front_input`;
      const [logoAsset, characterAsset] = await Promise.all([
        resolveInputAsset({
          body,
          qs,
          dataKeys: ["logoDataUrl", "logo"],
          urlKeys: ["logoUrl", "input_logo_url", "inputLogoUrl"],
          s3Prefix: inputPrefix,
          assetName: "logo",
          metadata: {
            "bnb-stage": "session",
            "bnb-design-side": "front",
            "bnb-bottle": bottleSafe,
            "bnb-source": "ai-input-logo",
            "bnb-session-id": sIdSafe,
          },
        }),
        resolveInputAsset({
          body,
          qs,
          dataKeys: ["characterDataUrl", "character"],
          urlKeys: ["characterUrl", "input_character_url", "inputCharacterUrl"],
          s3Prefix: inputPrefix,
          assetName: "character",
          metadata: {
            "bnb-stage": "session",
            "bnb-design-side": "front",
            "bnb-bottle": bottleSafe,
            "bnb-source": "ai-input-character",
            "bnb-session-id": sIdSafe,
          },
        }),
      ]);
      logoInline = logoAsset.inlineData;
      characterInline = characterAsset.inlineData;
      inputLogoUrl = logoAsset.inputUrl || "";
      inputCharacterUrl = characterAsset.inputUrl || "";
    } catch (assetError) {
      return {
        statusCode: assetError?.status || 500,
        body: JSON.stringify({
          message: assetError?.message || "Failed to process input assets",
        }),
      };
    }

    const isRevision = Boolean(critique && hasPreviousImage);

    const finalPrompt = isRevision
      ? buildRevisePrompt({
          alcoholName,
          dims,
          promptIn,
          logoInline,
          characterInline,
          titleIn,
          subtitleIn,
          primaryHex,
          secondaryHex,
          includeHexes,
          critique,
        })
      : buildCreatePrompt({
          alcoholName,
          dims,
          promptIn,
          logoInline,
          characterInline,
          titleIn,
          subtitleIn,
          primaryHex,
          secondaryHex,
          includeHexes,
        });
    
    console.log("Final prompt:", finalPrompt.replace(/\n/g, ' | '));
        
    const apiKey = getGeminiKey();
    if (!apiKey) {
      console.error("Gemini API key is missing (set GOOGLE_API_KEY or GEMINI_API_KEY).");
      return { statusCode: 500, body: "Gemini API key is missing" };
    }

    const ai = new GoogleGenAI({ apiKey });
    const modelId = "gemini-3-pro-image-preview";

    // Prepare contents in one place for better legibility
    let genContents = await buildContents({
      logoInline,
      characterInline,
      finalPrompt,
      previousInline: previousImageInline,
      isRevision,
    });

    // --- Simple single-shot generation for dev: return whatever the AI creates ---
    // (All trimming, aspect checks, and composing commented out temporarily.)
    let images = [];
    let modelMessage = "";

    let response;
    console.log("contents sent to Gemini:", Array.isArray(genContents) ? genContents.map(c => c.parts ? `[${c.parts.length} parts]` : c.text).join(' | ') : genContents);
    try {
      response = await ai.models.generateContent({
        model: modelId,
        contents: genContents,
      });
    } catch (err) {
      console.error("Gemini generateContent error:", err?.message || err);
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
        // Debug preview
        try { addDebugImage(Buffer.from(part.inlineData.data, 'base64'), `ai-output-${images.length}`); } catch (_) {}
      }
    }
    // --- Post-process: trim white borders from all AI images (front & back) ---
    // Uses trimWhiteBorder to find the first non-white pixel from all sides.
    try {
      const processedImages = [];
      for (let idx = 0; idx < images.length; idx++) {
        const dataUrl = images[idx];
        const b64 = (dataUrl.split(',')[1] || '').trim();
        if (!b64) { processedImages.push(dataUrl); continue; }

        const inputBuf = Buffer.from(b64, 'base64');
        const { buffer: trimmedBuf } = await trimWhiteBorder(inputBuf, { threshold: 12 });

        // Detect a reasonable background colour to use when composing to target size
        let bgHex = primaryHex || secondaryHex || '#FFFFFF';
        try {
          const detected = await detectFirstContentColour(trimmedBuf);
          if (detected) bgHex = detected;
        } catch (colourErr) {
          console.warn('detectFirstContentColour failed, fallback to palette/default.', colourErr?.message || colourErr);
        }

        // Compose onto a canvas sized to the exact mm dimensions (converted via DPI)
        let composedBuf = trimmedBuf;
        try {
          composedBuf = await composeLabel(trimmedBuf, dims.width, dims.height, bgHex);
        } catch (composeErr) {
          console.warn('composeLabel failed; returning trimmed image instead.', composeErr?.message || composeErr);
        }

        // Debug previews (only if debug=1)
        try { addDebugImage(trimmedBuf, `ai-output-${idx + 1}-trimmed`); } catch(_) {}
        try { addDebugImage(composedBuf, `ai-output-${idx + 1}-composed`); } catch(_) {}

        const composedDataUrl = `data:image/png;base64,${composedBuf.toString('base64')}`;
        processedImages.push(composedDataUrl);
      }
      images = processedImages;
    } catch (e) {
      console.warn('Post-process resize step failed; returning original images.', e?.message || e);
    }
    const firstImage = images[0] || '';

    // Upload generated images to S3
    const bucket = process.env.S3_BUCKET || DEFAULT_BUCKET;
    const basePrefix = `sessions/${sIdSafe}/${bottleSafe}/front`;

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
          "bnb-stage": "session",
          "bnb-design-side": "front",
          "bnb-bottle": bottleSafe,
          "bnb-source": "ai-generate",
          "bnb-session-id": sIdSafe
        }
      });
      uploaded.push({ key, url, contentType: mimeType });
    }
    const frontS3Url = uploaded[0]?.url || "";

    // // Old retry/processing path (kept for reference):
    // // - Trim white borders
    // // - Check aspect ratio
    // // - Retry up to 3x if >25% out
    // // - Compose final label (dims.width+2 x dims.height+2) with edge color background
    // //
    // // Code removed for now per request

    const responsePayload = {
        message: "AI image generated successfully",
        model: modelId,
        bottleName,
        designSide: 'front',
        width_mm: dims.width,
        height_mm: dims.height,
        // base64 for preview
        images,
        frontImage: firstImage,
        // S3 results
        s3Uploads: uploaded,
        frontS3Url,
        inputLogoUrl,
        inputCharacterUrl,
        modelMessage,
        ...(debugOn ? { debug: { enabled: true, imagesCaptured: debugImages.length } } : {}),
      };

    return {
      statusCode: 200,
      body: JSON.stringify(responsePayload),
    };
  } catch (err) {
    console.error("create-front-ai-label unhandled error:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: String(err?.message || err || "create-front-ai-label failed"),
        ...(debugOn ? { debug: { enabled: true, imagesCaptured: debugImages.length } } : {}),
      }),
    };
  }
}

export default withShopifyProxy(main, {
  methods: ["POST"],                   // Expect POST from your frontend
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});
