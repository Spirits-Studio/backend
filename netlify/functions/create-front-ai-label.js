import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { GoogleGenAI } from "@google/genai";
import sharp from "sharp";

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


// --- Label dimension map (mm) ---
const labelDimensions = {
  Polo:   { width: 110, height: 65 },
  Outlaw: { width: 55,  height: 95 },
  Antica: { width: 110, height: 110 },
  Manila: { width: 135, height: 50 },
  Origin: { width: 115, height: 45 },
};

// --- Normalise bottle & side casing to match keys above ---
function normaliseBottle(name = "") {
  if (!name) return "";
  return name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
}
function buildPrompt({ alcoholName, dims, promptIn, logoInline, titleIn, subtitleIn, primaryHex, secondaryHex, templateColour }) {
  const orientation =
    dims.width === dims.height ? 'square' :
    dims.width >= dims.height ? 'landscape' : 'portrait';

  const promptLines = [];
  const regulatoryInfo = [];

  promptLines.push(`You are designing the FRONT label for a bottle of ${alcoholName} (${orientation}).`);
  promptLines.push(`Use the provided TEMPLATE as the exact canvas. Remove the ${templateColour} background colour, and preserve its size, pixel dimensions, and aspect ratio.`);
  promptLines.push(`Fill the template completely (no borders). Corners must be square.`);

  if (promptIn) promptLines.push(`Creative direction: ${promptIn}`);
  if (logoInline) promptLines.push(`Include the provided LOGO exactly as given (do not alter).`);
  if (titleIn) promptLines.push(`Bottle title text: "${titleIn}".`);
  if (subtitleIn) promptLines.push(`Bottle subtitle text: "${subtitleIn}".`);
  if (primaryHex || secondaryHex) {
    promptLines.push(`Palette: ${primaryHex || '—'} (primary), ${secondaryHex || '—'} (secondary).`);
  }

  regulatoryInfo.push("40% ABV"); // extend if desired
  promptLines.push(`Include the following regulatory information clearly and legibly: ${regulatoryInfo.join(', ')}.`);
  promptLines.push(`Ensure all text is highly readable with adequate contrast.`);

  return promptLines.join('\n');
}


// Build Gemini contents (parts array) in one place for legibility
async function buildContents({ templateUrl, logoInline, finalPrompt, templateColour }) {
  const parts = [];

  // Template canvas
  if (templateUrl) {
    try {
      const templateInline = await fetchTemplateInlineData(templateUrl);
      if (templateInline) {
        parts.push({ text: `TEMPLATE CANVAS — Use this as the exact base. Remove the ${templateColour} background colour, and its size, pixel dimensions, and aspect ratio. Fill edge-to-edge with no borders; corners must be square.` });
        parts.push({ inlineData: templateInline });
      }
    } catch (e) {
      console.warn("buildContents: template fetch failed:", e?.message || e);
    }
  }

  // Logo (front only, when provided)
  if (logoInline) {
    parts.push({ text: "LOGO — Include this logo exactly as provided. Do not alter its colours or proportions." });
    parts.push({ inlineData: logoInline });
  }

  // Final textual brief (kept as a single block for readability)
  parts.push({ text: `${finalPrompt}\n\nRe-state constraints: Use the template as the base. Preserve its pixel dimensions. Remove the ${templateColour} background colour. No white borders; square corners.` });

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
    const titleIn = body.title ?? qs.title ?? '';
    const subtitleIn = body.subtitle ?? qs.subtitle ?? '';
    const promptIn = body.prompt ?? qs.prompt ?? "";
    const primaryIn = body.primaryColor ?? qs.primaryColor ?? '';
    const secondaryIn = body.secondaryColor ?? qs.secondaryColor ?? '';

    const primaryHex = normalizeHex(primaryIn);
    const secondaryHex = normalizeHex(secondaryIn);

    // Optional logo as data URL (if client sends it)
    const logoDataUrl = pickDataUrl(body, ['logoDataUrl', 'logo']) || pickDataUrl(qs, ['logoDataUrl']);
    const logoInline = logoDataUrl ? dataUrlToInlineData(logoDataUrl) : null;

    const bottleName = normaliseBottle(rawBottle);
    const dims = labelDimensions[bottleName] || null;

    // Build template URL from bottle/side convention
    const aiLabelTemplateColour = process.env.AI_LABEL_TEMPLATE_COLOUR || 'black';

    const templateUrl = bottleName
      ? `https://barrel-n-bond.s3.eu-west-2.amazonaws.com/templates/2mm_trim_bleed/${bottleName.toLowerCase()}/${aiLabelTemplateColour}/front.png`
      : '';

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
      primaryHex,
      secondaryHex,
      logoDataUrl,
      logoInline: Boolean(logoInline),
      sessionId: sessionId ? String(sessionId).slice(0, 6) + '…' : '',
      usingTemplate: Boolean(templateUrl)
    });

    if (!dims) {
      console.error("Label Dimensions not identified:", { bottleName });
      return {
        statusCode: 400,
        body: JSON.stringify({ message: "Label Dimensions not identified", bottleName }),
      };
    }
    if (!promptIn) {
      console.error("Prompt not provided");
      return { statusCode: 400, body: JSON.stringify({ message: "Prompt not provided" }) };
    }

    const finalPrompt = buildPrompt({
      alcoholName,
      dims,
      promptIn,
      logoInline,
      titleIn,
      subtitleIn,
      primaryHex,
      secondaryHex,
      templateColour: aiLabelTemplateColour
    });
    
    console.log("Final prompt:", finalPrompt.replace(/\n/g, ' | '));
    console.log("Template URL:", templateUrl || '(none)');
        
    const apiKey = getGeminiKey();
    if (!apiKey) {
      console.error("Gemini API key is missing (set GOOGLE_API_KEY or GEMINI_API_KEY).");
      return { statusCode: 500, body: "Gemini API key is missing" };
    }

    const ai = new GoogleGenAI({ apiKey });
    const modelId = "gemini-2.5-flash-image";

    // Prepare contents in one place for better legibility
    let genContents = await buildContents({
      templateUrl,
      logoInline,
      finalPrompt,
      templateColour: aiLabelTemplateColour
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

    // // Old retry/processing path (kept for reference):
    // // - Trim white borders
    // // - Check aspect ratio
    // // - Retry up to 3x if >25% out
    // // - Compose final label (dims.width+2 x dims.height+2) with edge color background
    // //
    // // Code removed for now per request

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "AI image generated successfully",
        model: modelId,
        bottleName,
        designSide: 'front',
        width_mm: dims.width,
        height_mm: dims.height,
        images,
        modelMessage,
        frontImage: firstImage,
        ...(debugOn ? { debugImages } : {}),
      }),
    };
  } catch (err) {
    console.error("create-front-ai-label unhandled error:", err);
    return { statusCode: 500, body: String(err?.message || err), debugImages };
  }
}

export default withShopifyProxy(main, {
  methods: ["POST"],                   // Expect POST from your frontend
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});
