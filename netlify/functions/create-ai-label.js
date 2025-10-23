import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { GoogleGenAI } from "@google/genai";

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
    return { mime, data: b64 };
  } catch { return null; }
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

async function main(arg, { qs, method }) {
  try {
    console.log("arg", arg)
    console.log("qs", qs)
    console.log("method", method)
    
    // Read incoming payload from Shopify App Proxy (qs) and JSON body
    const body = await readBody(arg);
    console.log('parsed body keys:', Object.keys(body || {}));

    // Accept both JSON keys and qs aliases
    const alcoholName = body.alcoholName ?? qs.alcoholName ?? "";
    const rawBottle = body.bottleName ?? qs.bottleName ?? "";
    const rawSide   = body.designSide ?? qs.designSide ?? "";
    const sessionId = body.sessionId ?? qs.sessionId ?? "";
    const titleIn      = body.title ?? qs.title ?? '';
    const subtitleIn   = body.subtitle ?? qs.subtitle ?? '';
    const promptIn  = body.prompt ?? qs.prompt ?? "";
    const primaryIn    = body.primaryColor ?? qs.primaryColor ?? '';
    const secondaryIn  = body.secondaryColor ?? qs.secondaryColor ?? '';

    const primaryHex   = normalizeHex(primaryIn);
    const secondaryHex = normalizeHex(secondaryIn);

    // Optional logo as data URL (if client sends it)
    const logoDataUrl  = pickDataUrl(body, ['logoDataUrl', 'logo']) || pickDataUrl(qs, ['logoDataUrl']);
    const logoInline   = logoDataUrl ? dataUrlToInlineData(logoDataUrl) : null;

    const bottleName = normaliseBottle(rawBottle);
    const designSide = normaliseSide(rawSide);
    const dims = labelDimensions[bottleName]?.[designSide] || null;

    console.log('create-ai-label incoming:', {
      method,
      alcoholName,
      bottleName,
      designSide,
      hasPrompt: Boolean(promptIn),
      hasTitle: Boolean(titleIn),
      hasSubtitle: Boolean(subtitleIn),
      primaryHex,
      secondaryHex,
      logoInline: Boolean(logoInline),
      sessionId: sessionId ? String(sessionId).slice(0, 6) + '…' : '',
    });

    if (!dims) {
      console.error("Label Dimensions not identified:", { bottleName, designSide });
      return {
        statusCode: 400,
        body: JSON.stringify({ message: "Label Dimensions not identified", bottleName, designSide }),
      };
    }
    if (!promptIn) {
      console.error("Prompt not provided");
      return { statusCode: 400, body: JSON.stringify({ message: "Prompt not provided" }) };
    }

    // Build augmented prompt with exact physical constraints (printer-friendly phrasing)
    const promptLines = [];
    const initialPromptLine = 'Design a creative and attractive label for a bottle of .';
    if (titleIn)    promptLines.push(`Bottle Title: ${titleIn}`);
    if (subtitleIn) promptLines.push(`Bottle Subtitle: ${subtitleIn}`);
    if (primaryHex || secondaryHex) {
      promptLines.push(`Palette: ${primaryHex || '—'} (primary), ${secondaryHex || '—'} (secondary)`);
    }
    if (promptIn)   promptLines.push(promptIn);

    const finalPrompt =
      `${promptLines.join('\n')}` +
      `\n\nImportant production constraints:\n` +
      `- The design must fit a label area of ${dims.width}mm (width) × ${dims.height}mm (height).\n` +
      `- Provide a clean, print-ready image without borders beyond the trim at 300dpi and in a CMYK print format.` +
      `- Keep a 2mm trim (bleed) on all sides; keep key text/logos inside a safe margin.\n` +
      `- Return an image precisely the label size + trim: width = ${dims.width+2}mm, height = ${dims.height+2}mm.`;

    const apiKey = getGeminiKey();
    if (!apiKey) {
      console.error("Gemini API key is missing (set GOOGLE_API_KEY or GEMINI_API_KEY).");
      return { statusCode: 500, body: "Gemini API key is missing" };
    }

    // Use the new @google/genai client per official docs
    const ai = new GoogleGenAI({ apiKey });
    const modelId = "gemini-2.5-flash-image";

    let response;
    try {
      response = await ai.models.generateContent({
        model: modelId,
        // The SDK accepts a string or a structured "contents" array; a plain string is fine here.
        contents: finalPrompt,
      });
    } catch (err) {
      console.error("Gemini generateContent error:", err?.message || err);
      // Forward error text if present (commonly contains API_KEY_INVALID)
      return {
        statusCode: 502,
        body: JSON.stringify({ message: "Gemini API error", error: String(err?.message || err) }),
      };
    }

    const parts = response?.candidates?.[0]?.content?.parts || [];
    const images = [];
    let modelMessage = "";

    for (const part of parts) {
      if (part.text) {
        modelMessage += (modelMessage ? "\n" : "") + part.text;
      } else if (part.inlineData?.data) {
        const mime = part.inlineData?.mime || "image/png";
        const dataUrl = `data:${mime};base64,${part.inlineData.data}`;
        images.push(dataUrl);
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "AI image generated successfully",
        model: modelId,
        bottleName,
        designSide,
        width_mm: dims.width,
        height_mm: dims.height,
        images,           // data URLs (base64) if any
        modelMessage,     // optional text returned by model
      }),
    };
  } catch (err) {
    console.error("create-ai-label unhandled error:", err);
    return { statusCode: 500, body: String(err?.message || err) };
  }
}

export default withShopifyProxy(main, {
  methods: ["POST"],                   // Expect POST from your frontend
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});