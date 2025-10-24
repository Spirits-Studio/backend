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

// Find closes aspect ratio from dimensions map (if needed)
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

async function main(arg, { qs, method }) {
  try {    
    // Read incoming payload from Shopify App Proxy (qs) and JSON body
    const body = await readBody(arg);

    // Accept both JSON keys and qs aliases
    const alcoholName = body.alcoholName ?? qs.alcoholName ?? "";
    const rawBottle = body.bottleName ?? qs.bottleName ?? "";
    const rawSide   = body.designSide ?? qs.designSide ?? "";
    const sessionId = body.sessionId ?? qs.sessionId ?? "";
    const responseModalitiesValue = body.responseModalitiesValue ?? qs.responseModalitiesValue ?? '';
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
    const designSide = normaliseSide(rawSide);
    const dims = labelDimensions[bottleName]?.[designSide] || null;

    console.log('create-ai-label incoming:', {
      method,
      alcoholName,
      bottleName,
      designSide,
      width: dims.width,
      height: dims.height,
      responseModalitiesValue,
      hasPrompt: Boolean(promptIn),
      hasTitle: Boolean(titleIn),
      hasSubtitle: Boolean(subtitleIn),
      primaryHex,
      secondaryHex,
      logoDataUrl,
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
    
    if (!responseModalitiesValue) {
      console.error("Response Modalities not provided");
      return { statusCode: 400, body: JSON.stringify({ message: "Response Modalities not provided" }) };
    }

    function getResponseModalities(responseModalitiesValue) {
      if(responseModalitiesValue === 'image_only') {
        return ['Image'];

      } else if(responseModalitiesValue === 'text_only' || Boolean(titleIn)) {
        return ['Text', 'Image'];

      } else {
        return ['Image'];
      }
    }

    // Build augmented prompt with exact physical constraints (printer-friendly phrasing)
    const promptLines = [];
    const initialPromptLine = `Design a creative and attractive label for a bottle of ${alcoholName}. The canvas should be ${dims.width+2}mm (width) x ${dims.height+2}mm height. Ensure the design covers the entire canvas area with no white space`;
    promptLines.push(initialPromptLine);
    if (promptIn)   promptLines.push(`Design Prompt: ${promptIn}`);
    if (logoInline) {
      promptLines.push(`Incorporate the provided logo unchanged, at the same dimensions into the label design as a prominent feature.`);
    }
    if (titleIn)    promptLines.push(`Bottle Title: ${titleIn}`);
    if (subtitleIn) promptLines.push(`Bottle Subtitle: ${subtitleIn}`);
    if (primaryHex || secondaryHex) {
      promptLines.push(`Palette: ${primaryHex || '—'} (primary), ${secondaryHex || '—'} (secondary)`);
    }

    const finalPrompt =
      `${promptLines.join('\n')}` +
      `- The design must have square edges.`

      console.log("responseModalities", getResponseModalities(responseModalitiesValue))
      console.log("aspectRatio", getClosestAspectRatio(dims.width, dims.height))
      console.log("Final prompt:", finalPrompt.replace(/\n/g, ' | '));

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
        contents: finalPrompt,
        // config: {
          // responseModalities: getResponseModalities(responseModalitiesValue),
          // imageConfig: {
          //   aspectRatio: getClosestAspectRatio(dims.width, dims.height),
          // },
        // }
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
        images,
        modelMessage,
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