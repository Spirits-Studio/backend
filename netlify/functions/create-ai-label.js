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
    // Read incoming payload from Shopify App Proxy (qs) and JSON body
    const body = safeParseJSON(arg?.body);

    // Accept both JSON keys and qs aliases (bottle|bottleName, side|designSide)
    const rawBottle = body.bottleName ?? body.bottle ?? qs.bottleName ?? qs.bottle ?? "";
    const rawSide   = body.designSide ?? body.side ?? qs.designSide ?? qs.side ?? "";
    const promptIn  = body.prompt ?? qs.prompt ?? "";
    const sessionId = body.sessionId ?? qs.sessionId ?? "";

    const bottleName = normaliseBottle(rawBottle);
    const designSide = normaliseSide(rawSide);
    const dims = labelDimensions[bottleName]?.[designSide] || null;

    console.log("create-ai-label incoming:", {
      method,
      bottleName,
      designSide,
      hasPrompt: Boolean(promptIn),
      sessionId: sessionId ? String(sessionId).slice(0, 6) + "…" : "",
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
    const finalPrompt =
      `${promptIn}\n\n` +
      `Important production constraints:\n` +
      `- The design must fit a label area of ${dims.width}mm (width) × ${dims.height}mm (height).\n` +
      `- Keep a 2mm trim (bleed) on all sides; keep key text/logos inside a safe margin.\n` +
      `- Provide a clean, print-ready image without borders beyond the trim.`;

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