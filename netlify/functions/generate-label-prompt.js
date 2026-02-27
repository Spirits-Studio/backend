import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { GoogleGenAI } from "@google/genai";

function getGeminiKey() {
  return (
    process.env.GOOGLE_API_KEY ||
    process.env.GEMINI_API_KEY ||
    process.env.GOOGLE_AI_STUDIO_KEY ||
    ""
  );
}

function getGeminiModel() {
  return (
    process.env.GEMINI_MODEL_PROMPTS ||
    process.env.GEMINI_MODEL_LATEST ||
    'gemini-pro-latest'
  )
}

function safeParseJSON(input) {
  if (!input) return {};
  if (typeof input === "object") return input;
  try { return JSON.parse(input); } catch { return {}; }
}

async function readBody(req) {
  try {
    const ct = req?.headers?.get?.("content-type") || "";
    if (ct.includes("application/json")) {
      const text = await req.text();
      return safeParseJSON(text);
    }
    const text = await req.text();
    return safeParseJSON(text);
  } catch (e) {
    console.error("readBody error:", e?.message || e);
    return {};
  }
}

const normalize = (v) => (typeof v === "string" ? v.trim() : "");

function buildPromptFromInputs(payload) {
  const theme = normalize(payload.theme);
  const subTheme = normalize(payload.subTheme);
  const mainSubjectType = normalize(payload.mainSubjectType);
  const mainSubject = normalize(payload.mainSubject);
  const action = normalize(payload.action);
  const styleFamily = normalize(payload.styleFamily);
  const paletteVibe = normalize(payload.paletteVibe);

  const subject = mainSubject || (mainSubjectType ? mainSubjectType.toLowerCase() : "subject");
  const locationBits = [];
  if (subTheme) locationBits.push(`in a ${subTheme}`);
  if (theme) locationBits.push(`in ${theme}`);
  const location = locationBits.join(", ");

  const style = styleFamily ? `Style should be ${styleFamily}` : "";
  const palette = paletteVibe ? `with a ${paletteVibe} palette` : "";
  const stylePalette = [style, palette].filter(Boolean).join(", ");

  const coreParts = [`A ${subject}`, action, location].filter(Boolean);
  const sentence = [coreParts.join(" "), stylePalette].filter(Boolean).join(". ");
  return sentence.replace(/\s+/g, " ").trim();
}

export default withShopifyProxy(
  async (event, { method }) => {
    try {
      if (method !== "POST") {
        return new Response("Method Not Allowed", { status: 405 });
      }
      const body = await readBody(event);
      const debug = !!body.debug;

      const basePrompt = buildPromptFromInputs(body);
      const apiKey = getGeminiKey();
      if (!apiKey) {
        return new Response(JSON.stringify({ prompt: basePrompt, fallback: true }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }

      const ai = new GoogleGenAI({ apiKey });
      const userPrompt = [
        "You are an expert prompt egineer who specialises in working with Gemini Nano Banana.",
        "Create a concise, production-grade image prompt using the fields below.",
        "Return only the final prompt sentence, no quotes, no labels.",
        "",
        body.theme ? `Concept theme: ${normalize(body.theme)}` : '',
        body.subTheme ? `Sub-theme: ${normalize(body.subTheme)}` : '',
        body.mainSubjectType ? `Main subject type: ${normalize(body.mainSubjectType)}` : '',
        body.mainSubject ? `Main subject: ${normalize(body.mainSubject)}` : '',
        body.action ? `Action: ${normalize(body.action)}` : '',
        body.styleFamily ? `Style family: ${normalize(body.styleFamily)}` : '',
        body.paletteVibe ? `Palette vibe: ${normalize(body.paletteVibe)}`: '',
      ].join("\n");

      const geminiModel = getGeminiModel();

      const result = await ai.models.generateContent({
        model: geminiModel,
        contents: [{ role: "user", parts: [{ text: userPrompt }] }],
        generationConfig: { temperature: 0.6, topP: 0.9, maxOutputTokens: 256 },
      });

      const prompt =
        result?.candidates?.[0]?.content?.parts?.find((p) => typeof p?.text === "string")?.text?.trim() ||
        basePrompt;

      return new Response(JSON.stringify({ prompt, basePrompt, debug: debug ? { body } : undefined }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (e) {
      console.error("generate-label-prompt error:", e);
      return new Response(JSON.stringify({ error: "server_error", message: String(e) }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
  { methods: ["POST"], allowlist: [process.env.SHOPIFY_STORE_DOMAIN], requireShop: true }
);
