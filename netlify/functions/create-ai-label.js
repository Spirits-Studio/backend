import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { GoogleGenerativeAI } from "@google/generative-ai";
import * as fs from "node:fs";

async function main(arg, { qs, isV2, method, shop }) {
  try {
    console.log("qs:", qs);

    const bottle = qs.bottle;
    const designSide = qs.designSide;
    
    console.log("bottle:", bottle);
    console.log("designSide:", designSide);

    const labelDimensions = {
      Polo: {
        front: { width: 110, height: 65 },
        back: { width: 110, height: 65 }
      },
      Outlaw: {
        front: { width: 55, height: 95 },
        back: { width: 55, height: 95 }
      },
      Antica: {
        front: { width: 110, height: 110 },
        back: { width: 80, height: 100 }
      },
      Manila: {
        front: { width: 135, height: 50 },
        back: { width: 115, height: 40 }
      },
      Origin: {
        front: { width: 115, height: 45 },
        back: { width: 100, height: 45 }
      }
    };

    const width = labelDimensions[bottle]?.[designSide]?.width || null;
    const height = labelDimensions[bottle]?.[designSide]?.height || null;
    let prompt = qs.prompt || null;

    if(width && height && prompt) {
      prompt += ` The output of the label dimensions are ${width}mm width and ${height}mm height, excluding a 2mm trim on all sides.`;
      if (!process.env.GOOGLE_API_KEY) {
        throw new Error("GOOGLE_API_KEY env var is missing");
      }

      const genAI = new GoogleGenerativeAI(process.env.GOOGLE_API_KEY);
      const model = genAI.getGenerativeModel({ model: "gemini-2.5-flash-image" });

      // The generative-ai SDK expects a string or a list of parts for `generateContent`.
      const result = await model.generateContent(prompt);
      const resp = await result.response;

      console.log("AI Response metadata:", {
        promptFeedback: resp.promptFeedback,
        candidates: Array.isArray(resp.candidates) ? resp.candidates.length : 0,
      });

      const parts = (resp.candidates?.[0]?.content?.parts) || [];
      for (const part of parts) {
        if (part.text) {
          console.log(part.text);
        } else if (part.inlineData?.data) {
          const imageData = part.inlineData.data; // base64
          const buffer = Buffer.from(imageData, "base64");
          fs.writeFileSync("gemini-native-image.png", buffer);
          console.log("Image saved as gemini-native-image.png");
        }
      }

      return {
        statusCode: 200,
        body: JSON.stringify({ message: "AI image generated successfully" }),
      };

    } else {
      if(!width || !height) {
        console.error("Label Dimensions not identified: width:", width, " height:", height);
      }

      if(!prompt) {
        console.error("Prompt not provided");
      }

      return {
        statusCode: 400,
        body: JSON.stringify({ message: "Label Dimensions and/or prompt not identified" }),
      };
      

    }
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: err.message };
  }
}

export default withShopifyProxy(main, {
  methods: ["GET"],
  allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
  requireShop: true,
});