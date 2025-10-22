import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { GoogleGenAI } from "@google/genai";
import * as fs from "node:fs";

async function main(arg, { qs, isV2, method, shop }) {
  try {
    console.log("qs:", qs);

    const bottle = qs.bottle;
    const side = qs.side;
    
    console.log("bottle:", bottle);
    console.log("side:", side);

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

    const width = labelDimensions[bottle]?.[side]?.width || null;
    const height = labelDimensions[bottle]?.[side]?.height || null;
    let prompt = qs.prompt || null;

    if(width && height && prompt) {
      prompt += ` The output of the label dimensions are ${width}mm width and ${height}mm height, excluding a 2mm trim on all sides.`;

      const ai = new GoogleGenAI({});

      const response = await ai.models.generateContent({
        model: "gemini-2.5-flash-image",
        contents: prompt,
      });

      console.log("AI Response:", response);

      for (const part of response.candidates[0].content.parts) {
        if (part.text) {
          console.log(part.text);

        } else if (part.inlineData) {
          const imageData = part.inlineData.data;
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