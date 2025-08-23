// OAuth callback: verifies HMAC + state, exchanges code -> permanent token
import crypto from "crypto";

function verifyHmac(query) {
  const { hmac, signature, ...rest } = query;
  const provided = hmac || signature;
  if (!provided) return false;

  const message = Object.keys(rest)
    .sort()
    .map((k) => `${k}=${rest[k] ?? ""}`)
    .join("&");

  const digest = crypto
    .createHmac("sha256", process.env.SHOPIFY_CLIENT_SECRET)
    .update(message)
    .digest("hex");

  const a = Buffer.from(digest, "utf8");
  const b = Buffer.from(provided, "utf8");
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function getCookieValue(headers, name) {
  const cookieHeader = headers.cookie || headers.Cookie || "";
  const parts = cookieHeader.split(";").map((s) => s.trim());
  for (const p of parts) {
    if (p.startsWith(name + "=")) return decodeURIComponent(p.split("=")[1]);
  }
  return null;
}

export const handler = async (event) => {
  try {
    const qs = event.queryStringParameters || {};
    const shop = (qs.shop || "").toLowerCase();
    const code = qs.code;
    const state = qs.state;

    if (!shop || !shop.endsWith(".myshopify.com")) {
      return { statusCode: 400, body: "Invalid shop" };
    }
    if (!code || !state) {
      return { statusCode: 400, body: "Missing code/state" };
    }
    if (!verifyHmac(qs)) {
      return { statusCode: 401, body: "Invalid HMAC" };
    }

    // check state (nonce)
    const cookieState = getCookieValue(event.headers, "shopify_oauth_state");
    if (!cookieState || cookieState !== state) {
      return { statusCode: 401, body: "Invalid state" };
    }

    // Exchange code -> permanent access token
    const tokenRes = await fetch(`https://${shop}/admin/oauth/access_token`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: process.env.SHOPIFY_CLIENT_ID,
        client_secret: process.env.SHOPIFY_CLIENT_SECRET,
        code
      })
    });

    if (!tokenRes.ok) {
      return { statusCode: tokenRes.status, body: await tokenRes.text() };
    }

    const data = await tokenRes.json(); // { access_token, scope }
    const accessToken = data.access_token;

    // TODO: persist accessToken for this shop.
    // Example: save to Airtable keyed by `shop`, or Netlify Blobs, etc.
    console.log("SHOPIFY ACCESS TOKEN for", shop, "=>", accessToken);

    // Clear state cookie & redirect somewhere friendly
    return {
      statusCode: 302,
      headers: {
        "Set-Cookie": "shopify_oauth_state=; Max-Age=0; Path=/; SameSite=Lax; Secure",
        Location: `https://${shop}/admin/apps` // or your app landing
      },
      body: ""
    };
  } catch (e) {
    return { statusCode: 500, body: String(e) };
  }
};