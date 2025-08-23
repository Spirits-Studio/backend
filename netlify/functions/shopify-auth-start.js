const crypto = await import("crypto");

function buildAuthUrl({ shop, state, scopes, redirectUri }) {
  const q = new URLSearchParams({
    client_id: process.env.SHOPIFY_CLIENT_ID,
    scope: scopes,
    redirect_uri: redirectUri,
    state,
    // access_mode=offline is default for permanent token in the new flow
  });
  return `https://${shop}/admin/oauth/authorize?${q.toString()}`;
}

export const handler = async (event) => {
  try {
    const shop = process.env.SHOPIFY_STORE_DOMAIN

    if (!shop || !shop.endsWith(".myshopify.com")) {
      return { statusCode: 400, body: "Missing or invalid ?shop" };
    }

    // generate state (nonce)
    const state = crypto.randomBytes(16).toString("hex");

    // set cookie (HttpOnly)
    const cookie = [
      `shopify_oauth_state=${state}`,
      "Path=/",
      "HttpOnly",
      "SameSite=Lax",
      "Secure"
    ].join("; ");

    const redirectUri = `https://${event.headers.host}/.netlify/functions/shopify-auth-callback`;
    const scopes = process.env.SHOPIFY_OAUTH_SCOPES || "read_products";

    const authUrl = buildAuthUrl({ shop, state, scopes, redirectUri });

    return {
      statusCode: 302,
      headers: {
        "Set-Cookie": cookie,
        Location: authUrl
      },
      body: ""
    };
  } catch (e) {
    return { statusCode: 500, body: String(e) };
  }
};