// netlify/functions/shopify-auth-start.js
export default async (event) => {
  try {
    if (event.httpMethod !== "GET") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const shop = process.env.SHOPIFY_STORE_DOMAIN; // e.g. wnbrmm-sg.myshopify.com
    const clientId = process.env.SHOPIFY_CLIENT_ID;
    const scopes = process.env.SHOPIFY_OAUTH_SCOPES || "read_products";
    const host = event.headers.host;

    if (!shop || !clientId) {
      return {
        statusCode: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "missing_env", details: { shop: !!shop, clientId: !!clientId } })
      };
    }

    // state nonce
    const state = Math.random().toString(36).slice(2);

    // set a lightweight, httpOnly cookie for state
    const cookie = [
      `shopify_oauth_state=${state}`,
      "Path=/",
      "HttpOnly",
      "SameSite=Lax",
      "Secure",
      "Max-Age=600"
    ].join("; ");

    // where Shopify will call back to after approval
    const redirectUri = `https://${host}/.netlify/functions/shopify-auth-callback`;

    const params = new URLSearchParams({
      client_id: clientId,
      scope: scopes,
      redirect_uri: redirectUri,
      state
      // access_mode=offline is default in new flow
    });

    const location = `https://${shop}/admin/oauth/authorize?${params.toString()}`;

    return {
      statusCode: 302,
      headers: {
        Location: location,
        "Set-Cookie": cookie,
        "Cache-Control": "no-store"
      },
      body: ""
    };
  } catch (e) {
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "auth_start_failed", message: String(e) })
    };
  }
};