const ALLOWED_ORIGINS = [
  'https://barrelnbond.com',
  'https://build.barrelnbond.com',
  'https://barrel-n-bond-backend.netlify.app'
];

export const handler = async (event) => {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: corsHeaders(event.headers?.origin),
    };
  }

  if (event.httpMethod !== 'GET' && event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  try {
    const res = await fetch('https://oauth.zakeke.com/connect/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: process.env.ZAKEKE_CLIENT_ID,
        client_secret: process.env.ZAKEKE_SECRET_KEY,
        grant_type: 'client_credentials',
        scope: 'api' // <-- optional, include if your app requires it
      })
    });

    if (!res.ok) {
      const errText = await res.text();
      return { statusCode: res.status, headers: corsHeaders(event.headers?.origin), body: errText };
    }

    const data = await res.json(); // { access_token, token_type, expires_in, ... }

    return {
      statusCode: 200,
      headers: {
        ...corsHeaders(event.headers?.origin),
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store' // don’t cache tokens
      },
      body: JSON.stringify({ token: data.access_token, expiresIn: data.expires_in })
    };
  } catch (err) {
    return { statusCode: 500, headers: corsHeaders(event.headers?.origin), body: String(err) };
  }
};

function corsHeaders(origin) {
  const allow = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    'Access-Control-Allow-Origin': allow,
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization'
  };
}