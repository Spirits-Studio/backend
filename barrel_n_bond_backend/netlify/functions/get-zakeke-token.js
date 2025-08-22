export async function handler() {
  try {
    const response = await fetch("https://oauth.zakeke.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: process.env.ZAKEKE_CLIENT_ID,
        client_secret: process.env.ZAKEKE_SECRET_KEY,
        grant_type: "client_credentials"
      })
    });

    if (!response.ok) {
      const error = await response.text();
      return { statusCode: response.status, body: error };
    }

    const data = await response.json();

    return {
      statusCode: 200,
      body: JSON.stringify({ token: data.access_token })
    };
  } catch (err) {
    return { statusCode: 500, body: err.message };
  }
}