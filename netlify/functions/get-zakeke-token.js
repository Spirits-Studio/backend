export default withShopifyProxy(
  async (event) => {
    try {
      // Optional controls via query string:
      // ?refresh=1 -> bypass cache
      // ?access_type=S2S|C2S -> force access type
      const qs = event.queryStringParameters || {};
      const refresh = qs.refresh === '1' || qs.refresh === 'true';
      const accessType = qs.access_type;

      if (refresh) cache = { token: null, exp: 0 };

      const { token, expires_in, cached } = await fetchZakekeToken({ accessType });

      return jsonResponse(
        200,
        { token, expiresIn: expires_in, cached, accessType: (accessType || process.env.ZAKEKE_ACCESS_TYPE || null) }
      );
    } catch (e) {
      const code = e?.code || 'server_error';
      const status = e?.status && Number.isInteger(e.status) ? e.status : 502;

      return jsonResponse(status, {
        error: code,
        message: e?.message || String(e),
        details: e?.missing || e?.data || undefined
      });
    }
  },
  {
    methods: ['GET', 'POST'],
    // Only allow requests proxied from your Shopify store
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);