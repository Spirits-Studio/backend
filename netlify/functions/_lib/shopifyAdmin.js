const DEFAULT_ADMIN_API_VERSION =
  process.env.SHOPIFY_ADMIN_API_VERSION || "2025-07";
const DEFAULT_ORDER_DETAILS_METAFIELD_NAMESPACE =
  process.env.SHOPIFY_ORDER_DETAILS_METAFIELD_NAMESPACE || "ss";
const DEFAULT_ORDER_DETAILS_METAFIELD_KEY =
  process.env.SHOPIFY_ORDER_DETAILS_METAFIELD_KEY || "order_details";

const normalizeText = (value, maxLen = 255) => {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.slice(0, maxLen);
};

const resolveAdminAccessToken = () => {
  const candidates = [
    process.env.SHOPIFY_ADMIN_ACCESS_TOKEN,
    process.env.SHOPIFY_ADMIN_API_ACCESS_TOKEN,
    process.env.SHOPIFY_ACCESS_TOKEN,
  ];

  for (const candidate of candidates) {
    const token = normalizeText(candidate, 4096);
    if (token) return token;
  }

  return null;
};

const normalizeShopDomain = (value) => {
  const text = normalizeText(value, 255);
  if (!text) return null;
  return text.toLowerCase();
};

const toOrderGid = (orderId) => {
  const normalizedOrderId = normalizeText(orderId, 255);
  if (!normalizedOrderId) return null;
  if (normalizedOrderId.startsWith("gid://shopify/Order/")) {
    return normalizedOrderId;
  }
  return `gid://shopify/Order/${normalizedOrderId}`;
};

const parseJsonResponse = async (response) => {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    const error = new Error(`Shopify Admin API returned non-JSON: ${text}`);
    error.status = response.status;
    throw error;
  }
};

export const setShopifyOrderDetailsMetafield = async ({
  shopDomain,
  orderId,
  payload,
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
  namespace = DEFAULT_ORDER_DETAILS_METAFIELD_NAMESPACE,
  key = DEFAULT_ORDER_DETAILS_METAFIELD_KEY,
} = {}) => {
  const normalizedShopDomain = normalizeShopDomain(shopDomain);
  if (!normalizedShopDomain) {
    return { ok: false, skipped: true, reason: "missing_shop_domain" };
  }

  const ownerId = toOrderGid(orderId);
  if (!ownerId) {
    return { ok: false, skipped: true, reason: "missing_order_id" };
  }

  const resolvedAccessToken = normalizeText(accessToken, 4096) || resolveAdminAccessToken();
  if (!resolvedAccessToken) {
    return { ok: false, skipped: true, reason: "missing_admin_access_token" };
  }

  const query = `
    mutation SetOrderDetailsMetafield($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          namespace
          key
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const variables = {
    metafields: [
      {
        ownerId,
        namespace,
        key,
        type: "json",
        value: JSON.stringify(payload || {}),
      },
    ],
  };

  const response = await fetch(
    `https://${normalizedShopDomain}/admin/api/${apiVersion}/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": resolvedAccessToken,
      },
      body: JSON.stringify({
        query,
        variables,
      }),
    }
  );

  const json = await parseJsonResponse(response);
  if (!response.ok) {
    const error = new Error(
      `Shopify Admin API request failed (${response.status})`
    );
    error.status = response.status;
    error.response = json;
    throw error;
  }

  const topLevelErrors = Array.isArray(json?.errors) ? json.errors : [];
  if (topLevelErrors.length) {
    const error = new Error(
      topLevelErrors.map((entry) => entry?.message).filter(Boolean).join("; ") ||
        "Shopify Admin API returned GraphQL errors"
    );
    error.status = 502;
    error.response = json;
    throw error;
  }

  const userErrors = Array.isArray(json?.data?.metafieldsSet?.userErrors)
    ? json.data.metafieldsSet.userErrors
    : [];
  if (userErrors.length) {
    const error = new Error(
      userErrors.map((entry) => entry?.message).filter(Boolean).join("; ") ||
        "Shopify Admin API returned metafield user errors"
    );
    error.status = 422;
    error.response = json;
    throw error;
  }

  const metafield = json?.data?.metafieldsSet?.metafields?.[0] || null;

  return {
    ok: true,
    skipped: false,
    ownerId,
    namespace,
    key,
    metafieldId: metafield?.id || null,
  };
};
