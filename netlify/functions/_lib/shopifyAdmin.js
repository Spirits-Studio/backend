const DEFAULT_ADMIN_API_VERSION =
  process.env.SHOPIFY_ADMIN_API_VERSION || "2025-07";
const DEFAULT_ORDER_DETAILS_METAFIELD_NAMESPACE =
  process.env.SHOPIFY_ORDER_DETAILS_METAFIELD_NAMESPACE || "ss";
const DEFAULT_ORDER_DETAILS_METAFIELD_KEY =
  process.env.SHOPIFY_ORDER_DETAILS_METAFIELD_KEY || "order_details";
const DEFAULT_ORDER_COMPLIANCE_METAFIELD_NAMESPACE =
  process.env.SHOPIFY_ORDER_COMPLIANCE_METAFIELD_NAMESPACE || "ss";
const DEFAULT_ORDER_COMPLIANCE_METAFIELD_KEY =
  process.env.SHOPIFY_ORDER_COMPLIANCE_METAFIELD_KEY || "compliance";
const DEFAULT_CUSTOMER_METAFIELD_NAMESPACE =
  process.env.SHOPIFY_CUSTOMER_METAFIELD_NAMESPACE || "SS";
const DEFAULT_CUSTOMER_AIRTABLE_ID_METAFIELD_KEY =
  process.env.SHOPIFY_CUSTOMER_AIRTABLE_ID_METAFIELD_KEY || "airtable_id";
const DEFAULT_CUSTOMER_COMPLIANCE_METAFIELD_KEY =
  process.env.SHOPIFY_CUSTOMER_COMPLIANCE_METAFIELD_KEY || "compliance_profile";

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

const normalizeBoolean = (value, fallback = false) => {
  if (typeof value === "boolean") return value;
  const text = normalizeText(value, 32);
  if (!text) return fallback;
  if (["1", "true", "yes", "on"].includes(text.toLowerCase())) return true;
  if (["0", "false", "no", "off"].includes(text.toLowerCase())) return false;
  return fallback;
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

const toCustomerGid = (customerId) => {
  const normalizedCustomerId = normalizeText(customerId, 255);
  if (!normalizedCustomerId) return null;
  if (normalizedCustomerId.startsWith("gid://shopify/Customer/")) {
    return normalizedCustomerId;
  }
  return `gid://shopify/Customer/${normalizedCustomerId}`;
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

export const shopifyAdminGraphql = async ({
  shopDomain,
  query,
  variables = {},
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
} = {}) => {
  const normalizedShopDomain = normalizeShopDomain(shopDomain);
  if (!normalizedShopDomain) {
    return { ok: false, skipped: true, reason: "missing_shop_domain" };
  }

  const normalizedQuery = normalizeText(query, 20000);
  if (!normalizedQuery) {
    return { ok: false, skipped: true, reason: "missing_query" };
  }

  const resolvedAccessToken = normalizeText(accessToken, 4096) || resolveAdminAccessToken();
  if (!resolvedAccessToken) {
    return { ok: false, skipped: true, reason: "missing_admin_access_token" };
  }

  const response = await fetch(
    `https://${normalizedShopDomain}/admin/api/${apiVersion}/graphql.json`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": resolvedAccessToken,
      },
      body: JSON.stringify({
        query: normalizedQuery,
        variables: variables || {},
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

  return {
    ok: true,
    skipped: false,
    data: json?.data || null,
    raw: json,
  };
};

export const listShopifyCarrierServices = async ({
  shopDomain,
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
} = {}) => {
  const query = `
    query CarrierServices($first: Int!) {
      carrierServices(first: $first) {
        nodes {
          id
          name
          callbackUrl
          active
          supportsServiceDiscovery
        }
      }
    }
  `;

  const result = await shopifyAdminGraphql({
    shopDomain,
    query,
    variables: { first: 50 },
    accessToken,
    apiVersion,
  });

  return {
    ok: true,
    services: Array.isArray(result?.data?.carrierServices?.nodes)
      ? result.data.carrierServices.nodes
      : [],
    raw: result.raw,
  };
};

export const upsertShopifyCarrierService = async ({
  shopDomain,
  name,
  callbackUrl,
  active = true,
  supportsServiceDiscovery = true,
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
} = {}) => {
  const normalizedShopDomain = normalizeShopDomain(shopDomain);
  const normalizedName = normalizeText(name, 255);
  const normalizedCallbackUrl = normalizeText(callbackUrl, 2048);

  if (!normalizedShopDomain) {
    return { ok: false, skipped: true, reason: "missing_shop_domain" };
  }

  if (!normalizedName) {
    return { ok: false, skipped: true, reason: "missing_name" };
  }

  if (!normalizedCallbackUrl) {
    return { ok: false, skipped: true, reason: "missing_callback_url" };
  }

  const listing = await listShopifyCarrierServices({
    shopDomain: normalizedShopDomain,
    accessToken,
    apiVersion,
  });
  if (!listing.ok) return listing;

  const existingService =
    listing.services.find((service) => service?.name === normalizedName) ||
    listing.services.find((service) => service?.callbackUrl === normalizedCallbackUrl) ||
    null;

  if (!existingService?.id) {
    const createMutation = `
      mutation CarrierServiceCreate($input: DeliveryCarrierServiceCreateInput!) {
        carrierServiceCreate(input: $input) {
          carrierService {
            id
            name
            callbackUrl
            active
            supportsServiceDiscovery
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const createResult = await shopifyAdminGraphql({
      shopDomain: normalizedShopDomain,
      query: createMutation,
      variables: {
        input: {
          name: normalizedName,
          callbackUrl: normalizedCallbackUrl,
          active: normalizeBoolean(active, true),
          supportsServiceDiscovery: normalizeBoolean(
            supportsServiceDiscovery,
            true
          ),
        },
      },
      accessToken,
      apiVersion,
    });

    const payload = createResult?.data?.carrierServiceCreate || {};
    const userErrors = Array.isArray(payload.userErrors) ? payload.userErrors : [];
    if (userErrors.length) {
      const error = new Error(
        userErrors.map((entry) => entry?.message).filter(Boolean).join("; ") ||
          "Shopify returned carrier service create user errors"
      );
      error.status = 422;
      error.response = createResult.raw;
      throw error;
    }

    return {
      ok: true,
      action: "created",
      carrierService: payload.carrierService || null,
      existingCarrierService: null,
      services: listing.services,
    };
  }

  const updateMutation = `
    mutation CarrierServiceUpdate($input: DeliveryCarrierServiceUpdateInput!) {
      carrierServiceUpdate(input: $input) {
        carrierService {
          id
          name
          callbackUrl
          active
          supportsServiceDiscovery
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const updateResult = await shopifyAdminGraphql({
    shopDomain: normalizedShopDomain,
    query: updateMutation,
    variables: {
      input: {
        id: existingService.id,
        name: normalizedName,
        callbackUrl: normalizedCallbackUrl,
        active: normalizeBoolean(active, true),
        supportsServiceDiscovery: normalizeBoolean(
          supportsServiceDiscovery,
          true
        ),
      },
    },
    accessToken,
    apiVersion,
  });

  const payload = updateResult?.data?.carrierServiceUpdate || {};
  const userErrors = Array.isArray(payload.userErrors) ? payload.userErrors : [];
  if (userErrors.length) {
    const error = new Error(
      userErrors.map((entry) => entry?.message).filter(Boolean).join("; ") ||
        "Shopify returned carrier service update user errors"
    );
    error.status = 422;
    error.response = updateResult.raw;
    throw error;
  }

  return {
    ok: true,
    action: "updated",
    carrierService: payload.carrierService || null,
    existingCarrierService: existingService,
    services: listing.services,
  };
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

  const result = await setShopifyMetafields({
    shopDomain: normalizedShopDomain,
    metafields: [
      {
        ownerId,
        namespace,
        key,
        type: "json",
        value: JSON.stringify(payload || {}),
      },
    ],
    accessToken,
    apiVersion,
  });

  return {
    ...result,
    ownerId,
    namespace,
    key,
    metafieldId: result?.metafields?.[0]?.id || null,
  };
};

export const setShopifyMetafields = async ({
  shopDomain,
  metafields = [],
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
} = {}) => {
  const normalizedShopDomain = normalizeShopDomain(shopDomain);
  if (!normalizedShopDomain) {
    return { ok: false, skipped: true, reason: "missing_shop_domain" };
  }

  const normalizedMetafields = Array.isArray(metafields)
    ? metafields.filter(
        (entry) =>
          entry?.ownerId &&
          entry?.namespace &&
          entry?.key &&
          entry?.type &&
          typeof entry?.value === "string"
      )
    : [];
  if (!normalizedMetafields.length) {
    return { ok: false, skipped: true, reason: "missing_metafields" };
  }

  const query = `
    mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          ownerType
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

  const result = await shopifyAdminGraphql({
    shopDomain: normalizedShopDomain,
    query,
    variables: { metafields: normalizedMetafields },
    accessToken,
    apiVersion,
  });
  if (!result?.ok) return result;

  const userErrors = Array.isArray(result?.data?.metafieldsSet?.userErrors)
    ? result.data.metafieldsSet.userErrors
    : [];
  if (userErrors.length) {
    const error = new Error(
      userErrors.map((entry) => entry?.message).filter(Boolean).join("; ") ||
        "Shopify Admin API returned metafield user errors"
    );
    error.status = 422;
    error.response = result.raw;
    throw error;
  }

  return {
    ok: true,
    skipped: false,
    metafields: Array.isArray(result?.data?.metafieldsSet?.metafields)
      ? result.data.metafieldsSet.metafields
      : [],
  };
};

export const setShopifyOrderComplianceMetafield = async ({
  shopDomain,
  orderId,
  payload,
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
  namespace = DEFAULT_ORDER_COMPLIANCE_METAFIELD_NAMESPACE,
  key = DEFAULT_ORDER_COMPLIANCE_METAFIELD_KEY,
} = {}) => {
  const ownerId = toOrderGid(orderId);
  if (!ownerId) {
    return { ok: false, skipped: true, reason: "missing_order_id" };
  }

  const result = await setShopifyMetafields({
    shopDomain,
    metafields: [
      {
        ownerId,
        namespace,
        key,
        type: "json",
        value: JSON.stringify(payload || {}),
      },
    ],
    accessToken,
    apiVersion,
  });

  return {
    ...result,
    ownerId,
    namespace,
    key,
    metafieldId: result?.metafields?.[0]?.id || null,
  };
};

export const getShopifyCustomerComplianceMetafields = async ({
  shopDomain,
  customerId,
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
  namespace = DEFAULT_CUSTOMER_METAFIELD_NAMESPACE,
  airtableIdKey = DEFAULT_CUSTOMER_AIRTABLE_ID_METAFIELD_KEY,
  complianceKey = DEFAULT_CUSTOMER_COMPLIANCE_METAFIELD_KEY,
} = {}) => {
  const normalizedShopDomain = normalizeShopDomain(shopDomain);
  if (!normalizedShopDomain) {
    return { ok: false, skipped: true, reason: "missing_shop_domain" };
  }

  const ownerId = toCustomerGid(customerId);
  if (!ownerId) {
    return { ok: false, skipped: true, reason: "missing_customer_id" };
  }

  const query = `
    query GetCustomerComplianceMetafields(
      $id: ID!
      $namespace: String!
      $airtableIdKey: String!
      $complianceKey: String!
    ) {
      customer(id: $id) {
        id
        airtableId: metafield(namespace: $namespace, key: $airtableIdKey) {
          value
        }
        complianceProfile: metafield(namespace: $namespace, key: $complianceKey) {
          value
        }
      }
    }
  `;

  const result = await shopifyAdminGraphql({
    shopDomain: normalizedShopDomain,
    query,
    variables: {
      id: ownerId,
      namespace,
      airtableIdKey,
      complianceKey,
    },
    accessToken,
    apiVersion,
  });
  if (!result?.ok) return result;

  const customer = result?.data?.customer || null;
  let complianceProfile = null;
  try {
    complianceProfile = customer?.complianceProfile?.value
      ? JSON.parse(customer.complianceProfile.value)
      : null;
  } catch {
    complianceProfile = null;
  }

  return {
    ok: true,
    skipped: false,
    ownerId,
    airtableId: normalizeText(customer?.airtableId?.value, 255),
    complianceProfile:
      complianceProfile && typeof complianceProfile === "object"
        ? complianceProfile
        : null,
  };
};

export const setShopifyCustomerComplianceMetafields = async ({
  shopDomain,
  customerId,
  airtableId = null,
  profile = null,
  accessToken = null,
  apiVersion = DEFAULT_ADMIN_API_VERSION,
  namespace = DEFAULT_CUSTOMER_METAFIELD_NAMESPACE,
  airtableIdKey = DEFAULT_CUSTOMER_AIRTABLE_ID_METAFIELD_KEY,
  complianceKey = DEFAULT_CUSTOMER_COMPLIANCE_METAFIELD_KEY,
} = {}) => {
  const ownerId = toCustomerGid(customerId);
  if (!ownerId) {
    return { ok: false, skipped: true, reason: "missing_customer_id" };
  }

  const metafields = [];
  const normalizedAirtableId = normalizeText(airtableId, 255);
  if (normalizedAirtableId) {
    metafields.push({
      ownerId,
      namespace,
      key: airtableIdKey,
      type: "single_line_text_field",
      value: normalizedAirtableId,
    });
  }

  if (profile && typeof profile === "object") {
    metafields.push({
      ownerId,
      namespace,
      key: complianceKey,
      type: "json",
      value: JSON.stringify(profile),
    });
  }

  if (!metafields.length) {
    return { ok: false, skipped: true, reason: "missing_metafields" };
  }

  return setShopifyMetafields({
    shopDomain,
    metafields,
    accessToken,
    apiVersion,
  });
};
