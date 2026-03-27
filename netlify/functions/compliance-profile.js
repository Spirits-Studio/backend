import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  CUSTOMER_CREATION_SOURCES,
  STUDIO_FIELDS,
  STUDIO_TABLES,
  getLinkedIds,
  getRecordOrNull,
  listAllRecords,
  mapErrorResponse,
  normalizeRecordId,
  normalizeShopifyCustomerId,
  parseBody,
  updateResilient,
} from "./_lib/studio.js";
import { upsertCanonicalCustomer } from "./_lib/shopifyWebhookStudio.js";
import {
  buildCustomerComplianceProfile,
  validateComplianceProfile,
} from "./_lib/compliance.js";
import {
  getShopifyCustomerComplianceMetafields,
  setShopifyCustomerComplianceMetafields,
} from "./_lib/shopifyAdmin.js";

const sendJson = (status, payload) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
  });

const pick = (source, ...keys) => {
  for (const key of keys) {
    const value = source?.[key];
    if (value !== undefined && value !== null && String(value).trim()) {
      return String(value).trim();
    }
  }
  return null;
};

const normalizeEmail = (value) => {
  const text = String(value || "").trim().toLowerCase();
  return text && text.includes("@") ? text : null;
};

const normalizePhone = (value) => {
  const text = String(value || "").trim();
  return text || null;
};

const hasStoredComplianceProfile = (profile) => {
  if (!profile || typeof profile !== "object") return false;
  if (profile.for_resale != null) return true;

  return [
    "premise_licence",
    "alcohol_licence",
    "licence_type",
    "personal_licence",
    "ten_licence",
    "is_business_purchase",
    "company_name",
    "trading_name",
    "company_number",
    "vat_number",
    "saved_at",
  ].some((key) => profile[key] != null);
};

const resolveIdentityInput = ({ qs = {}, body = {} } = {}) => {
  const payload = body && typeof body === "object" ? body : {};
  const source = { ...qs, ...payload };
  const authenticatedShopifyCustomerId = normalizeShopifyCustomerId(
    qs?.logged_in_customer_id
  );
  const authenticatedEmail = normalizeEmail(qs?.logged_in_customer_email);

  return {
    airtableId: pick(
      source,
      "airtable_id",
      "airtableId",
      "ss_customer_airtable_id"
    ),
    shopifyCustomerId:
      authenticatedShopifyCustomerId ||
      normalizeShopifyCustomerId(pick(source, "customer_id", "customerId")),
    email:
      authenticatedEmail ||
      normalizeEmail(pick(source, "email", "customer_email", "customerEmail")),
    firstName: pick(source, "first_name", "firstName"),
    lastName: pick(source, "last_name", "lastName"),
    phone: normalizePhone(
      pick(source, "phone", "customer_phone", "customerPhone")
    ),
    sessionId: pick(source, "session_id", "sessionId"),
    isAuthenticatedShopifyCustomer: Boolean(
      authenticatedShopifyCustomerId || authenticatedEmail
    ),
  };
};

const resolveCustomerProfileFromRecord = (record) => {
  const fields = record?.fields || {};
  const premiseLicence = fields[STUDIO_FIELDS.customers.premiseLicence] || null;
  const alcoholLicence = fields[STUDIO_FIELDS.customers.alcoholLicence] || null;
  const personalLicence = fields[STUDIO_FIELDS.customers.personalLicence] || null;
  const tenLicence = fields[STUDIO_FIELDS.customers.tenLicence] || null;
  const companyName = fields[STUDIO_FIELDS.customers.companyName] || null;
  const tradingName = fields[STUDIO_FIELDS.customers.tradingName] || null;
  const companyNumber = fields[STUDIO_FIELDS.customers.companyNumber] || null;
  const vatNumber = fields[STUDIO_FIELDS.customers.vatNumber] || null;

  if (
    !premiseLicence &&
    !alcoholLicence &&
    !personalLicence &&
    !tenLicence &&
    !companyName &&
    !tradingName &&
    !companyNumber &&
    !vatNumber
  ) {
    return null;
  }

  return validateComplianceProfile({
    for_resale: true,
    premise_licence: premiseLicence,
    alcohol_licence: alcoholLicence,
    licence_type: personalLicence ? "personal" : tenLicence ? "ten" : null,
    personal_licence: personalLicence,
    ten_licence: tenLicence,
    is_business_purchase: Boolean(
      companyName || tradingName || companyNumber || vatNumber
    ),
    company_name: companyName,
    trading_name: tradingName,
    company_number: companyNumber,
    vat_number: vatNumber,
  }).profile;
};

const resolveCustomerRecordIdFromSessionId = async (sessionId) => {
  const normalizedSessionId = String(sessionId || "").trim();
  if (!normalizedSessionId) return null;

  const rows = await listAllRecords(STUDIO_TABLES.savedConfigurations, {
    filterByFormula: `{${STUDIO_FIELDS.savedConfigurations.sessionId}}='${String(
      normalizedSessionId
    ).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`,
    maxRecords: 25,
  });

  for (const row of rows || []) {
    const customerIds = getLinkedIds(row, STUDIO_FIELDS.savedConfigurations.customer);
    if (customerIds[0]) return customerIds[0];
  }

  return null;
};

const buildCustomerFieldPatch = (profile) => ({
  [STUDIO_FIELDS.customers.premiseLicence]: profile.premise_licence || null,
  [STUDIO_FIELDS.customers.alcoholLicence]: profile.alcohol_licence || null,
  [STUDIO_FIELDS.customers.personalLicence]: profile.personal_licence || null,
  [STUDIO_FIELDS.customers.tenLicence]: profile.ten_licence || null,
  [STUDIO_FIELDS.customers.companyName]: profile.company_name || null,
  [STUDIO_FIELDS.customers.tradingName]: profile.trading_name || null,
  [STUDIO_FIELDS.customers.companyNumber]: profile.company_number || null,
  [STUDIO_FIELDS.customers.vatNumber]: profile.vat_number || null,
});

const loadCustomerRecord = async (recordId) => {
  const normalizedRecordId = normalizeRecordId(recordId);
  if (!normalizedRecordId) {
    return {
      customerRecordId: null,
      customerRecord: null,
    };
  }

  const customerRecord = await getRecordOrNull(STUDIO_TABLES.customers, normalizedRecordId);
  if (!customerRecord?.id) {
    return {
      customerRecordId: null,
      customerRecord: null,
    };
  }

  return {
    customerRecordId: customerRecord.id,
    customerRecord,
  };
};

const resolveExistingCustomerContext = async ({
  shop,
  airtableId,
  shopifyCustomerId,
  email,
  firstName,
  lastName,
  phone,
  sessionId,
  isAuthenticatedShopifyCustomer,
}) => {
  let customerRecord = null;
  let customerRecordId = null;
  let shopifyProfile = null;
  const explicitCustomerRecordId = normalizeRecordId(airtableId);
  const hasSignedInIdentity = Boolean(
    isAuthenticatedShopifyCustomer && (shopifyCustomerId || email || phone)
  );

  if (shopifyCustomerId) {
    try {
      shopifyProfile = await getShopifyCustomerComplianceMetafields({
        shopDomain: shop,
        customerId: shopifyCustomerId,
      });
    } catch (error) {
      console.warn("[compliance] Shopify customer metafield lookup failed", {
        shopifyCustomerId,
        message: error?.message || String(error),
      });
    }
  }

  if (shopifyProfile?.airtableId) {
    const resolved = await loadCustomerRecord(shopifyProfile.airtableId);
    customerRecordId = resolved.customerRecordId;
    customerRecord = resolved.customerRecord;
  }

  if (!customerRecordId && hasSignedInIdentity) {
    const existing = await upsertCanonicalCustomer({
      shopifyId: shopifyCustomerId,
      email,
      firstName,
      lastName,
      phone,
      shopDomain: shop,
      creationSource: CUSTOMER_CREATION_SOURCES.createAirtableCustomerLoggedIn,
      preferredCustomerRecordIds: [],
      allowCreate: false,
    });
    const resolved = await loadCustomerRecord(existing?.customerRecordId);
    customerRecordId = resolved.customerRecordId;
    customerRecord = resolved.customerRecord;
  }

  if (
    !customerRecordId &&
    explicitCustomerRecordId &&
    !isAuthenticatedShopifyCustomer
  ) {
    const resolved = await loadCustomerRecord(explicitCustomerRecordId);
    customerRecordId = resolved.customerRecordId;
    customerRecord = resolved.customerRecord;
  }

  if (!customerRecordId) {
    const sessionCustomerRecordId = await resolveCustomerRecordIdFromSessionId(sessionId);
    if (sessionCustomerRecordId) {
      const resolved = await loadCustomerRecord(sessionCustomerRecordId);
      customerRecordId = resolved.customerRecordId;
      customerRecord = resolved.customerRecord;
    }
  }

  return {
    customerRecordId: normalizeRecordId(customerRecord?.id || customerRecordId),
    customerRecord: customerRecord?.id ? customerRecord : null,
    shopifyProfile:
      shopifyProfile && shopifyProfile.ok && !shopifyProfile.skipped
        ? shopifyProfile
        : null,
  };
};

export default withShopifyProxy(
  async (arg, { qs, isV2, method, shop }) => {
    try {
      const body = (await parseBody(arg, method, isV2)) || {};
      const {
        airtableId,
        shopifyCustomerId,
        email,
        firstName,
        lastName,
        phone,
        sessionId,
        isAuthenticatedShopifyCustomer,
      } = resolveIdentityInput({ qs, body });

      if (method === "GET") {
        const context = await resolveExistingCustomerContext({
          shop,
          airtableId,
          shopifyCustomerId,
          email,
          firstName,
          lastName,
          phone,
          sessionId,
          isAuthenticatedShopifyCustomer,
        });

        const airtableProfile = resolveCustomerProfileFromRecord(context.customerRecord);
        const shopifyProfileCandidate =
          context.shopifyProfile?.complianceProfile &&
          typeof context.shopifyProfile.complianceProfile === "object"
            ? validateComplianceProfile(context.shopifyProfile.complianceProfile).profile
            : null;
        const shopifyProfile = hasStoredComplianceProfile(shopifyProfileCandidate)
          ? shopifyProfileCandidate
          : null;
        const profile = airtableProfile || shopifyProfile || null;

        return sendJson(200, {
          ok: true,
          airtableId: context.customerRecordId || null,
          shopifyCustomerId: shopifyCustomerId || null,
          hasProfile: hasStoredComplianceProfile(profile),
          profile,
          source: airtableProfile ? "airtable" : shopifyProfile ? "shopify" : null,
        });
      }

      const builtProfile = buildCustomerComplianceProfile(body);
      if (!builtProfile.ok) {
        return sendJson(422, {
          ok: false,
          error: "invalid_compliance_profile",
          errors: builtProfile.errors,
        });
      }

      const existingContext = await resolveExistingCustomerContext({
        shop,
        airtableId,
        shopifyCustomerId,
        email,
        firstName,
        lastName,
        phone,
        sessionId,
        isAuthenticatedShopifyCustomer,
      });
      let customerRecordId = existingContext.customerRecordId;
      let customerRecord = existingContext.customerRecord;

      if (!customerRecordId) {
        const sessionCustomerRecordId = await resolveCustomerRecordIdFromSessionId(sessionId);
        const createdOrMatched = await upsertCanonicalCustomer({
          shopifyId: shopifyCustomerId,
          email,
          firstName,
          lastName,
          phone,
          shopDomain: shop,
          creationSource: CUSTOMER_CREATION_SOURCES.createAirtableCustomerLoggedIn,
          preferredCustomerRecordIds: [sessionCustomerRecordId].filter(Boolean),
          allowCreate: true,
        });
        const resolved = await loadCustomerRecord(createdOrMatched?.customerRecordId);
        customerRecordId = resolved.customerRecordId;
        customerRecord = resolved.customerRecord;
      }

      if (!customerRecordId || !customerRecord?.id) {
        return sendJson(400, {
          ok: false,
          error: "missing_customer_identity",
          message:
            "Unable to resolve or create a customer record for compliance persistence.",
        });
      }

      customerRecord = await updateResilient(
        STUDIO_TABLES.customers,
        customerRecord.id,
        {},
        buildCustomerFieldPatch(builtProfile.profile)
      );

      let shopifySync = { ok: false, skipped: true, reason: "missing_shopify_customer_id" };
      if (shopifyCustomerId) {
        shopifySync = await setShopifyCustomerComplianceMetafields({
          shopDomain: shop,
          customerId: shopifyCustomerId,
          airtableId: customerRecord.id,
          profile: builtProfile.profile,
        });
      }

      return sendJson(200, {
        ok: true,
        airtableId: customerRecord.id,
        shopifyCustomerId: shopifyCustomerId || null,
        hasProfile: true,
        profile: builtProfile.profile,
        source: "airtable",
        shopifySync,
      });
    } catch (error) {
      return sendJson(error?.status || 500, mapErrorResponse(error));
    }
  },
  { methods: ["GET", "POST", "PATCH"] }
);
