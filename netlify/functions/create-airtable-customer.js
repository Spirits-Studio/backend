import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { findOneBy, createOne, updateOne, getOne } from "../../src/lib/airtable.js";
import {
  CUSTOMER_CREATION_SOURCES,
  normalizeShopifyCustomerId,
  buildShopifyCustomerIdLookupValues,
  isAirtableLookupRecoverableError,
  parseAirtableErrorType,
} from "./_lib/studio.js";

const send = (status, obj) =>
  new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });

// Parse body for Netlify v2 (Request) and v1 (event)
const parseBody = async (arg, method, isV2) => {
  if (method === "GET") return null;
  if (isV2) {
    const ct = (arg.headers.get("content-type") || "").toLowerCase();
    if (ct.includes("application/json")) return await arg.json();
    if (ct.includes("application/x-www-form-urlencoded")) {
      const fd = await arg.formData();
      return Object.fromEntries([...fd.entries()]);
    }
    return await arg.text();
  } else {
    const ct = (arg.headers?.["content-type"] || "").toLowerCase();
    const raw = arg.body || "";
    if (ct.includes("application/json")) return raw ? JSON.parse(raw) : {};
    if (ct.includes("application/x-www-form-urlencoded")) {
      return Object.fromEntries(new URLSearchParams(raw));
    }
    return raw;
  }
};

// small helper to read from qs first (HMAC-signed), then body
const pick = (qs, body, ...keys) => {
  for (const k of keys) {
    const v = qs?.[k] ?? body?.[k];
    if (v !== undefined && v !== null && String(v).length) return String(v);
  }
  return null;
};

const logCustomerResolution = ({
  customerCreationReason,
  providedRecordId = null,
  shopifyIdPresent = false,
  emailPresent = false,
  phonePresent = false,
  recovered = false,
  created = false,
}) => {
  console.log("[customer] create-airtable-customer", {
    customer_creation_reason: customerCreationReason || "unknown",
    endpoint: "create-airtable-customer",
    shopify_id_present: Boolean(shopifyIdPresent),
    email_present: Boolean(emailPresent),
    phone_present: Boolean(phonePresent),
    provided_record_id: providedRecordId || null,
    recovered: Boolean(recovered),
    created: Boolean(created),
  });
};

const normalizeEmail = (value) => {
  const text = String(value || "").trim().toLowerCase();
  return text && text.includes("@") ? text : null;
};

const normalizePhone = (value) => {
  const text = String(value || "").trim();
  return text || null;
};

const findCustomerByShopifyId = async (customerId) => {
  if (!customerId) return null;
  for (const lookupValue of buildShopifyCustomerIdLookupValues(customerId)) {
    const record = await findOneBy(
      process.env.AIRTABLE_CUSTOMERS_TABLE_ID,
      "Shopify ID",
      lookupValue
    );
    if (record?.id) return { record, matchedBy: "shopify_id" };
  }
  return null;
};

const findCustomerByAirtableId = async (airtableId) => {
  if (!airtableId) return null;
  try {
    const record = await getOne(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, airtableId);
    return record?.id ? { record, matchedBy: "airtable_id" } : null;
  } catch (error) {
    if (isAirtableLookupRecoverableError(error)) {
      console.warn("[customer] create-airtable-customer lookup skipped", {
        fieldName: "Airtable Record ID",
        airtableId,
        status: error?.status,
        errorType: parseAirtableErrorType(error),
      });
      return null;
    }
    throw error;
  }
};

const findCustomerByEmail = async (email) => {
  if (!email) return null;
  const record = await findOneBy(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, "Email", email);
  return record?.id ? { record, matchedBy: "email" } : null;
};

const findCustomerByPhone = async (phone) => {
  if (!phone) return null;
  const record = await findOneBy(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, "Phone", phone);
  return record?.id ? { record, matchedBy: "phone" } : null;
};

const findCustomerByPriority = async ({
  customerId,
  airtableId,
  email,
  phone,
} = {}) => {
  if (customerId) {
    return (
      (await findCustomerByShopifyId(customerId)) ||
      (await findCustomerByAirtableId(airtableId)) ||
      (await findCustomerByEmail(email)) ||
      (await findCustomerByPhone(phone)) ||
      null
    );
  }

  return (
    (await findCustomerByAirtableId(airtableId)) ||
    (await findCustomerByEmail(email)) ||
    (await findCustomerByPhone(phone)) ||
    null
  );
};

const syncCustomerRecord = async ({
  record,
  customerId,
  email,
  firstName,
  lastName,
  phone,
  shop,
}) => {
  const updates = {};
  if (customerId && record.fields["Shopify ID"] !== customerId) {
    updates["Shopify ID"] = customerId;
  }
  if (email && record.fields.Email !== email) updates.Email = email;
  if (firstName && record.fields["First Name"] !== firstName) updates["First Name"] = firstName;
  if (lastName && record.fields["Last Name"] !== lastName) updates["Last Name"] = lastName;
  if (phone && record.fields["Phone"] !== phone) updates["Phone"] = phone;
  if (record.fields["Source"] !== "Shopify") updates["Source"] = "Shopify";
  if (shop && record.fields["Shop Domain"] !== shop) updates["Shop Domain"] = shop;

  if (!Object.keys(updates).length) return record;
  return updateOne(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, record.id, updates);
};

export default withShopifyProxy(
  async (arg, { qs, isV2, method, shop }) => {
    try {
      const body = await parseBody(arg, method, isV2) || {};

      // Prefer HMAC-signed query params from the App Proxy URL; fall back to body.
      const customerId = normalizeShopifyCustomerId(
        pick(qs, body, "customer_id", "customerId")
      );
      const email = pick(qs, body, "email", "customer_email", "customerEmail");
      const firstName = pick(qs, body, "first_name", "firstName");
      const lastName = pick(qs, body, "last_name", "lastName");
      const phone = normalizePhone(
        pick(qs, body, "phone", "customer_phone", "customerPhone")
      );
      const airtableId = pick(qs, body, "airtable_id", "airtableId");
      const shopifyIdPresent = Boolean(customerId);
      const emailNormalized = normalizeEmail(email);
      const emailPresent = Boolean(emailNormalized);
      const phonePresent = Boolean(phone);

      if (method === "PATCH") {
        if (!airtableId) {
          return send(400, {
            ok: false,
            error: "missing_airtable_id",
            message: "airtableId is required for PATCH updates.",
          });
        }

        const existingMatch = await findCustomerByPriority({
          customerId,
          airtableId,
          email: emailNormalized,
          phone,
        });
        if (!existingMatch?.record?.id) {
          if (!(customerId || emailNormalized || phone)) {
            return send(404, {
              ok: false,
              error: "not_found",
              message: "Customer record not found for provided airtableId.",
            });
          }

          const created = await createOne(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, {
            "Shopify ID": customerId || undefined,
            "Email": emailNormalized || undefined,
            "First Name": firstName || undefined,
            "Last Name": lastName || undefined,
            "Phone": phone || undefined,
            "Source": "Shopify",
            "Shop Domain": shop,
            "Creation Source":
              CUSTOMER_CREATION_SOURCES.createAirtableCustomerLoggedIn,
          });

          logCustomerResolution({
            customerCreationReason: "patch_create_new",
            providedRecordId: airtableId,
            shopifyIdPresent,
            emailPresent,
            phonePresent,
            recovered: Boolean(airtableId),
            created: true,
          });

          return send(200, {
            ok: true,
            created: true,
            matchedBy: "created",
            airtableId: created.id,
            fields: created.fields,
          });
        }

        const updated = await syncCustomerRecord({
          record: existingMatch.record,
          customerId,
          email: emailNormalized,
          firstName,
          lastName,
          phone,
          shop,
        });

        logCustomerResolution({
          customerCreationReason: `patch_${existingMatch.matchedBy}`,
          providedRecordId: existingMatch.record.id,
          shopifyIdPresent,
          emailPresent,
          phonePresent,
          recovered: Boolean(airtableId && existingMatch.record.id !== airtableId),
          created: false,
        });

        return send(200, {
          ok: true,
          created: false,
          matchedBy: existingMatch.matchedBy,
          airtableId: updated.id,
          fields: updated.fields,
        });
      }
      
      // 1) If we have a Shopify user (id or email): find or create
      if (customerId || airtableId || emailNormalized || phone) {
        const match = await findCustomerByPriority({
          customerId,
          airtableId,
          email: emailNormalized,
          phone,
        });

        if (match?.record?.id) {
          const record = await syncCustomerRecord({
            record: match.record,
            customerId,
            email: emailNormalized,
            firstName,
            lastName,
            phone,
            shop,
          });

          logCustomerResolution({
            customerCreationReason: `matched_by_${match.matchedBy}`,
            providedRecordId: record.id,
            shopifyIdPresent,
            emailPresent,
            phonePresent,
            recovered: false,
            created: false,
          });

          return send(200, {
            ok: true,
            created: false,
            matchedBy: match.matchedBy,
            airtableId: record.id,
            fields: record.fields
          });
        }

        // Not found — create
        const created = await createOne(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, {
          "Shopify ID": customerId || undefined,
          "Email": emailNormalized || undefined,
          "First Name": firstName || undefined,
          "Last Name": lastName || undefined,
          "Phone": phone || undefined,
          "Source": "Shopify",
          "Shop Domain": shop,
          "Creation Source":
            CUSTOMER_CREATION_SOURCES.createAirtableCustomerLoggedIn,
        });

        logCustomerResolution({
          customerCreationReason: "created_logged_in",
          providedRecordId: created.id,
          shopifyIdPresent,
          emailPresent,
          phonePresent,
          recovered: false,
          created: true,
        });

        return send(200, {
          ok: true,
          created: true,
          matchedBy: customerId ? "shopify_id" : emailNormalized ? "email" : "phone",
          airtableId: created.id,
          fields: created.fields
        });
      }

      // 2) No Shopify user — create a bare record
      const anon = await createOne(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, {
        "Source": "Shopify",
        "Shop Domain": shop,
        "Creation Source":
          CUSTOMER_CREATION_SOURCES.createAirtableCustomerGuest,
      });

      logCustomerResolution({
        customerCreationReason: "created_anonymous",
        providedRecordId: anon.id,
        shopifyIdPresent,
        emailPresent,
        phonePresent,
        recovered: false,
        created: true,
      });

      return send(200, {
        ok: true,
        created: true,
        matchedBy: "none",
        airtableId: anon.id,
        fields: anon.fields
      });
    } catch (err) {
      // Surface verbose errors from airtable.js (status, url, method, responseText)
      return send(err.status || 500, {
        ok: false,
        error: "server_error",
        message: err.message,
        status: err.status,
        url: err.url,
        method: err.method,
        responseText: err.responseText
      });
    }
  },
  {
    methods: ["GET", "POST", "PATCH"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);
