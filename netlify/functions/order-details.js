import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import {
  STUDIO_TABLES,
  sendJson,
  parseBody,
  firstNonEmpty,
  buildShopifyCustomerIdLookupValues,
  normalizeShopifyCustomerId,
  normalizeRecordId,
  mapErrorResponse,
} from "./_lib/studio.js";
import { fetchOrderDetailsPayload } from "./_lib/orderDetails.js";
import { findOneBy } from "../../src/lib/airtable.js";

const STAFF_EMAIL_SUFFIX = "@spiritsstudio.co.uk";

const normalizeStaffEmail = (value) => {
  const text = String(value || "").trim().toLowerCase();
  return text || null;
};

const isStaffEmail = (value) => {
  const email = normalizeStaffEmail(value);
  return Boolean(email && email.endsWith(STAFF_EMAIL_SUFFIX));
};

const resolveStaffAccess = async (qs = {}) => {
  const directEmail = normalizeStaffEmail(qs.logged_in_customer_email);
  if (isStaffEmail(directEmail)) {
    return {
      ok: true,
      method: "proxy_email",
      email: directEmail,
      shopifyCustomerId: normalizeShopifyCustomerId(qs.logged_in_customer_id),
    };
  }

  const shopifyCustomerId = normalizeShopifyCustomerId(qs.logged_in_customer_id);
  if (!shopifyCustomerId) {
    return {
      ok: false,
      method: "none",
      email: directEmail,
      shopifyCustomerId: null,
    };
  }

  for (const lookupValue of buildShopifyCustomerIdLookupValues(shopifyCustomerId)) {
    const customerRecord = await findOneBy(
      STUDIO_TABLES.customers,
      "Shopify ID",
      lookupValue
    );
    if (!customerRecord?.id) continue;

    const customerEmail = normalizeStaffEmail(customerRecord?.fields?.Email);
    return {
      ok: isStaffEmail(customerEmail),
      method: "airtable_customer",
      email: customerEmail,
      shopifyCustomerId,
      customerRecordId: customerRecord.id,
    };
  }

  return {
    ok: false,
    method: "airtable_customer_missing",
    email: directEmail,
    shopifyCustomerId,
    customerRecordId: null,
  };
};

export default withShopifyProxy(async (arg, { qs, isV2, method }) => {
  try {
    const body = (await parseBody(arg, method, isV2)) || {};
    const staffAccess = await resolveStaffAccess(qs);
    const orderId = firstNonEmpty(qs.order_id, qs.orderId, body.order_id, body.orderId);
    const savedConfigurationId = normalizeRecordId(
      firstNonEmpty(
        qs.saved_configuration_id,
        qs.savedConfigurationId,
        body.saved_configuration_id,
        body.savedConfigurationId
      )
    );

    if (!staffAccess.ok) {
      console.warn("[order-details] forbidden", {
        auth_method: staffAccess.method,
        shopify_customer_id: staffAccess.shopifyCustomerId || null,
        email_present: Boolean(staffAccess.email),
      });
      return sendJson(403, {
        ok: false,
        error: "forbidden",
        message: "Only signed-in Spirits Studio staff can view order details.",
      });
    }

    const payload = await fetchOrderDetailsPayload({
      orderId,
      savedConfigurationId,
    });

    return sendJson(200, payload);
  } catch (error) {
    return sendJson(Number(error?.status || 500), mapErrorResponse(error));
  }
});
