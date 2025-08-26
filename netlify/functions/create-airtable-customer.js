import { withShopifyProxy } from "./_lib/shopifyProxy.js";
import { findOneBy, createOne, updateOne } from "../../src/lib/airtable.js";

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

export default withShopifyProxy(
  async (arg, { qs, isV2, method, shop }) => {
    try {
      const body = await parseBody(arg, method, isV2);

      // Prefer HMAC-signed query params from the App Proxy URL; fall back to body.
      const customerId =
        (qs.customer_id && String(qs.customer_id)) ||
        (body && (body.customer_id || body.customerId)) ||
        null;
      const email =
        (qs.email && String(qs.email)) ||
        (body && (body.email || body.customer_email || body.customerEmail)) ||
        null;

      // Optional extra fields you might send from theme/frontend
      const firstName =
        (qs.first_name && String(qs.first_name)) ||
        (body && (body.first_name || body.firstName)) ||
        null;
      const lastName =
        (qs.last_name && String(qs.last_name)) ||
        (body && (body.last_name || body.lastName)) ||
        null;

      // 1) If we have a Shopify user (id or email): find or create
      if (customerId || email) {
        let record = null;
        let matchedBy = null;

        if (customerId) {
          record = await findOneBy("Customers", "Shopify ID", customerId);
          matchedBy = record ? "shopify_id" : null;
        }
        if (!record && email) {
          record = await findOneBy("Customers", "Email", email);
          matchedBy = record ? "email" : matchedBy;
        }

        if (record) {
          // Optionally keep fields in sync
          const updates = {};
          if (email && record.fields.Email !== email) updates.Email = email;
          if (firstName && record.fields["First Name"] !== firstName)
            updates["First Name"] = firstName;
          if (lastName && record.fields["Last Name"] !== lastName)
            updates["Last Name"] = lastName;
          if (Object.keys(updates).length) {
            record = await updateOne(process.env.AIRTABLE_CUSTOMERS_TABLE_ID, record.id, updates);
          }

          return send(200, {
            ok: true,
            created: false,
            matchedBy,
            airtableId: record.id,
            fields: record.fields
          });
        }

        // Not found — create
        const created = await createOne("Customers", {
          "Shopify ID": customerId || undefined,
          "Email": email || undefined,
          "First Name": firstName || undefined,
          "Last Name": lastName || undefined,
          "Phone": phone || undefined,
          "Store Domain": shop,
          "Source": "Netlify Backend -> create-customer (Existing Shopify User)"
        });

        return send(200, {
          ok: true,
          created: true,
          matchedBy: customerId ? "shopify_id" : "email",
          airtableId: created.id,
          fields: created.fields
        });
      }

      const anon = await createOne("Customers", {
        "Shop Domain": shop,
        "Creation Source": "Netlify Backend -> create-customer (Anonymous Shopify User)"
      });

      return send(200, {
        ok: true,
        created: true,
        matchedBy: "none",
        airtableId: anon.id,
        fields: anon.fields
      });
    } catch (e) {
      return send(500, { ok: false, error: "server_error", message: String(e) });
    }
  },
  {
    methods: ["GET", "POST"], 
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);