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

// small helper to read from qs first (HMAC-signed), then body
const pick = (qs, body, ...keys) => {
  for (const k of keys) {
    const v = qs?.[k] ?? body?.[k];
    if (v !== undefined && v !== null && String(v).length) return String(v);
  }
  return null;
};

export default withShopifyProxy(
  async (arg, { qs, isV2, method, shop }) => {
    try {
      const body = await parseBody(arg, method, isV2) || {};

      // Prefer HMAC-signed query params from the App Proxy URL; fall back to body.
      const customerId = pick(qs, body, "customer_id", "customerId");
      const email = pick(qs, body, "email", "customer_email", "customerEmail");
      const firstName = pick(qs, body, "first_name", "firstName");
      const lastName = pick(qs, body, "last_name", "lastName");
      const phone = pick(qs, body, "phone", "customer_phone", "customerPhone");

      // Optional address info (usually from Liquid if you choose to pass it)
      const address1 = pick(qs, body, "address1");
      const address2 = pick(qs, body, "address2");
      const city = pick(qs, body, "city");
      const province = pick(qs, body, "province", "region");
      const zip = pick(qs, body, "zip", "postal_code", "postcode");
      const country = pick(qs, body, "country");

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
          // Keep fields in sync where new data provided
          const updates = {};
          if (email && record.fields.Email !== email) updates.Email = email;
          if (firstName && record.fields["First Name"] !== firstName) updates["First Name"] = firstName;
          if (lastName && record.fields["Last Name"] !== lastName) updates["Last Name"] = lastName;
          if (phone && record.fields["Phone"] !== phone) updates["Phone"] = phone;

          // Optional address sync (writes only what you pass)
          if (address1 && record.fields["Address 1"] !== address1) updates["Address 1"] = address1;
          if (address2 && record.fields["Address 2"] !== address2) updates["Address 2"] = address2;
          if (city && record.fields["City"] !== city) updates["City"] = city;
          if (province && record.fields["Province/State"] !== province) updates["Province/State"] = province;
          if (zip && record.fields["Postal Code"] !== zip) updates["Postal Code"] = zip;
          if (country && record.fields["Country"] !== country) updates["Country"] = country;

          if (Object.keys(updates).length) {
            // Use same table identifier across all calls for continuity
            record = await updateOne("Customers", record.id, updates);
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
          "Address 1": address1 || undefined,
          "Address 2": address2 || undefined,
          "City": city || undefined,
          "Province/State": province || undefined,
          "Postal Code": zip || undefined,
          "Country": country || undefined,
          "Shop Domain": shop,
          "Source": "Netlify Backend → create-airtable-customer (Logged-in)"
        });

        return send(200, {
          ok: true,
          created: true,
          matchedBy: customerId ? "shopify_id" : "email",
          airtableId: created.id,
          fields: created.fields
        });
      }

      // 2) No Shopify user — create a bare record
      const anon = await createOne("Customers", {
        "Shop Domain": shop,
        "Source": "Netlify Backend → create-airtable-customer (Anonymous)"
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
    methods: ["GET", "POST"],
    allowlist: [process.env.SHOPIFY_STORE_DOMAIN],
    requireShop: true
  }
);