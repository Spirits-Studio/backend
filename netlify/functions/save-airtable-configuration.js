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
      const body = await parseBody(arg, method, isV2) || {};

      console.log("body", body)
      
      const customer_id = body.customer_id || null;
      const shopify_variant_id = body.shopify_variant_id || null;
      const internal_sku = body.internal_sku || null;
      const liquor = body.liquor || null;
      const bottle = body.bottle || null;
      const liquid = body.liquid || null;
      const closure = body.closure || null;
      const wax = body.wax || null;
      const front_label = body.front_label || null;
      const back_label = body.back_label || null;
             
      let record = null;

      const created = await createOne(process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID, {
        "Customer": [customer_id] || undefined,
        "Configurator Tool": "Zakeke",
        "Alcohol Selection": liquor || undefined,
        "Bottle Selection": bottle  || undefined,
        "Liquid Selection": liquid  || undefined,
        "Closure Selection": closure || undefined,
        "Wax Selection": wax || undefined,
        "Label Design Tool": "VistaCreate",
        "Front Label Design ID": front_label.vista_create_id || undefined,
        "Front Label Zakeke ID": front_label.zakeke_id || undefined,
        "Front Label S3 Link": front_label.s3_link || undefined,
        "Back Label Design ID": back_label.vista_create_id || undefined,
        "Back Label Zakeke ID": back_label.zakeke_id || undefined,
        "Back Label S3 Link": back_label.s3_link || undefined,
        "Internal SKU": internal_sku || undefined,
        "Shopify Variant ID": shopify_variant_id || undefined,
        "Creation Source": "Shopify -> Netlify Backend (save-airtable-configuration)"
      });

      return send(200, {
        ok: true,
        created: true,
        airtableId: created.id,
        fields: created.fields
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