import test from "node:test";
import assert from "node:assert/strict";

process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID = "Orders & Fulfilment";

const { default: createOrder } = await import(
  "../netlify/functions/create-order.js"
);
const { STUDIO_FIELDS, STUDIO_TABLES } = await import(
  "../netlify/functions/_lib/studio.js"
);

const parseMaybeJson = (raw) => {
  if (typeof raw !== "string" || !raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
};

const installFetchSequence = (steps) => {
  const originalFetch = global.fetch;
  let index = 0;

  global.fetch = async (input, init = {}) => {
    const method = String(init.method || "GET").toUpperCase();
    const inputUrl =
      typeof input === "string" ? input : input?.url || String(input || "");
    const url = new URL(inputUrl);
    const pathParts = url.pathname.split("/").filter(Boolean);
    const table = decodeURIComponent(pathParts[2] || "");
    const recordId = decodeURIComponent(pathParts[3] || "") || null;
    const call = {
      method,
      url,
      table,
      recordId,
      body: parseMaybeJson(typeof init.body === "string" ? init.body : ""),
    };

    const step = steps[index];
    assert.ok(step, `Unexpected fetch #${index + 1}: ${method} ${url.toString()}`);
    if (step.method) {
      assert.equal(method, step.method, `Fetch #${index + 1} method mismatch`);
    }
    if (Object.hasOwn(step, "table")) {
      assert.equal(table, step.table, `Fetch #${index + 1} table mismatch`);
    }
    if (Object.hasOwn(step, "recordId")) {
      assert.equal(recordId, step.recordId, `Fetch #${index + 1} record id mismatch`);
    }
    if (typeof step.assert === "function") {
      await step.assert(call);
    }
    index += 1;

    return new Response(JSON.stringify(step.response || {}), {
      status: step.status || 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  return {
    restore() {
      global.fetch = originalFetch;
    },
    assertDone() {
      assert.equal(index, steps.length, "Not all expected Airtable calls were made.");
    },
  };
};

const createReq = (body, method = "POST") => ({
  method,
  body,
});

const createRes = () => ({
  statusCode: null,
  payload: null,
  status(code) {
    this.statusCode = code;
    return this;
  },
  json(body) {
    this.payload = body;
    return this;
  },
});

test("create-order writes Orders & Fulfilment schema fields only", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerA",
      response: {
        id: "recCustomerA",
        fields: {},
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "shopify-order-1001");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], ["recCustomerA"]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedConfigurationA",
        ]);
        assert.equal(fields[STUDIO_FIELDS.orders.orderStatus], "Ordered");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.addresses], [
          "recAddressA",
          "recAddressB",
        ]);
        assert.equal(Object.hasOwn(fields, "orderId"), false);
        assert.equal(Object.hasOwn(fields, "userId"), false);
        assert.equal(Object.hasOwn(fields, "configId"), false);
      },
      response: {
        records: [{ id: "recOrderA", fields: {} }],
      },
    },
  ]);

  try {
    const req = createReq({
      userRecordId: "recCustomerA",
      configId: "recSavedConfigurationA",
      orderId: "shopify-order-1001",
      addressRecordIds: ["recAddressA", "recAddressB"],
    });
    const res = createRes();

    await createOrder(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.payload?.orderId, "shopify-order-1001");
    assert.equal(res.payload?.recordId, "recOrderA");
    assert.equal(res.payload?.customer_record_id, "recCustomerA");
    assert.equal(
      res.payload?.saved_configuration_record_id,
      "recSavedConfigurationA"
    );
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
