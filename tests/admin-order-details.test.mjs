import test from "node:test";
import assert from "node:assert/strict";

process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_ADDRESSES_TABLE_ID = "Addresses";
process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID = "Orders & Fulfilment";

const { default: adminOrderDetails } = await import(
  "../netlify/functions/admin-order-details.js"
);
const { STUDIO_FIELDS, STUDIO_TABLES } = await import(
  "../netlify/functions/_lib/studio.js"
);

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
    const call = { method, url, table, recordId };

    const step = steps[index];
    assert.ok(step, `Unexpected fetch #${index + 1}: ${method} ${url.toString()}`);
    if (step.method) assert.equal(method, step.method);
    if (Object.hasOwn(step, "table")) assert.equal(table, step.table);
    if (Object.hasOwn(step, "recordId")) assert.equal(recordId, step.recordId);
    if (typeof step.assert === "function") await step.assert(call);
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

test("admin-order-details serves normalized order data with CORS", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='1001'"
        );
      },
      response: {
        records: [
          {
            id: "recOrderA",
            fields: {
              [STUDIO_FIELDS.orders.orderId]: "1001",
              [STUDIO_FIELDS.orders.customer]: ["recCustomerA"],
              [STUDIO_FIELDS.orders.addresses]: ["recAddressA"],
              [STUDIO_FIELDS.orders.shopifyProductId]: "10781413704026",
              [STUDIO_FIELDS.orders.shopifyProduct]: "Build Your Brand",
              [STUDIO_FIELDS.orders.quantity]: 2,
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerA",
      response: {
        id: "recCustomerA",
        fields: {
          "Full Name": "Staff User",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.addresses,
      recordId: "recAddressA",
      response: {
        id: "recAddressA",
        fields: {
          [STUDIO_FIELDS.addresses.fullAddress]:
            "1 Admin Way, London, EC1A 1AA, United Kingdom",
        },
      },
    },
  ]);

  try {
    const response = await adminOrderDetails(
      new Request(
        "https://spirits-studio-backend.netlify.app/.netlify/functions/admin-order-details?order_id=1001",
        {
          headers: {
            origin: "https://extensions.shopifycdn.com",
          },
        }
      )
    );

    assert.equal(response.status, 200);
    assert.equal(
      response.headers.get("access-control-allow-origin"),
      "https://extensions.shopifycdn.com"
    );

    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.orderId, "1001");
    assert.equal(payload.records?.[0]?.productId, "10781413704026");
    assert.equal(payload.records?.[0]?.quantity, 2);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("admin-order-details handles preflight requests", async () => {
  const response = await adminOrderDetails(
    new Request(
      "https://spirits-studio-backend.netlify.app/.netlify/functions/admin-order-details",
      {
        method: "OPTIONS",
        headers: {
          origin: "https://extensions.shopifycdn.com",
        },
      }
    )
  );

  assert.equal(response.status, 204);
  assert.equal(
    response.headers.get("access-control-allow-origin"),
    "https://extensions.shopifycdn.com"
  );
});
