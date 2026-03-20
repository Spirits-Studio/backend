import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";

process.env.SHOPIFY_API_SECRET = "test-shopify-secret";
process.env.SHOPIFY_STORE_DOMAIN = "spiritsstudio.co.uk";
process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_ADDRESSES_TABLE_ID = "Addresses";
process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID = "Orders & Fulfilment";

const { default: orderDetails } = await import(
  "../netlify/functions/order-details.js"
);
const { STUDIO_FIELDS, STUDIO_TABLES } = await import(
  "../netlify/functions/_lib/studio.js"
);

const signProxyQuery = (query, secret = process.env.SHOPIFY_API_SECRET) => {
  const message = Object.keys(query)
    .sort()
    .map((key) => `${key}=${query[key] ?? ""}`)
    .join("");
  const signature = crypto
    .createHmac("sha256", secret)
    .update(message, "utf8")
    .digest("hex");
  return { ...query, signature };
};

const createProxyEvent = ({ method = "GET", query = {}, body = null } = {}) => {
  const signedQuery = signProxyQuery({
    shop: process.env.SHOPIFY_STORE_DOMAIN,
    timestamp: "1700000000",
    ...query,
  });

  return {
    httpMethod: method,
    headers: {
      origin: "https://spiritsstudio.co.uk",
      ...(body != null ? { "content-type": "application/json" } : {}),
    },
    queryStringParameters: signedQuery,
    body: body == null ? "" : JSON.stringify(body),
  };
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
    const call = { method, url, table, recordId };

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

test("order-details rejects non-staff customers", async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error("fetch should not be called for forbidden requests");
  };

  try {
    const response = await orderDetails(
      createProxyEvent({
        query: {
          order_id: "1001",
          logged_in_customer_email: "customer@example.com",
        },
      })
    );

    assert.equal(response.status, 403);
    const payload = await response.json();
    assert.equal(payload.ok, false);
    assert.equal(payload.error, "forbidden");
  } finally {
    global.fetch = originalFetch;
  }
});

test("order-details requires order_id or saved_configuration_id", async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error("fetch should not be called when lookup params are missing");
  };

  try {
    const response = await orderDetails(
      createProxyEvent({
        query: {
          logged_in_customer_email: "staff@spiritsstudio.co.uk",
        },
      })
    );

    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.equal(payload.ok, false);
    assert.equal(payload.error, "missing_order_id");
    assert.equal(
      payload.message,
      "order_id or saved_configuration_id is required."
    );
  } finally {
    global.fetch = originalFetch;
  }
});

test("order-details authorizes staff via logged_in_customer_id when proxy email is absent", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("maxRecords"), "1");
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='9001')"
        );
      },
      response: {
        records: [
          {
            id: "recStaffCustomerA",
            fields: {
              Email: "staff@spiritsstudio.co.uk",
            },
          },
        ],
      },
    },
  ]);

  try {
    const response = await orderDetails(
      createProxyEvent({
        query: {
          logged_in_customer_id: "gid://shopify/Customer/9001",
        },
      })
    );

    assert.equal(response.status, 400);
    const payload = await response.json();
    assert.equal(payload.ok, false);
    assert.equal(payload.error, "missing_order_id");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("order-details returns normalized order payload", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='1001'"
        );
        assert.equal(call.url.searchParams.get("maxRecords"), "25");
        assert.equal(call.url.searchParams.get("pageSize"), "100");
      },
      response: {
        records: [
          {
            id: "recOrderA",
            fields: {
              [STUDIO_FIELDS.orders.orderId]: "1001",
              [STUDIO_FIELDS.orders.customer]: ["recCustomerA"],
              [STUDIO_FIELDS.orders.addresses]: ["recAddressA"],
              [STUDIO_FIELDS.orders.shopifyProduct]: "Build Your Brand",
              [STUDIO_FIELDS.orders.shopifyProductId]: "123456789",
              [STUDIO_FIELDS.orders.shopifyVariantId]: "987654321",
              [STUDIO_FIELDS.orders.internalSku]: "SKU-1001",
              [STUDIO_FIELDS.orders.quantity]: 3,
              [STUDIO_FIELDS.orders.configJson]: JSON.stringify({
                preview_url: "https://cdn.example.com/preview.png",
                selectedLabelVersion: {
                  outputImageUrl: "https://cdn.example.com/front-label.png",
                },
              }),
              [STUDIO_FIELDS.orders.displayName]: "Marcus Rum",
              [STUDIO_FIELDS.orders.bottleSelection]: "Origin",
              [STUDIO_FIELDS.orders.liquidSelection]: "White Rum",
              [STUDIO_FIELDS.orders.closureSelection]: "Wooden Cork",
              [STUDIO_FIELDS.orders.waxSelection]: "Black",
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
          "Full Name": "Marcus Jones",
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
            "Marcus Jones, 1 Test Street, London, E1 1AA, United Kingdom",
        },
      },
    },
  ]);

  try {
    const response = await orderDetails(
      createProxyEvent({
        query: {
          order_id: "1001",
          logged_in_customer_email: "staff@spiritsstudio.co.uk",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.orderId, "1001");
    assert.equal(payload.savedConfigurationId, null);
    assert.deepEqual(payload.records, [
      {
        recordId: "recOrderA",
        orderId: "1001",
        customerName: "Marcus Jones",
        shippingAddress:
          "Marcus Jones, 1 Test Street, London, E1 1AA, United Kingdom",
        productName: "Build Your Brand",
        productId: "123456789",
        variantId: "987654321",
        sku: "SKU-1001",
        quantity: 3,
        previewImageUrl: "https://cdn.example.com/preview.png",
        displayName: "Marcus Rum",
        bottleSelection: "Origin",
        liquidSelection: "White Rum",
        closureSelection: "Wooden Cork",
        waxSelection: "Black",
        frontLabelUrl: "https://cdn.example.com/front-label.png",
      },
    ]);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("order-details supports saved_configuration_id lookup", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recSavedConfigA', ARRAYJOIN({Saved Configuration}))"
        );
        assert.equal(call.url.searchParams.get("maxRecords"), "25");
        assert.equal(call.url.searchParams.get("pageSize"), "100");
      },
      response: {
        records: [
          {
            id: "recOrderSavedConfig",
            fields: {
              [STUDIO_FIELDS.orders.orderId]: "2002",
              [STUDIO_FIELDS.orders.savedConfiguration]: ["recSavedConfigA"],
              [STUDIO_FIELDS.orders.customer]: ["recCustomerB"],
              [STUDIO_FIELDS.orders.addresses]: ["recAddressB"],
              [STUDIO_FIELDS.orders.shopifyProduct]: "Build Your Brand",
              [STUDIO_FIELDS.orders.displayName]: "Golden Label",
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerB",
      response: {
        id: "recCustomerB",
        fields: {
          "Full Name": "Staff User",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.addresses,
      recordId: "recAddressB",
      response: {
        id: "recAddressB",
        fields: {
          [STUDIO_FIELDS.addresses.fullAddress]:
            "1 Saved Config Way, London, EC1A 1AA, United Kingdom",
        },
      },
    },
  ]);

  try {
    const response = await orderDetails(
      createProxyEvent({
        query: {
          saved_configuration_id: "recSavedConfigA",
          logged_in_customer_email: "staff@spiritsstudio.co.uk",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.orderId, "2002");
    assert.equal(payload.savedConfigurationId, "recSavedConfigA");
    assert.equal(payload.records?.length, 1);
    assert.equal(payload.records?.[0]?.recordId, "recOrderSavedConfig");
    assert.equal(payload.records?.[0]?.displayName, "Golden Label");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
