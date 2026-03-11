import test from "node:test";
import assert from "node:assert/strict";

process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";

const { resolveCustomerRecordIdOrCreate } = await import(
  "../netlify/functions/_lib/studio.js"
);

const installFetchSequence = (steps) => {
  const originalFetch = global.fetch;
  let index = 0;

  global.fetch = async (input, init = {}) => {
    const method = String(init.method || "GET").toUpperCase();
    const inputUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url || String(input || "");
    const url = new URL(inputUrl);
    const step = steps[index];
    assert.ok(step, `Unexpected fetch #${index + 1}: ${method} ${url.toString()}`);
    if (step.method) {
      assert.equal(method, step.method, `Fetch #${index + 1} method mismatch`);
    }
    if (typeof step.assert === "function") {
      await step.assert({ method, url, body: init.body });
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

test("resolveCustomerRecordIdOrCreate recovers when provided record lookup returns Airtable 403 invalid model", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers/recStaleCustomer");
      },
      status: 403,
      response: {
        error: {
          type: "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
          message:
            "Invalid permissions, or the requested model was not found.",
        },
      },
    },
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers");
        assert.equal(url.searchParams.get("maxRecords"), "1");
        assert.equal(
          url.searchParams.get("filterByFormula"),
          "({Shopify ID}='legacy_airtable_record:recStaleCustomer')"
        );
      },
      response: {
        records: [],
      },
    },
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers");
        assert.equal(url.searchParams.get("maxRecords"), "1");
        assert.equal(
          url.searchParams.get("filterByFormula"),
          "({Email}='user@example.com')"
        );
      },
      response: {
        records: [{ id: "recRecoveredByEmail", fields: { Email: "user@example.com" } }],
      },
    },
  ]);

  try {
    const result = await resolveCustomerRecordIdOrCreate({
      providedCustomerRecordId: "recStaleCustomer",
      body: { email: "user@example.com" },
      qs: {},
    });

    assert.equal(result.customerRecordId, "recRecoveredByEmail");
    assert.equal(result.created, false);
    assert.equal(result.recovered, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("resolveCustomerRecordIdOrCreate can create when customer searches are denied but writes are allowed", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers/recStaleCustomer");
      },
      status: 403,
      response: {
        error: {
          type: "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
          message:
            "Invalid permissions, or the requested model was not found.",
        },
      },
    },
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers");
        assert.equal(url.searchParams.get("maxRecords"), "1");
        assert.equal(
          url.searchParams.get("filterByFormula"),
          "({Shopify ID}='gid://shopify/Customer/123')"
        );
      },
      status: 403,
      response: {
        error: {
          type: "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
          message:
            "Invalid permissions, or the requested model was not found.",
        },
      },
    },
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers");
        assert.equal(url.searchParams.get("maxRecords"), "1");
        assert.equal(
          url.searchParams.get("filterByFormula"),
          "({Shopify ID}='legacy_airtable_record:recStaleCustomer')"
        );
      },
      status: 403,
      response: {
        error: {
          type: "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
          message:
            "Invalid permissions, or the requested model was not found.",
        },
      },
    },
    {
      method: "GET",
      assert: ({ url }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers");
        assert.equal(url.searchParams.get("maxRecords"), "1");
        assert.equal(
          url.searchParams.get("filterByFormula"),
          "({Email}='user@example.com')"
        );
      },
      status: 403,
      response: {
        error: {
          type: "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
          message:
            "Invalid permissions, or the requested model was not found.",
        },
      },
    },
    {
      method: "POST",
      assert: ({ url, body }) => {
        assert.equal(url.pathname, "/v0/appTestBase/Customers");
        const parsed = JSON.parse(String(body || "{}"));
        const fields = parsed?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "gid://shopify/Customer/123");
        assert.equal(fields.Email, "user@example.com");
      },
      response: {
        records: [{ id: "recCreatedCustomer", fields: {} }],
      },
    },
  ]);

  try {
    const result = await resolveCustomerRecordIdOrCreate({
      providedCustomerRecordId: "recStaleCustomer",
      body: {
        shopify_customer_id: "gid://shopify/Customer/123",
        email: "user@example.com",
      },
      qs: {},
    });

    assert.equal(result.customerRecordId, "recCreatedCustomer");
    assert.equal(result.created, true);
    assert.equal(result.recovered, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
