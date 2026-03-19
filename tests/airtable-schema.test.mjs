import test from "node:test";
import assert from "node:assert/strict";

const { default: airtableSchema } = await import(
  "../netlify/functions/airtable-schema.js"
);

const TABLE_ID_ENV_KEYS = [
  "AIRTABLE_ADDRESSES_TABLE_ID",
  "AIRTABLE_CUSTOMERS_TABLE_ID",
  "AIRTABLE_LABEL_VERSIONS_TABLE_ID",
  "AIRTABLE_LABELS_TABLE_ID",
  "AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID",
  "AIRTABLE_SAVED_CONFIGS_TABLE_ID",
];

const withEnv = async (overrides, fn) => {
  const keys = ["AIRTABLE_BASE_ID", "AIRTABLE_TOKEN", ...TABLE_ID_ENV_KEYS];
  const previous = new Map(keys.map((key) => [key, process.env[key]]));

  keys.forEach((key) => {
    const next = overrides[key];
    if (next == null) delete process.env[key];
    else process.env[key] = String(next);
  });

  try {
    await fn();
  } finally {
    keys.forEach((key) => {
      const prior = previous.get(key);
      if (prior == null) delete process.env[key];
      else process.env[key] = prior;
    });
  }
};

const mockFetchOnce = (assertFn, responseBody, status = 200) => {
  const originalFetch = global.fetch;
  let called = false;

  global.fetch = async (input, init = {}) => {
    called = true;
    const inputUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input?.url || String(input || "");
    await assertFn({
      url: new URL(inputUrl),
      method: String(init.method || "GET").toUpperCase(),
      headers: init.headers || {},
    });

    return new Response(JSON.stringify(responseBody), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  };

  return {
    restore() {
      global.fetch = originalFetch;
    },
    assertCalled() {
      assert.equal(called, true, "Expected fetch to be called.");
    },
  };
};

test("airtable-schema returns configured table schemas as id/name/schema", async () => {
  const fetchMock = mockFetchOnce(
    ({ url, method, headers }) => {
      assert.equal(method, "GET");
      assert.equal(url.pathname, "/v0/meta/bases/appBase123/tables");
      assert.equal(headers.Authorization, "Bearer patToken123");
    },
    {
      tables: [
        {
          id: "tblCustomers",
          name: "Customers",
          primaryFieldId: "fldCustomerName",
          fields: [{ id: "fldCustomerName", name: "Name", type: "singleLineText" }],
          views: [{ id: "viwCustomers", name: "Grid view", type: "grid" }],
        },
        {
          id: "tblLabels",
          name: "Labels",
          primaryFieldId: "fldLabelName",
          fields: [{ id: "fldLabelName", name: "Label Name", type: "singleLineText" }],
          views: [{ id: "viwLabels", name: "Grid view", type: "grid" }],
        },
      ],
    }
  );

  try {
    await withEnv(
      {
        AIRTABLE_BASE_ID: "appBase123",
        AIRTABLE_TOKEN: "patToken123",
        AIRTABLE_CUSTOMERS_TABLE_ID: "tblCustomers",
        AIRTABLE_LABELS_TABLE_ID: "tblLabels",
      },
      async () => {
        const response = await airtableSchema({ method: "GET" });
        assert.equal(response.status, 200);

        const payload = await response.json();
        assert.deepEqual(payload, {
          tables: [
            {
              id: "tblCustomers",
              name: "Customers",
              schema: {
                primaryFieldId: "fldCustomerName",
                fields: [{ id: "fldCustomerName", name: "Name", type: "singleLineText" }],
                views: [{ id: "viwCustomers", name: "Grid view", type: "grid" }],
              },
            },
            {
              id: "tblLabels",
              name: "Labels",
              schema: {
                primaryFieldId: "fldLabelName",
                fields: [{ id: "fldLabelName", name: "Label Name", type: "singleLineText" }],
                views: [{ id: "viwLabels", name: "Grid view", type: "grid" }],
              },
            },
          ],
        });
      }
    );

    fetchMock.assertCalled();
  } finally {
    fetchMock.restore();
  }
});

test("airtable-schema rejects non-GET methods", async () => {
  const response = await airtableSchema({ method: "POST" });
  assert.equal(response.status, 405);
  const payload = await response.json();
  assert.equal(payload.error, "method_not_allowed");
});

test("airtable-schema reports missing required env", async () => {
  await withEnv(
    {
      AIRTABLE_BASE_ID: "",
      AIRTABLE_TOKEN: "",
    },
    async () => {
      const response = await airtableSchema({ method: "GET" });
      assert.equal(response.status, 500);
      const payload = await response.json();
      assert.equal(payload.error, "missing_env");
      assert.deepEqual(payload.missing.sort(), ["AIRTABLE_BASE_ID", "AIRTABLE_TOKEN"]);
    }
  );
});
