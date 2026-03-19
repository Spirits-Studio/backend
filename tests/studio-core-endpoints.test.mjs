import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";

process.env.SHOPIFY_API_SECRET = "test-shopify-secret";
process.env.SHOPIFY_STORE_DOMAIN = "spiritsstudio.co.uk";
process.env.SHOPIFY_WEBHOOK_SECRET = "test-webhook-secret";
process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID = "Orders & Fulfilment";

const { default: studioSaveConfiguration } = await import(
  "../netlify/functions/studio-save-configuration.js"
);
const { default: studioList } = await import("../netlify/functions/studio-list.js");
const { default: studioConfiguration } = await import(
  "../netlify/functions/studio-configuration.js"
);
const { default: createAirtableCustomer } = await import(
  "../netlify/functions/create-airtable-customer.js"
);
const { CUSTOMER_CREATION_SOURCES, STUDIO_FIELDS, STUDIO_TABLES } = await import(
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
      if (recordId == null && method === "PATCH") {
        const bodyRecordId = call.body?.records?.[0]?.id || null;
        assert.equal(
          bodyRecordId,
          step.recordId,
          `Fetch #${index + 1} record id mismatch`
        );
      } else {
        assert.equal(recordId, step.recordId, `Fetch #${index + 1} record id mismatch`);
      }
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

test("studio-save-configuration persists links and session contract", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerA",
      response: {
        id: "recCustomerA",
        fields: {
          "Shopify ID": "12345",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontA",
      response: {
        id: "recVersionFrontA",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.sessionId]: "session-from-version",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelFrontA",
      response: {
        id: "recLabelFrontA",
        fields: {
          [STUDIO_FIELDS.labels.customers]: ["recCustomerA"],
          [STUDIO_FIELDS.labels.savedConfigurations]: [],
          [STUDIO_FIELDS.labels.sessionId]: "session-from-label",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelFrontA",
      response: {
        id: "recLabelFrontA",
        fields: {
          [STUDIO_FIELDS.labels.customers]: ["recCustomerA"],
          [STUDIO_FIELDS.labels.savedConfigurations]: [],
          [STUDIO_FIELDS.labels.sessionId]: "session-from-label",
        },
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.customer], [
          "recCustomerA",
        ]);
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.sessionId], "session-123");
        assert.deepEqual(
          fields[STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion],
          ["recVersionFrontA"]
        );
        assert.equal(
          fields[STUDIO_FIELDS.savedConfigurations.shopifyProductId],
          "987654321"
        );
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.closureSelection], "Ebony");
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.waxSelection], "Pink Rose");
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.labels], ["recLabelFrontA"]);
      },
      response: {
        records: [
          {
            id: "recSavedConfigurationA",
            createdTime: "2026-02-26T10:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.configurationId]: "CFG-TEST-1",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labels.savedConfigurations], [
          "recSavedConfigurationA",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.labels.currentFrontLabelVersion], [
          "recVersionFrontA",
        ]);
        assert.equal(fields[STUDIO_FIELDS.labels.sessionId], "session-123");
      },
      response: { records: [{ id: "recLabelFrontA" }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontA",
      response: {
        id: "recVersionFrontA",
        fields: {
          [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: [],
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recSavedConfigurationA",
        ]);
        assert.equal(fields[STUDIO_FIELDS.labelVersions.sessionId], "session-123");
      },
      response: { records: [{ id: "recVersionFrontA" }] },
    },
  ]);

  try {
    const response = await studioSaveConfiguration(
      createProxyEvent({
        method: "POST",
        body: {
          customer_record_id: "recCustomerA",
          session_id: "session-123",
          status: "Saved",
          preview_url: "https://cdn.example.com/preview.png",
          shopify_product_id: "987654321",
          shopify_variant_id: "123456789",
          internal_sku: "SKU-TEST-1",
          label_front_version_id: "recVersionFrontA",
          snapshot: {
            bottle: { name: "Antica" },
            liquid: { name: "London Dry Gin" },
            closure: { name: "Wax Sealed in Pink Rose" },
            closureExtras: {
              wood: { name: "Ebony" },
              wax: { name: "Wax Sealed in Pink Rose" },
            },
          },
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.saved_configuration_record_id, "recSavedConfigurationA");
    assert.equal(payload.session_id, "session-123");
    assert.equal(payload.label_front_record_id, "recLabelFrontA");
    assert.equal(payload.label_front_version_id, "recVersionFrontA");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("create-airtable-customer stores Shopify customer ids in numeric form", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Shopify ID}='9001')");
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Shopify ID}='gid://shopify/Customer/9001')");
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Email}='legacy@example.com')");
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "9001");
        assert.equal(fields.Email, "legacy@example.com");
        assert.equal(fields["First Name"], "Legacy");
        assert.equal(fields["Last Name"], "User");
        assert.equal(
          fields["Creation Source"],
          CUSTOMER_CREATION_SOURCES.createAirtableCustomerLoggedIn
        );
      },
      response: {
        records: [{ id: "recCreatedCustomer9001", fields: {} }],
      },
    },
  ]);

  try {
    const response = await createAirtableCustomer(
      createProxyEvent({
        method: "POST",
        query: {
          customer_id: "gid://shopify/Customer/9001",
          email: "legacy@example.com",
          first_name: "Legacy",
          last_name: "User",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.airtableId, "recCreatedCustomer9001");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("create-airtable-customer backfills legacy gid customer records to numeric IDs", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Shopify ID}='9002')");
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Shopify ID}='gid://shopify/Customer/9002')");
      },
      response: {
        records: [
          {
            id: "recLegacyCustomer9002",
            fields: {
              "Shopify ID": "gid://shopify/Customer/9002",
              Email: "legacy@example.com",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const id = call.body?.records?.[0]?.id;
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(id, "recLegacyCustomer9002");
        assert.equal(fields["Shopify ID"], "9002");
        assert.equal(fields.Source, "Shopify");
        assert.equal(fields["Shop Domain"], "spiritsstudio.co.uk");
      },
      response: {
        records: [{ id: "recLegacyCustomer9002", fields: {} }],
      },
    },
  ]);

  try {
    const response = await createAirtableCustomer(
      createProxyEvent({
        method: "POST",
        query: {
          customer_id: "gid://shopify/Customer/9002",
          email: "legacy@example.com",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.airtableId, "recLegacyCustomer9002");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("create-airtable-customer prioritizes Shopify id before Airtable id, email, then phone", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='9010')"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='gid://shopify/Customer/9010')"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recStaleCustomer9010",
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
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Email}='priority@example.com')"
        );
      },
      response: {
        records: [
          {
            id: "recRecoveredByEmail9010",
            fields: {
              Email: "priority@example.com",
              Source: "Guest",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recRecoveredByEmail9010",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "9010");
        assert.equal(fields.Phone, "+447700900001");
        assert.equal(fields["First Name"], "Priority");
        assert.equal(fields["Last Name"], "Customer");
        assert.equal(fields.Source, "Shopify");
      },
      response: {
        records: [{ id: "recRecoveredByEmail9010", fields: {} }],
      },
    },
  ]);

  try {
    const response = await createAirtableCustomer(
      createProxyEvent({
        method: "GET",
        query: {
          customer_id: "gid://shopify/Customer/9010",
          airtable_id: "recStaleCustomer9010",
          email: "priority@example.com",
          first_name: "Priority",
          last_name: "Customer",
          phone: "+447700900001",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.created, false);
    assert.equal(payload.matchedBy, "email");
    assert.equal(payload.airtableId, "recRecoveredByEmail9010");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("create-airtable-customer guest resolution uses Airtable id, email, then phone", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestStaleCustomer",
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
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Email}='guest-priority@example.com')"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Phone}='+447700900002')"
        );
      },
      response: {
        records: [
          {
            id: "recRecoveredGuestByPhone",
            fields: {
              Phone: "+447700900002",
              Source: "Guest",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recRecoveredGuestByPhone",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Email, "guest-priority@example.com");
        assert.equal(fields["First Name"], "Guest");
        assert.equal(fields["Last Name"], "Priority");
        assert.equal(fields.Source, "Shopify");
      },
      response: {
        records: [{ id: "recRecoveredGuestByPhone", fields: {} }],
      },
    },
  ]);

  try {
    const response = await createAirtableCustomer(
      createProxyEvent({
        method: "PATCH",
        body: {
          airtableId: "recGuestStaleCustomer",
          email: "guest-priority@example.com",
          first_name: "Guest",
          last_name: "Priority",
          phone: "+447700900002",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.created, false);
    assert.equal(payload.matchedBy, "phone");
    assert.equal(payload.airtableId, "recRecoveredGuestByPhone");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-save-configuration links all label versions and maps preview/alcohol fields", async () => {
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
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontCurrent",
      response: {
        id: "recVersionFrontCurrent",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.sessionId]: "session-current",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelFrontA",
      response: {
        id: "recLabelFrontA",
        fields: {
          [STUDIO_FIELDS.labels.customers]: ["recCustomerA"],
          [STUDIO_FIELDS.labels.savedConfigurations]: [],
          [STUDIO_FIELDS.labels.sessionId]: "session-from-label",
          [STUDIO_FIELDS.labels.labelVersions]: [
            "recVersionFrontPrev",
            "recVersionFrontCurrent",
          ],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelFrontA",
      response: {
        id: "recLabelFrontA",
        fields: {
          [STUDIO_FIELDS.labels.customers]: ["recCustomerA"],
          [STUDIO_FIELDS.labels.savedConfigurations]: [],
          [STUDIO_FIELDS.labels.sessionId]: "session-from-label",
          [STUDIO_FIELDS.labels.labelVersions]: [
            "recVersionFrontPrev",
            "recVersionFrontCurrent",
          ],
        },
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(
          fields[STUDIO_FIELDS.savedConfigurations.alcoholSelection],
          "Vodka"
        );
        assert.equal(
          fields[STUDIO_FIELDS.savedConfigurations.previewImageUrl],
          "https://files.example.com/preview.png"
        );
        assert.equal(
          fields[STUDIO_FIELDS.savedConfigurations.shopifyProductId],
          "24680"
        );
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.previewImage], [
          { url: "https://files.example.com/preview.png" },
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.labels], [
          "recLabelFrontA",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.labelVersions], [
          "recVersionFrontCurrent",
          "recVersionFrontPrev",
        ]);
      },
      response: {
        records: [
          {
            id: "recSavedConfigurationAllVersionsA",
            createdTime: "2026-03-01T10:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.configurationId]: "CFG-ALL-VERSIONS-1",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labels,
      response: { records: [{ id: "recLabelFrontA" }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontCurrent",
      response: {
        id: "recVersionFrontCurrent",
        fields: {
          [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: [],
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recSavedConfigurationAllVersionsA",
        ]);
      },
      response: { records: [{ id: "recVersionFrontCurrent" }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontPrev",
      response: {
        id: "recVersionFrontPrev",
        fields: {
          [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: [],
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recSavedConfigurationAllVersionsA",
        ]);
      },
      response: { records: [{ id: "recVersionFrontPrev" }] },
    },
  ]);

  try {
    const response = await studioSaveConfiguration(
      createProxyEvent({
        method: "POST",
        body: {
          customer_record_id: "recCustomerA",
          session_id: "session-save-order",
          status: "Saved",
          preview_url: "https://files.example.com/preview.png",
          label_front_version_id: "recVersionFrontCurrent",
          snapshot: {
            product_id: 24680,
            bottle: { name: "Antica" },
            liquid: { name: "Triple Distilled Vodka" },
            closure: { name: "Beech" },
          },
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(
      payload.saved_configuration_record_id,
      "recSavedConfigurationAllVersionsA"
    );
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-save-configuration resolves whisky from liquid mapping", async () => {
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
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(
          fields[STUDIO_FIELDS.savedConfigurations.alcoholSelection],
          "Whisky"
        );
      },
      response: {
        records: [
          {
            id: "recSavedConfigurationWhiskyA",
            createdTime: "2026-03-01T11:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.configurationId]: "CFG-WHISKY-1",
            },
          },
        ],
      },
    },
  ]);

  try {
    const response = await studioSaveConfiguration(
      createProxyEvent({
        method: "POST",
        body: {
          customer_record_id: "recCustomerA",
          session_id: "session-whisky-123",
          status: "Saved",
          snapshot: {
            bottle: { name: "Outlaw" },
            liquid: { name: "Bourbon" },
            closure: { name: "Ebony" },
          },
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(
      payload.saved_configuration_record_id,
      "recSavedConfigurationWhiskyA"
    );
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-save-configuration maps lite closure and default no-wax value", async () => {
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
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.closureSelection], "Ebony");
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.waxSelection], "No Wax Seal");
      },
      response: {
        records: [
          {
            id: "recSavedConfigurationLiteA",
            createdTime: "2026-02-27T10:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.configurationId]: "CFG-LITE-1",
            },
          },
        ],
      },
    },
  ]);

  try {
    const response = await studioSaveConfiguration(
      createProxyEvent({
        method: "POST",
        body: {
          customer_record_id: "recCustomerA",
          session_id: "session-lite-123",
          status: "Saved",
          preview_url: "https://cdn.example.com/lite-preview.png",
          shopify_variant_id: "999999",
          snapshot: {
            bottle: { name: "Outlaw" },
            liquid: { name: "Pink Gin" },
            closure: { name: "Ebony" },
          },
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.saved_configuration_record_id, "recSavedConfigurationLiteA");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-save-configuration auto-creates customer when stale id is provided without identity signals", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerLegacyA",
      status: 404,
      response: { error: { message: "NOT_FOUND" } },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], undefined);
        assert.equal(
          fields["Creation Source"],
          CUSTOMER_CREATION_SOURCES.studioSaveConfiguration
        );
      },
      response: {
        records: [{ id: "recCustomerRecreatedLegacyA", fields: {} }],
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.customer], [
          "recCustomerRecreatedLegacyA",
        ]);
      },
      response: {
        records: [{ id: "recSavedConfigurationRecoveredLegacyA", fields: {} }],
      },
    },
  ]);

  try {
    const response = await studioSaveConfiguration(
      createProxyEvent({
        method: "POST",
        body: {
          customer_record_id: "recCustomerLegacyA",
          status: "Saved",
          snapshot: {
            bottle: { name: "Outlaw" },
            liquid: { name: "Pink Gin" },
            closure: { name: "Ebony" },
          },
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.customer_record_id, "recCustomerRecreatedLegacyA");
    assert.equal(payload.customer_record_recovered, true);
    assert.equal(
      payload.saved_configuration_record_id,
      "recSavedConfigurationRecoveredLegacyA"
    );
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-save-configuration auto-creates customer for stale id when signed identity is present", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Shopify ID}='9001')");
      },
      response: {
        records: [],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Shopify ID}='gid://shopify/Customer/9001')");
      },
      response: {
        records: [],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerLegacyB",
      status: 404,
      response: { error: { message: "NOT_FOUND" } },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Email}='legacy@example.com')");
      },
      response: {
        records: [],
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "9001");
        assert.equal(fields.Email, "legacy@example.com");
        assert.equal(
          fields["Creation Source"],
          CUSTOMER_CREATION_SOURCES.studioSaveConfiguration
        );
      },
      response: {
        records: [{ id: "recCustomerRecreatedB", fields: {} }],
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.customer], [
          "recCustomerRecreatedB",
        ]);
      },
      response: {
        records: [{ id: "recSavedConfigurationRecoveredB", fields: {} }],
      },
    },
  ]);

  try {
    const response = await studioSaveConfiguration(
      createProxyEvent({
        method: "POST",
        query: {
          logged_in_customer_id: "gid://shopify/Customer/9001",
          logged_in_customer_email: "legacy@example.com",
        },
        body: {
          customer_record_id: "recCustomerLegacyB",
          status: "Saved",
          snapshot: {
            bottle: { name: "Outlaw" },
            liquid: { name: "Pink Gin" },
            closure: { name: "Ebony" },
          },
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.customer_record_id, "recCustomerRecreatedB");
    assert.equal(payload.customer_record_recovered, true);
    assert.equal(
      payload.saved_configuration_record_id,
      "recSavedConfigurationRecoveredB"
    );
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-list returns grouped configurations and label timeline with session fallback", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerA",
      response: {
        id: "recCustomerA",
        fields: {
          "Shopify ID": "9001",
          Source: "Shopify",
          Email: "customer@example.com",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: {
        records: [
          {
            id: "recSavedConfigurationOther",
            createdTime: "2026-02-19T10:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.customer]: ["recCustomerOther"],
              [STUDIO_FIELDS.savedConfigurations.configurationId]: "CFG-OTHER",
            },
          },
          {
            id: "recSavedConfigurationA",
            createdTime: "2026-02-20T10:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.customer]: ["recCustomerA"],
              [STUDIO_FIELDS.savedConfigurations.configurationId]: "CFG-001",
              [STUDIO_FIELDS.savedConfigurations.displayName]: "Holiday Batch",
              [STUDIO_FIELDS.savedConfigurations.status]: "Saved",
              [STUDIO_FIELDS.savedConfigurations.bottleSelection]: "Antica",
              [STUDIO_FIELDS.savedConfigurations.liquidSelection]: "London Dry Gin",
              [STUDIO_FIELDS.savedConfigurations.previewImageUrl]:
                "https://cdn.example.com/preview-a.png",
              [STUDIO_FIELDS.savedConfigurations.labels]: ["recLabelFrontA"],
              [STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion]: ["recVersionFrontA"],
              [STUDIO_FIELDS.savedConfigurations.configJson]:
                '{"hello":"world"}',
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: {
        records: [
          {
            id: "recLabelOther",
            createdTime: "2026-02-19T09:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.labels.customers]: ["recCustomerOther"],
              [STUDIO_FIELDS.labels.displayName]: "Other Label Thread",
            },
          },
          {
            id: "recLabelFrontA",
            createdTime: "2026-02-20T09:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.labels.customers]: ["recCustomerA"],
              [STUDIO_FIELDS.labels.displayName]: "Front Label Thread",
              [STUDIO_FIELDS.labels.sessionId]: "session-label-1",
              [STUDIO_FIELDS.labels.labelVersions]: ["recVersionFrontA"],
              [STUDIO_FIELDS.labels.currentFrontLabelVersion]: ["recVersionFrontA"],
              [STUDIO_FIELDS.labels.savedConfigurations]: ["recSavedConfigurationA"],
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: {
        records: [
          {
            id: "recVersionOther",
            createdTime: "2026-02-19T09:30:00.000Z",
            fields: {
              [STUDIO_FIELDS.labelVersions.labels]: ["recLabelOther"],
              [STUDIO_FIELDS.labelVersions.designSide]: "Front",
              [STUDIO_FIELDS.labelVersions.versionKind]: "Initial",
              [STUDIO_FIELDS.labelVersions.versionNumber]: 1,
            },
          },
          {
            id: "recVersionFrontA",
            createdTime: "2026-02-20T09:30:00.000Z",
            fields: {
              [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
              [STUDIO_FIELDS.labelVersions.designSide]: "Front",
              [STUDIO_FIELDS.labelVersions.versionKind]: "Initial",
              [STUDIO_FIELDS.labelVersions.versionNumber]: 1,
              [STUDIO_FIELDS.labelVersions.outputS3Url]:
                "https://cdn.example.com/front-v1.png",
            },
          },
        ],
      },
    },
  ]);

  try {
    const response = await studioList(
      createProxyEvent({
        method: "GET",
        query: {
          customer_record_id: "recCustomerA",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.customer_record_id, "recCustomerA");
    assert.equal(payload.saved_configurations.length, 1);
    assert.equal(payload.labels.length, 1);
    assert.equal(payload.counts.label_versions, 1);
    assert.equal(payload.saved_configurations[0].session_id, "session-label-1");
    assert.equal(payload.labels[0].versions[0].session_id, "session-label-1");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-list includes current label versions when direct label-version links are missing", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCustomerA",
      response: {
        id: "recCustomerA",
        fields: {
          "Shopify ID": "9001",
          Source: "Shopify",
          Email: "customer@example.com",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: {
        records: [
          {
            id: "recLabelFrontA",
            createdTime: "2026-02-20T09:00:00.000Z",
            fields: {
              [STUDIO_FIELDS.labels.customers]: ["recCustomerA"],
              [STUDIO_FIELDS.labels.displayName]: "Front Label Thread",
              [STUDIO_FIELDS.labels.sessionId]: "session-label-1",
              [STUDIO_FIELDS.labels.labelVersions]: [],
              [STUDIO_FIELDS.labels.currentFrontLabelVersion]: ["recVersionFrontA"],
              [STUDIO_FIELDS.labels.savedConfigurations]: [],
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      assert: (call) => {
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: {
        records: [
          {
            id: "recVersionOther",
            createdTime: "2026-02-20T09:10:00.000Z",
            fields: {
              [STUDIO_FIELDS.labelVersions.labels]: ["recLabelOther"],
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontA",
      response: {
        id: "recVersionFrontA",
        createdTime: "2026-02-20T09:30:00.000Z",
        fields: {
          "Labels: Current Front Label Versions": ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.versionKind]: "Initial",
          [STUDIO_FIELDS.labelVersions.versionNumber]: 1,
          [STUDIO_FIELDS.labelVersions.outputS3Url]:
            "https://cdn.example.com/front-v1.png",
        },
      },
    },
  ]);

  try {
    const response = await studioList(
      createProxyEvent({
        method: "GET",
        query: {
          customer_record_id: "recCustomerA",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.labels.length, 1);
    assert.equal(payload.labels[0].versions.length, 1);
    assert.equal(payload.labels[0].versions[0].id, "recVersionFrontA");
    assert.equal(payload.labels[0].versions[0].session_id, "session-label-1");
    assert.equal(payload.counts.label_versions, 1);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("studio-configuration returns bootstrap payload with version and session lineage", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigurationA",
      response: {
        id: "recSavedConfigurationA",
        createdTime: "2026-02-20T10:00:00.000Z",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCustomerA"],
          [STUDIO_FIELDS.savedConfigurations.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion]: ["recVersionFrontA"],
          [STUDIO_FIELDS.savedConfigurations.displayName]: "Holiday Batch",
          [STUDIO_FIELDS.savedConfigurations.bottleSelection]: "Antica",
          [STUDIO_FIELDS.savedConfigurations.liquidSelection]: "London Dry Gin",
          [STUDIO_FIELDS.savedConfigurations.closureSelection]: "Wood",
          [STUDIO_FIELDS.savedConfigurations.configJson]:
            '{"shopifyProductHandle":"build-your-own-gin","sessionId":"session-from-snapshot"}',
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelFrontA",
      response: {
        id: "recLabelFrontA",
        fields: {
          [STUDIO_FIELDS.labels.displayName]: "Front Label Thread",
          [STUDIO_FIELDS.labels.sessionId]: "session-label-1",
          [STUDIO_FIELDS.labels.currentFrontLabelVersion]: ["recVersionFrontA"],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontA",
      response: {
        id: "recVersionFrontA",
        fields: {
          [STUDIO_FIELDS.labelVersions.labels]: ["recLabelFrontA"],
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.versionKind]: "Edit",
          [STUDIO_FIELDS.labelVersions.versionNumber]: 2,
          [STUDIO_FIELDS.labelVersions.outputS3Url]:
            "https://cdn.example.com/front-v2.png",
        },
      },
    },
  ]);

  try {
    const response = await studioConfiguration(
      createProxyEvent({
        method: "GET",
        query: {
          customer_record_id: "recCustomerA",
          saved_configuration_record_id: "recSavedConfigurationA",
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.configuration.id, "recSavedConfigurationA");
    assert.equal(payload.configuration.session_id, "session-label-1");
    assert.equal(payload.configuration.label_front_version_id, "recVersionFrontA");
    assert.equal(payload.label_versions.front.id, "recVersionFrontA");
    assert.equal(payload.label_versions.front.session_id, "session-label-1");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
