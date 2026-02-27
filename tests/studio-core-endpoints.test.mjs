import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";

process.env.SHOPIFY_API_SECRET = "test-shopify-secret";
process.env.SHOPIFY_STORE_DOMAIN = "spiritsstudio.co.uk";
process.env.SHOPIFY_WEBHOOK_SECRET = "test-webhook-secret";
process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_ORDERS_TABLE_ID = "Orders & Fulfilment";

const { default: studioSaveConfiguration } = await import(
  "../netlify/functions/studio-save-configuration.js"
);
const { default: studioList } = await import("../netlify/functions/studio-list.js");
const { default: studioConfiguration } = await import(
  "../netlify/functions/studio-configuration.js"
);
const { default: shopifyWebhookOrdersCreate } = await import(
  "../netlify/functions/shopify-webhook-orders-create.js"
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

test("studio-save-configuration persists links and session contract", async () => {
  const fetchMock = installFetchSequence([
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

test("studio-save-configuration maps lite closure and default no-wax value", async () => {
  const fetchMock = installFetchSequence([
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

test("studio-list returns grouped configurations and label timeline with session fallback", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.match(formula, /recCustomerA/);
      },
      response: {
        records: [
          {
            id: "recSavedConfigurationA",
            createdTime: "2026-02-20T10:00:00.000Z",
            fields: {
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
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.match(formula, /recCustomerA/);
      },
      response: {
        records: [
          {
            id: "recLabelFrontA",
            createdTime: "2026-02-20T09:00:00.000Z",
            fields: {
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
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.match(formula, /recLabelFrontA/);
      },
      response: {
        records: [
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

const createWebhookRequest = (payload) => {
  const rawBody = JSON.stringify(payload);
  const hmac = crypto
    .createHmac("sha256", process.env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, "utf8")
    .digest("base64");
  return {
    headers: {
      "x-shopify-hmac-sha256": hmac,
    },
    async *[Symbol.asyncIterator]() {
      yield Buffer.from(rawBody, "utf8");
    },
  };
};

const createWebhookResponse = () => {
  return {
    statusCode: null,
    body: null,
    text: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
    end(text) {
      this.text = text;
      return this;
    },
  };
};

test("shopify-webhook-orders-create writes canonical Orders & Fulfilment mapping", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigurationA",
      response: {
        id: "recSavedConfigurationA",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCustomerA"],
        },
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Order ID"], "987654321");
        assert.deepEqual(fields.Customer, ["recCustomerA"]);
        assert.deepEqual(fields["Saved Configuration"], ["recSavedConfigurationA"]);
        assert.equal(fields["Order Status"], "Order Received");
      },
      response: {
        records: [{ id: "recOrderA" }],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        const row = call.body?.records?.[0] || {};
        assert.equal(row.id, "recSavedConfigurationA");
        const fields = row.fields || {};
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.status], "Ordered");
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.order], ["recOrderA"]);
      },
      response: {
        records: [{ id: "recSavedConfigurationA" }],
      },
    },
  ]);

  try {
    const req = createWebhookRequest({
      id: 987654321,
      financial_status: "paid",
      line_items: [
        {
          properties: [
            { name: "_saved_configuration_id", value: "recSavedConfigurationA" },
          ],
        },
      ],
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.deepEqual(res.body?.updated_saved_configurations, ["recSavedConfigurationA"]);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
