import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";

process.env.SHOPIFY_WEBHOOK_SECRET = "test-webhook-secret";
process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID = "Saved Configurations";
process.env.AIRTABLE_LABELS_TABLE_ID = "Labels";
process.env.AIRTABLE_LABEL_VERSIONS_TABLE_ID = "Label Versions";
process.env.AIRTABLE_ORDERS_TABLE_ID = "Orders & Fulfilment";
process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID = "Webhook Events";

const { default: shopifyWebhookOrdersCreate } = await import(
  "../netlify/functions/shopify-webhook-orders-create.js"
);
const { default: shopifyWebhookCustomersCreate } = await import(
  "../netlify/functions/shopify-webhook-customers-create.js"
);
const { default: shopifyWebhookCustomersUpdate } = await import(
  "../netlify/functions/shopify-webhook-customers-update.js"
);
const { mergeGuestCustomersIntoCanonical } = await import(
  "../netlify/functions/_lib/shopifyWebhookStudio.js"
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
      headers: init.headers || {},
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

const createWebhookRequest = ({
  payload,
  topic,
  webhookId,
  shopDomain = "wnbrmm-sg.myshopify.com",
}) => {
  const rawBody = JSON.stringify(payload || {});
  const hmac = crypto
    .createHmac("sha256", process.env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, "utf8")
    .digest("base64");

  return {
    headers: {
      "x-shopify-topic": topic,
      "x-shopify-shop-domain": shopDomain,
      "x-shopify-webhook-id": webhookId,
      "x-shopify-hmac-sha256": hmac,
    },
    async *[Symbol.asyncIterator]() {
      yield Buffer.from(rawBody, "utf8");
    },
  };
};

const createWebhookEvent = ({
  payload,
  topic,
  webhookId,
  shopDomain = "wnbrmm-sg.myshopify.com",
}) => {
  const rawBody = JSON.stringify(payload || {});
  const hmac = crypto
    .createHmac("sha256", process.env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, "utf8")
    .digest("base64");

  return {
    headers: {
      "x-shopify-topic": topic,
      "x-shopify-shop-domain": shopDomain,
      "x-shopify-webhook-id": webhookId,
      "x-shopify-hmac-sha256": hmac,
    },
    body: rawBody,
    isBase64Encoded: false,
  };
};

const createWebhookResponse = () => ({
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
});

const parseNetlifyResponseBody = (response) => {
  if (!response || typeof response.body !== "string" || !response.body) return null;
  return JSON.parse(response.body);
};

test("duplicate webhook delivery is idempotently skipped", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_dup_1')");
      },
      response: {
        records: [{ id: "recWebhookEventDup", fields: { Status: "processed" } }],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventDup",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventDup", fields: {} }],
      },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_dup_1",
      payload: {
        id: 10001,
        line_items: [],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("orders webhook handles Netlify event body without stream reader", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_event_orders_1')");
      },
      response: {
        records: [{ id: "recWebhookEventOrders", fields: { Status: "processed" } }],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventOrders",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventOrders", fields: {} }],
      },
    },
  ]);

  try {
    const event = createWebhookEvent({
      topic: "orders/create",
      webhookId: "wh_event_orders_1",
      payload: {
        id: 10002,
        line_items: [],
      },
    });

    const response = await shopifyWebhookOrdersCreate(event);
    const body = parseNetlifyResponseBody(response);

    assert.equal(response?.statusCode, 200);
    assert.equal(body?.ok, true);
    assert.equal(body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customers/create webhook handles Netlify event body without stream reader", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_event_customers_create_1')");
      },
      response: {
        records: [{ id: "recWebhookEventCustomersCreate", fields: { Status: "processed" } }],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventCustomersCreate",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventCustomersCreate", fields: {} }],
      },
    },
  ]);

  try {
    const event = createWebhookEvent({
      topic: "customers/create",
      webhookId: "wh_event_customers_create_1",
      payload: {
        id: 501,
        email: "customer-create@example.com",
      },
    });

    const response = await shopifyWebhookCustomersCreate(event);
    const body = parseNetlifyResponseBody(response);

    assert.equal(response?.statusCode, 200);
    assert.equal(body?.ok, true);
    assert.equal(body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customers/update webhook handles Netlify event body without stream reader", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_event_customers_update_1')");
      },
      response: {
        records: [{ id: "recWebhookEventCustomersUpdate", fields: { Status: "processed" } }],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventCustomersUpdate",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventCustomersUpdate", fields: {} }],
      },
    },
  ]);

  try {
    const event = createWebhookEvent({
      topic: "customers/update",
      webhookId: "wh_event_customers_update_1",
      payload: {
        id: 502,
        email: "customer-update@example.com",
      },
    });

    const response = await shopifyWebhookCustomersUpdate(event);
    const body = parseNetlifyResponseBody(response);

    assert.equal(response?.statusCode, 200);
    assert.equal(body?.ok, true);
    assert.equal(body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("idempotency retries previously failed webhook ids instead of skipping", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_retry_error_1')");
      },
      response: {
        records: [{ id: "recWebhookRetry1", fields: { Status: "error" } }],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookRetry1",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "processed");
      },
      response: {
        records: [{ id: "recWebhookRetry1", fields: { Status: "processed" } }],
      },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_retry_error_1",
      payload: {
        id: 10002,
        line_items: [],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.idempotent_skip, false);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("webhook processing fails closed when idempotency storage is unavailable", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      status: 403,
      response: {
        error: {
          type: "INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND",
          message: "Invalid permissions, or the requested model was not found.",
        },
      },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_idempotency_down_1",
      payload: {
        id: 10003,
        line_items: [],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 503);
    assert.equal(res.body?.ok, false);
    assert.equal(res.body?.error, "idempotency_unavailable");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("guest order with _saved_configuration_id merges and enforces saved/label-version linkage", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_saved_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Webhook ID"], "wh_order_saved_1");
        assert.equal(fields.Topic, "orders/create");
        assert.equal(fields.Status, "received");
      },
      response: { records: [{ id: "recWebhookEventSaved1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='9001')"
        );
      },
      response: {
        records: [
          {
            id: "recCanonicalCustomer",
            fields: {
              "Shopify ID": "9001",
              Email: "guest@example.com",
              "First Name": "Guest",
              "Last Name": "Buyer",
              Source: "Shopify",
              "Shop Domain": "wnbrmm-sg.myshopify.com",
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigA",
      response: {
        id: "recSavedConfigA",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestCustomer"],
          [STUDIO_FIELDS.savedConfigurations.labels]: ["recLabelA"],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(
          formula,
          "AND(LOWER({Email})='guest@example.com', OR({Shopify ID}=BLANK(), {Shopify ID}=''))"
        );
      },
      response: {
        records: [{ id: "recGuestCustomer", fields: { Email: "guest@example.com" } }],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestCustomer', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recSavedConfigA",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestCustomer"],
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigA",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.customer], [
          "recCanonicalCustomer",
        ]);
      },
      response: { records: [{ id: "recSavedConfigA", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestCustomer', ARRAYJOIN({Customers}))"
        );
      },
      response: {
        records: [
          {
            id: "recLabelA",
            fields: { [STUDIO_FIELDS.labels.customers]: ["recGuestCustomer"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelA",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labels.customers], [
          "recCanonicalCustomer",
        ]);
      },
      response: { records: [{ id: "recLabelA", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestCustomer', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recOrderOldGuest",
            fields: { [STUDIO_FIELDS.orders.customer]: ["recGuestCustomer"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.orders,
      recordId: "recOrderOldGuest",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], ["recCanonicalCustomer"]);
      },
      response: { records: [{ id: "recOrderOldGuest", fields: {} }] },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestCustomer",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Merge Status"], "Merged");
        assert.deepEqual(fields["Merged Into Customer"], ["recCanonicalCustomer"]);
      },
      response: {
        records: [
          {
            id: "recGuestCustomer",
            fields: {
              "Merge Status": "Merged",
              "Merged Into Customer": ["recCanonicalCustomer"],
              "Merged At": "2026-03-12T00:00:00.000Z",
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigA",
      response: {
        id: "recSavedConfigA",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalCustomer"],
          [STUDIO_FIELDS.savedConfigurations.labels]: ["recLabelA"],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion]: [],
        },
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "123456789");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], ["recCanonicalCustomer"]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedConfigA",
        ]);
        assert.equal(fields[STUDIO_FIELDS.orders.orderStatus], "Order Received");
      },
      response: { records: [{ id: "recOrderNew1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigA",
      response: {
        id: "recSavedConfigA",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalCustomer"],
          [STUDIO_FIELDS.savedConfigurations.labels]: ["recLabelA"],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelA",
      response: {
        id: "recLabelA",
        fields: {
          [STUDIO_FIELDS.labels.labelVersions]: [
            "recVersionFrontCurrent",
            "recVersionFrontOld",
          ],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigA",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.status], "Order Received");
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.order], ["recOrderNew1"]);
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion], [
          "recVersionFrontCurrent",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.labelVersions], [
          "recVersionFrontCurrent",
          "recVersionFrontOld",
        ]);
      },
      response: { records: [{ id: "recSavedConfigA", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontCurrent",
      response: {
        id: "recVersionFrontCurrent",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontCurrent",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recSavedConfigA",
        ]);
      },
      response: { records: [{ id: "recVersionFrontCurrent", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontOld",
      response: {
        id: "recVersionFrontOld",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: ["recOtherConfig"],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recVersionFrontOld",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recOtherConfig",
          "recSavedConfigA",
        ]);
      },
      response: { records: [{ id: "recVersionFrontOld", fields: {} }] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventSaved1",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "processed");
      },
      response: { records: [{ id: "recWebhookEventSaved1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_order_saved_1",
      payload: {
        id: 123456789,
        email: "guest@example.com",
        customer: {
          id: 9001,
          email: "guest@example.com",
          first_name: "Guest",
          last_name: "Buyer",
        },
        line_items: [
          {
            id: 1,
            properties: [
              { name: "_saved_configuration_id", value: "recSavedConfigA" },
              { name: "_label_front_version_id", value: "recVersionFrontCurrent" },
              { name: "_ss_customer_airtable_id", value: "recGuestCustomer" },
            ],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, "recCanonicalCustomer");
    assert.equal(res.body?.merge_candidates_count, 1);
    assert.deepEqual(res.body?.merged_customer_pairs, [
      "recGuestCustomer->recCanonicalCustomer",
    ]);
    assert.deepEqual(res.body?.updated_saved_configurations, ["recSavedConfigA"]);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("guest order with _session_id only resolves saved configuration and links order", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_session_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookEventSession1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Email}='guest-session@example.com')"
        );
      },
      response: {
        records: [
          {
            id: "recCanonicalByEmail",
            fields: {
              Email: "guest-session@example.com",
              Source: "Shopify",
              "Shop Domain": "wnbrmm-sg.myshopify.com",
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Session ID}='session-only-1'"
        );
      },
      response: {
        records: [
          {
            id: "recSavedBySession",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestSession"],
              [STUDIO_FIELDS.savedConfigurations.labels]: [],
              [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "AND(LOWER({Email})='guest-session@example.com', OR({Shopify ID}=BLANK(), {Shopify ID}=''))"
        );
      },
      response: {
        records: [{ id: "recGuestSession", fields: { Email: "guest-session@example.com" } }],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestSession', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recSavedBySession",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestSession"],
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedBySession",
      response: { records: [{ id: "recSavedBySession", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestSession', ARRAYJOIN({Customers}))"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestSession', ARRAYJOIN({Customer}))"
        );
      },
      response: { records: [] },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestSession",
      response: {
        records: [
          {
            id: "recGuestSession",
            fields: {
              "Merge Status": "Merged",
              "Merged Into Customer": ["recCanonicalByEmail"],
              "Merged At": "2026-03-12T00:00:00.000Z",
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedBySession",
      response: {
        id: "recSavedBySession",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalByEmail"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "987001");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], ["recCanonicalByEmail"]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedBySession",
        ]);
      },
      response: { records: [{ id: "recOrderSessionOnly", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedBySession",
      response: {
        id: "recSavedBySession",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalByEmail"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedBySession",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.status], "Order Received");
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.order], [
          "recOrderSessionOnly",
        ]);
      },
      response: { records: [{ id: "recSavedBySession", fields: {} }] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventSession1",
      response: { records: [{ id: "recWebhookEventSession1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_order_session_1",
      payload: {
        id: 987001,
        email: "guest-session@example.com",
        line_items: [
          {
            id: 99,
            properties: [{ name: "_session_id", value: "session-only-1" }],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.deepEqual(res.body?.updated_saved_configurations, ["recSavedBySession"]);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customer created after guest order resolves by email and backfills Shopify id", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_customer_create_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookCustomerCreate1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='7001')"
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
          "({Email}='guest-created@example.com')"
        );
      },
      response: {
        records: [{ id: "recGuestFromOldOrder", fields: { Email: "guest-created@example.com" } }],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestFromOldOrder",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "7001");
        assert.equal(fields.Source, "Shopify");
      },
      response: { records: [{ id: "recGuestFromOldOrder", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "AND(LOWER({Email})='guest-created@example.com', OR({Shopify ID}=BLANK(), {Shopify ID}=''))"
        );
      },
      response: { records: [] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookCustomerCreate1",
      response: { records: [{ id: "recWebhookCustomerCreate1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "customers/create",
      webhookId: "wh_customer_create_1",
      payload: {
        id: 7001,
        email: "guest-created@example.com",
        first_name: "Guest",
        last_name: "Created",
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookCustomersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, "recGuestFromOldOrder");
    assert.equal(res.body?.created_customer, false);
    assert.equal(res.body?.matched_by, "email");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customer email changes are synced on customers/update", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_customer_update_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookCustomerUpdate1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='7002')"
        );
      },
      response: {
        records: [
          {
            id: "recCanonicalExisting",
            fields: {
              "Shopify ID": "7002",
              Email: "old-email@example.com",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recCanonicalExisting",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Email, "new-email@example.com");
      },
      response: { records: [{ id: "recCanonicalExisting", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "AND(LOWER({Email})='new-email@example.com', OR({Shopify ID}=BLANK(), {Shopify ID}=''))"
        );
      },
      response: { records: [] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookCustomerUpdate1",
      response: { records: [{ id: "recWebhookCustomerUpdate1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "customers/update",
      webhookId: "wh_customer_update_1",
      payload: {
        id: 7002,
        email: "new-email@example.com",
        first_name: "Changed",
        last_name: "Email",
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookCustomersUpdate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, "recCanonicalExisting");
    assert.equal(res.body?.created_customer, false);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("canonical merge handles multiple guest records", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuest1', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recSavedGuest1",
            fields: { [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuest1"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedGuest1",
      response: { records: [{ id: "recSavedGuest1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuest1', ARRAYJOIN({Customers}))"
        );
      },
      response: {
        records: [
          {
            id: "recLabelGuest1",
            fields: { [STUDIO_FIELDS.labels.customers]: ["recGuest1"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelGuest1",
      response: { records: [{ id: "recLabelGuest1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuest1', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recOrderGuest1",
            fields: { [STUDIO_FIELDS.orders.customer]: ["recGuest1"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.orders,
      recordId: "recOrderGuest1",
      response: { records: [{ id: "recOrderGuest1", fields: {} }] },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuest1",
      response: {
        records: [
          {
            id: "recGuest1",
            fields: {
              "Merge Status": "Merged",
              "Merged Into Customer": ["recCanonicalMain"],
              "Merged At": "2026-03-12T00:00:00.000Z",
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuest2', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recSavedGuest2",
            fields: { [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuest2"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedGuest2",
      response: { records: [{ id: "recSavedGuest2", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuest2', ARRAYJOIN({Customers}))"
        );
      },
      response: {
        records: [
          {
            id: "recLabelGuest2",
            fields: { [STUDIO_FIELDS.labels.customers]: ["recGuest2"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labels,
      recordId: "recLabelGuest2",
      response: { records: [{ id: "recLabelGuest2", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuest2', ARRAYJOIN({Customer}))"
        );
      },
      response: {
        records: [
          {
            id: "recOrderGuest2",
            fields: { [STUDIO_FIELDS.orders.customer]: ["recGuest2"] },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.orders,
      recordId: "recOrderGuest2",
      response: { records: [{ id: "recOrderGuest2", fields: {} }] },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuest2",
      response: {
        records: [
          {
            id: "recGuest2",
            fields: {
              "Merge Status": "Merged",
              "Merged Into Customer": ["recCanonicalMain"],
              "Merged At": "2026-03-12T00:00:00.000Z",
            },
          },
        ],
      },
    },
  ]);

  try {
    const result = await mergeGuestCustomersIntoCanonical({
      canonicalCustomerRecordId: "recCanonicalMain",
      guestCustomerRecordIds: ["recGuest1", "recGuest2"],
    });

    assert.deepEqual(result.mergedPairs, [
      "recGuest1->recCanonicalMain",
      "recGuest2->recCanonicalMain",
    ]);
    assert.equal(result.relinkedSavedConfigurations, 2);
    assert.equal(result.relinkedLabels, 2);
    assert.equal(result.relinkedOrders, 2);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("merge marker persistence failure throws when merge fields are missing", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestNoMarker', ARRAYJOIN({Customer}))"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labels,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestNoMarker', ARRAYJOIN({Customers}))"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recGuestNoMarker', ARRAYJOIN({Customer}))"
        );
      },
      response: { records: [] },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestNoMarker",
      response: {
        records: [{ id: "recGuestNoMarker", fields: {} }],
      },
    },
  ]);

  try {
    await assert.rejects(
      mergeGuestCustomersIntoCanonical({
        canonicalCustomerRecordId: "recCanonicalMain",
        guestCustomerRecordIds: ["recGuestNoMarker"],
      }),
      (error) => {
        assert.equal(error?.code, "merge_marker_not_persisted");
        return true;
      }
    );
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
