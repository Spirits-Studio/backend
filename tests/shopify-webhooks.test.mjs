import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";

process.env.SHOPIFY_WEBHOOK_SECRET = "test-webhook-secret";
process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_ADDRESSES_TABLE_ID = "Addresses";
process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID = "Saved Configurations";
process.env.AIRTABLE_LABELS_TABLE_ID = "Labels";
process.env.AIRTABLE_LABEL_VERSIONS_TABLE_ID = "Label Versions";
process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID = "Orders & Fulfilment";
process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID = "Webhook Events";

const { default: shopifyWebhookOrdersCreate } = await import(
  "../netlify/functions/shopify-webhook-orders-create.js"
);
const { default: shopifyWebhookOrdersPaid } = await import(
  "../netlify/functions/shopify-webhook-orders-paid.js"
);
const { default: shopifyWebhookCustomersCreate } = await import(
  "../netlify/functions/shopify-webhook-customers-create.js"
);
const { default: shopifyWebhookCustomersUpdate } = await import(
  "../netlify/functions/shopify-webhook-customers-update.js"
);
const {
  createWebhookVerificationDebugInfo,
  createWebhookPayloadDebugInfo,
} = await import(
  "../netlify/functions/_lib/shopifyWebhook.js"
);
const {
  createOrderRecordForSavedConfiguration,
  upsertCanonicalCustomer,
} = await import(
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

const createWebhookRequestV2 = ({
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

  return new Request("https://example.netlify.app/.netlify/functions/webhook", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-shopify-topic": topic,
      "x-shopify-shop-domain": shopDomain,
      "x-shopify-webhook-id": webhookId,
      "x-shopify-hmac-sha256": hmac,
    },
    body: rawBody,
  });
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

test("webhook verification debug info redacts secrets and hmacs", () => {
  const info = createWebhookVerificationDebugInfo({
    rawBody: JSON.stringify({ id: 123, email: "debug@example.com" }),
    headers: {
      "content-type": "application/json",
      "user-agent": "Shopify-Captain-Hook",
      "x-shopify-topic": "orders/paid",
      "x-shopify-shop-domain": "wnbrmm-sg.myshopify.com",
      "x-shopify-webhook-id": "wh_debug_1",
    },
    providedHmac: "abcdefghijklmnopqrstuvwxyz0123456789",
    secret: "super-secret-value",
  });

  assert.equal(info.verification_debug.enabled, true);
  assert.equal(info.verification_debug.has_secret, true);
  assert.notEqual(info.verification_debug.secret_fingerprint, "super-secret-value");
  assert.equal(
    info.verification_debug.provided_hmac,
    "abcdefghijkl...456789"
  );
  assert.ok(info.verification_debug.raw_body_sha256);
  assert.equal(info.verification_debug.topic_header, "orders/paid");
  assert.equal(
    info.verification_debug.shop_domain_header,
    "wnbrmm-sg.myshopify.com"
  );
});

test("webhook payload debug info includes full raw body and parsed payload", () => {
  const info = createWebhookPayloadDebugInfo({
    envelope: {
      topic: "orders/paid",
      shop_domain: "wnbrmm-sg.myshopify.com",
      webhook_id: "wh_payload_debug_1",
      received_at: "2026-03-19T03:00:00.000Z",
      headers: {
        "content-type": "application/json",
        "x-shopify-topic": "orders/paid",
      },
      rawBody: '{"id":123,"email":"debug@example.com"}',
      payload: { id: 123, email: "debug@example.com" },
    },
  });

  assert.equal(info.request_debug.topic, "orders/paid");
  assert.equal(info.request_debug.shop_domain, "wnbrmm-sg.myshopify.com");
  assert.equal(info.request_debug.webhook_id, "wh_payload_debug_1");
  assert.equal(info.request_debug.raw_body, '{"id":123,"email":"debug@example.com"}');
  assert.deepEqual(info.request_debug.parsed_payload, {
    id: 123,
    email: "debug@example.com",
  });
  assert.equal(info.request_debug.headers["x-shopify-topic"], "orders/paid");
});

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

test("orders/paid webhook returns a Web Response for Request-based invocations", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_event_orders_paid_1')");
      },
      response: {
        records: [{ id: "recWebhookEventOrdersPaid", fields: { Status: "processed" } }],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventOrdersPaid",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventOrdersPaid", fields: {} }],
      },
    },
  ]);

  try {
    const request = createWebhookRequestV2({
      topic: "orders/paid",
      webhookId: "wh_event_orders_paid_1",
      payload: {
        id: 10003,
        line_items: [],
      },
    });

    const response = await shopifyWebhookOrdersPaid(request);
    assert.ok(response instanceof Response);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/json");

    const body = await response.json();
    assert.equal(body?.ok, true);
    assert.equal(body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customers/create webhook returns a Web Response for Request-based invocations", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_event_customers_create_v2_1')");
      },
      response: {
        records: [
          { id: "recWebhookEventCustomersCreateV2", fields: { Status: "processed" } },
        ],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventCustomersCreateV2",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventCustomersCreateV2", fields: {} }],
      },
    },
  ]);

  try {
    const request = createWebhookRequestV2({
      topic: "customers/create",
      webhookId: "wh_event_customers_create_v2_1",
      payload: {
        id: 503,
        email: "customer-create-v2@example.com",
      },
    });

    const response = await shopifyWebhookCustomersCreate(request);
    assert.ok(response instanceof Response);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/json");

    const body = await response.json();
    assert.equal(body?.ok, true);
    assert.equal(body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customers/update webhook returns a Web Response for Request-based invocations", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "({Webhook ID}='wh_event_customers_update_v2_1')");
      },
      response: {
        records: [
          { id: "recWebhookEventCustomersUpdateV2", fields: { Status: "processed" } },
        ],
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookEventCustomersUpdateV2",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "skipped");
      },
      response: {
        records: [{ id: "recWebhookEventCustomersUpdateV2", fields: {} }],
      },
    },
  ]);

  try {
    const request = createWebhookRequestV2({
      topic: "customers/update",
      webhookId: "wh_event_customers_update_v2_1",
      payload: {
        id: 504,
        email: "customer-update-v2@example.com",
      },
    });

    const response = await shopifyWebhookCustomersUpdate(request);
    assert.ok(response instanceof Response);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/json");

    const body = await response.json();
    assert.equal(body?.ok, true);
    assert.equal(body?.idempotent_skip, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("orders webhook processes without webhook idempotency storage when no webhook table is configured", async () => {
  const previousWebhookEventsTableId = process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID;
  const previousWebhookEventsTable = process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE;

  delete process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID;
  delete process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE;

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_no_webhook_table_1",
      payload: {
        id: 10004,
        line_items: [],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.status, "processed");
  } finally {
    if (previousWebhookEventsTableId == null) {
      delete process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID;
    } else {
      process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE_ID = previousWebhookEventsTableId;
    }

    if (previousWebhookEventsTable == null) {
      delete process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE;
    } else {
      process.env.AIRTABLE_WEBHOOK_EVENTS_TABLE = previousWebhookEventsTable;
    }
  }
});

test("order record helper updates existing order for the same order id and saved configuration", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Orders & Fulfilment",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "{Order ID}='7786446422362'");
      },
      response: {
        records: [
          {
            id: "recDifferentOrderForSameShopifyOrderId",
            fields: {
              "Order ID": "7786446422362",
              "Saved Configuration": ["recOtherSavedConfig"],
              "Order Status": "Ordered",
            },
          },
          {
            id: "recExistingOrderA",
            fields: {
              "Order ID": "7786446422362",
              "Saved Configuration": ["recSavedConfigA"],
              "Order Status": "Ordered",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: "Orders & Fulfilment",
      recordId: "recExistingOrderA",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Order ID"], "7786446422362");
        assert.deepEqual(fields["Saved Configuration"], ["recSavedConfigA"]);
        assert.deepEqual(fields.Customer, ["recCanonicalCustomer"]);
        assert.equal(fields["Order Status"], "Ordered");
      },
      response: {
        records: [{ id: "recExistingOrderA", fields: {} }],
      },
    },
  ]);

  try {
    const record = await createOrderRecordForSavedConfiguration({
      orderId: "7786446422362",
      orderStatus: "Paid",
      savedConfigurationRecordId: "recSavedConfigA",
      savedConfigurationRecord: {
        id: "recSavedConfigA",
        fields: {},
      },
      customerRecordIds: ["recCanonicalCustomer"],
    });

    assert.equal(record?.id, "recExistingOrderA");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("order record helper copies saved configuration snapshot fields into Orders & Fulfilment", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Orders & Fulfilment",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "{Order ID}='7786446422999'");
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Orders & Fulfilment",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "7786446422999");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], [
          "recCanonicalCustomer",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedConfigSnapshot",
        ]);
        assert.equal(fields[STUDIO_FIELDS.orders.orderStatus], "Ordered");
        assert.equal(fields[STUDIO_FIELDS.orders.configurationId], "cfg_123");
        assert.equal(fields[STUDIO_FIELDS.orders.sessionId], "session-123");
        assert.equal(fields[STUDIO_FIELDS.orders.configuratorTool], "Build Your Rum Brand");
        assert.equal(fields[STUDIO_FIELDS.orders.alcoholSelection], "Rum");
        assert.equal(fields[STUDIO_FIELDS.orders.bottleSelection], "Origin");
        assert.equal(fields[STUDIO_FIELDS.orders.liquidSelection], "White Rum");
        assert.equal(fields[STUDIO_FIELDS.orders.closureSelection], "Wooden Cork");
        assert.equal(fields[STUDIO_FIELDS.orders.waxSelection], "Black");
        assert.equal(fields[STUDIO_FIELDS.orders.internalSku], "SKU-123");
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyProductId], "10243100311898");
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyVariantId], "445566778899");
        assert.equal(
          fields[STUDIO_FIELDS.orders.configJson],
          JSON.stringify({
            preview_url: "https://cdn.example.com/preview-image.png",
            selectedLabelVersion: {
              designSide: "front",
              outputImageUrl: "https://cdn.example.com/front-label-fallback.png",
            },
          })
        );
        assert.equal(
          fields[STUDIO_FIELDS.orders.frontLabelUrl],
          "https://cdn.example.com/front-label.png"
        );
        assert.deepEqual(fields[STUDIO_FIELDS.orders.frontLabel], [
          { url: "https://cdn.example.com/front-label.png" },
        ]);
        assert.equal(
          fields[STUDIO_FIELDS.orders.previewImageUrl],
          "https://cdn.example.com/preview-image.png"
        );
        assert.deepEqual(fields[STUDIO_FIELDS.orders.previewImage], [
          { url: "https://cdn.example.com/preview-image.png" },
        ]);
        assert.equal(fields[STUDIO_FIELDS.orders.displayName], "Marcus Rum");
        assert.equal(
          fields[STUDIO_FIELDS.orders.creationSource],
          "Shopify -> Netlify Backend (studio-save-configuration)"
        );
      },
      response: {
        records: [{ id: "recOrderSnapshotA", fields: {} }],
      },
    },
  ]);

  try {
    const record = await createOrderRecordForSavedConfiguration({
      orderId: "7786446422999",
      orderStatus: "Paid",
      savedConfigurationRecordId: "recSavedConfigSnapshot",
      savedConfigurationRecord: {
        id: "recSavedConfigSnapshot",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.configurationId]: "cfg_123",
          [STUDIO_FIELDS.savedConfigurations.sessionId]: "session-123",
          [STUDIO_FIELDS.savedConfigurations.configuratorTool]:
            "Build Your Rum Brand",
          [STUDIO_FIELDS.savedConfigurations.alcoholSelection]: "Rum",
          [STUDIO_FIELDS.savedConfigurations.bottleSelection]: "Origin",
          [STUDIO_FIELDS.savedConfigurations.liquidSelection]: "White Rum",
          [STUDIO_FIELDS.savedConfigurations.closureSelection]: "Wooden Cork",
          [STUDIO_FIELDS.savedConfigurations.waxSelection]: "Black",
          [STUDIO_FIELDS.savedConfigurations.internalSku]: "SKU-123",
          [STUDIO_FIELDS.savedConfigurations.shopifyProductId]: "10243100311898",
          [STUDIO_FIELDS.savedConfigurations.shopifyVariantId]: "445566778899",
          [STUDIO_FIELDS.savedConfigurations.configJson]: JSON.stringify({
            preview_url: "https://cdn.example.com/preview-image.png",
            selectedLabelVersion: {
              designSide: "front",
              outputImageUrl: "https://cdn.example.com/front-label-fallback.png",
            },
          }),
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelOutputImageUrl]: [
            "https://cdn.example.com/front-label.png",
          ],
          [STUDIO_FIELDS.savedConfigurations.previewImageUrl]:
            "https://cdn.example.com/preview-image.png",
          [STUDIO_FIELDS.savedConfigurations.displayName]: "Marcus Rum",
          [STUDIO_FIELDS.savedConfigurations.creationSource]:
            "Shopify -> Netlify Backend (studio-save-configuration)",
        },
      },
      customerRecordIds: ["recCanonicalCustomer"],
    });

    assert.equal(record?.id, "recOrderSnapshotA");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("orders webhook creates and links billing address records", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_billing_address_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookBillingAddress1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigBilling",
      response: {
        id: "recSavedConfigBilling",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: [
            "recCanonicalBillingCustomer",
          ],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "AND(LOWER({Email})='billing@example.com', OR({Shopify ID}=BLANK(), {Shopify ID}=''))"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCanonicalBillingCustomer",
      response: {
        id: "recCanonicalBillingCustomer",
        fields: {
          "Shopify ID": "9010",
          Email: "billing@example.com",
          Source: "Guest",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recCanonicalBillingCustomer",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["First Name"], "Marcus");
        assert.equal(fields["Last Name"], "Buyer");
        assert.equal(fields.Phone, "+447700900123");
        assert.equal(fields.Source, "Shopify");
        assert.equal(fields["Shop Domain"], "wnbrmm-sg.myshopify.com");
      },
      response: {
        records: [{ id: "recCanonicalBillingCustomer", fields: {} }],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigBilling",
      response: {
        id: "recSavedConfigBilling",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: [
            "recCanonicalBillingCustomer",
          ],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='321654987'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "321654987");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], [
          "recCanonicalBillingCustomer",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedConfigBilling",
        ]);
      },
      response: { records: [{ id: "recOrderBillingAddress", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.addresses,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='987654321')"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.addresses,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "FIND('recOrderBillingAddress', ARRAYJOIN({Orders & Fulfilment}))"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.addresses,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.addresses.fullAddress], "Marcus Buyer, 1 Test Street, Studio 2, London, Greater London, SW1A 1AA, United Kingdom");
        assert.deepEqual(fields[STUDIO_FIELDS.addresses.customer], [
          "recCanonicalBillingCustomer",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.addresses.orders], [
          "recOrderBillingAddress",
        ]);
        assert.equal(fields[STUDIO_FIELDS.addresses.firstName], "Marcus");
        assert.equal(fields[STUDIO_FIELDS.addresses.lastName], "Buyer");
        assert.equal(fields[STUDIO_FIELDS.addresses.shopifyId], "987654321");
        assert.equal(fields[STUDIO_FIELDS.addresses.streetAddress1], "1 Test Street");
        assert.equal(fields[STUDIO_FIELDS.addresses.streetAddress2], "Studio 2");
        assert.equal(fields[STUDIO_FIELDS.addresses.townCity], "London");
        assert.equal(fields[STUDIO_FIELDS.addresses.county], "Greater London");
        assert.equal(fields[STUDIO_FIELDS.addresses.postalCode], "SW1A 1AA");
        assert.equal(fields[STUDIO_FIELDS.addresses.country], "United Kingdom");
        assert.equal(fields[STUDIO_FIELDS.addresses.phone], "+447700900123");
      },
      response: { records: [{ id: "recBillingAddress1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigBilling",
      response: {
        id: "recSavedConfigBilling",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: [
            "recCanonicalBillingCustomer",
          ],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigBilling",
      response: { records: [{ id: "recSavedConfigBilling", fields: {} }] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookBillingAddress1",
      response: { records: [{ id: "recWebhookBillingAddress1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/paid",
      webhookId: "wh_order_billing_address_1",
      payload: {
        id: 321654987,
        email: "billing@example.com",
        customer: {
          id: 9010,
          email: "billing@example.com",
          first_name: "Marcus",
          last_name: "Buyer",
          phone: "+447700900123",
        },
        billing_address: {
          id: 987654321,
          first_name: "Marcus",
          last_name: "Buyer",
          address1: "1 Test Street",
          address2: "Studio 2",
          city: "London",
          province: "Greater London",
          zip: "SW1A 1AA",
          country: "United Kingdom",
          phone: "+447700900123",
        },
        line_items: [
          {
            id: 11,
            properties: [
              {
                name: "_saved_configuration_id",
                value: "recSavedConfigBilling",
              },
            ],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersPaid(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.deepEqual(res.body?.linked_address_records, ["recBillingAddress1"]);
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

test("guest order with _saved_configuration_id claims the linked customer and enforces saved/label-version linkage", async () => {
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
      table: STUDIO_TABLES.customers,
      recordId: "recGuestCustomer",
      response: {
        id: "recGuestCustomer",
        fields: {
          Email: "guest@example.com",
          Source: "Guest",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestCustomer",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "9001");
        assert.equal(fields["First Name"], "Guest");
        assert.equal(fields["Last Name"], "Buyer");
        assert.equal(fields.Source, "Shopify");
        assert.equal(fields["Shop Domain"], "wnbrmm-sg.myshopify.com");
      },
      response: {
        records: [{ id: "recGuestCustomer", fields: {} }],
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
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelVersion]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='123456789'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "123456789");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], ["recGuestCustomer"]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedConfigA",
        ]);
        assert.equal(fields[STUDIO_FIELDS.orders.orderStatus], "Ordered");
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
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestCustomer"],
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
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.status], "Ordered");
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
    assert.equal(res.body?.canonical_customer_record_id, "recGuestCustomer");
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
      table: STUDIO_TABLES.customers,
      recordId: "recGuestSession",
      response: {
        id: "recGuestSession",
        fields: {
          Email: "guest-session@example.com",
          Source: "Guest",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestSession",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Source, "Shopify");
        assert.equal(fields["Shop Domain"], "wnbrmm-sg.myshopify.com");
      },
      response: {
        records: [{ id: "recGuestSession", fields: {} }],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedBySession",
      response: {
        id: "recSavedBySession",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestSession"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='987001'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "987001");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], ["recGuestSession"]);
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
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestSession"],
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
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.status], "Ordered");
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

test("orders/paid promotes a single linked guest customer record in place", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_paid_promote_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookPromote1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigPromote",
      response: {
        id: "recSavedConfigPromote",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestCustomerPromote"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "AND(LOWER({Email})='marcuswonder@hotmail.co.uk', OR({Shopify ID}=BLANK(), {Shopify ID}=''))"
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestCustomerPromote",
      response: {
        id: "recGuestCustomerPromote",
        fields: {
          Email: "stale-guest@example.com",
          Source: "Shopify",
          "Shop Domain": "wnbrmm-sg.myshopify.com",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recGuestCustomerPromote",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "10425417531738");
        assert.equal(fields.Email, "marcuswonder@hotmail.co.uk");
        assert.equal(fields["First Name"], "Marcus");
        assert.equal(fields["Last Name"], "Smith");
      },
      response: {
        records: [{ id: "recGuestCustomerPromote", fields: {} }],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigPromote",
      response: {
        id: "recSavedConfigPromote",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestCustomerPromote"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='7786796843354'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "7786796843354");
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], [
          "recGuestCustomerPromote",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.savedConfiguration], [
          "recSavedConfigPromote",
        ]);
      },
      response: { records: [{ id: "recOrderPromote1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigPromote",
      response: {
        id: "recSavedConfigPromote",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recGuestCustomerPromote"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigPromote",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.customer], [
          "recGuestCustomerPromote",
        ]);
        assert.equal(fields[STUDIO_FIELDS.savedConfigurations.status], "Ordered");
        assert.deepEqual(fields[STUDIO_FIELDS.savedConfigurations.order], [
          "recOrderPromote1",
        ]);
      },
      response: { records: [{ id: "recSavedConfigPromote", fields: {} }] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookPromote1",
      response: { records: [{ id: "recWebhookPromote1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/paid",
      webhookId: "wh_order_paid_promote_1",
      payload: {
        id: 7786796843354,
        email: "marcuswonder@hotmail.co.uk",
        customer: {
          id: 10425417531738,
          email: "marcuswonder@hotmail.co.uk",
          first_name: "Marcus",
          last_name: "Smith",
        },
        line_items: [
          {
            id: 1,
            properties: [
              {
                name: "_saved_configuration_id",
                value: "recSavedConfigPromote",
              },
            ],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersPaid(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, "recGuestCustomerPromote");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("upsertCanonicalCustomer prefers the linked Airtable customer when Shopify id already matches", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recPreferredCustomerSameId",
      response: {
        id: "recPreferredCustomerSameId",
        fields: {
          "Shopify ID": "10425417531738",
          Email: "stale-guest@example.com",
          Source: "Guest",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recPreferredCustomerSameId",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Email, "marcuswonder@hotmail.co.uk");
        assert.equal(fields["First Name"], "Marcus");
        assert.equal(fields["Last Name"], "Smith");
        assert.equal(fields.Source, "Shopify");
      },
      response: {
        records: [{ id: "recPreferredCustomerSameId", fields: {} }],
      },
    },
  ]);

  try {
    const result = await upsertCanonicalCustomer({
      shopifyId: "gid://shopify/Customer/10425417531738",
      email: "marcuswonder@hotmail.co.uk",
      firstName: "Marcus",
      lastName: "Smith",
      preferredCustomerRecordIds: ["recPreferredCustomerSameId"],
    });

    assert.equal(result.customerRecordId, "recPreferredCustomerSameId");
    assert.equal(result.created, false);
    assert.equal(result.matchedBy, "preferred_record");
    assert.equal(result.updated, true);
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
          "({Shopify ID}='gid://shopify/Customer/7001')"
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
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='gid://shopify/Customer/7002')"
        );
      },
      response: {
        records: [
          {
            id: "recCanonicalExisting",
            fields: {
              "Shopify ID": "gid://shopify/Customer/7002",
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
        assert.equal(fields["Shopify ID"], "7002");
        assert.equal(fields.Email, "new-email@example.com");
      },
      response: { records: [{ id: "recCanonicalExisting", fields: {} }] },
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
