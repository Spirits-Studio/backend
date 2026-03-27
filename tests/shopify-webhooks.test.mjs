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
const { CUSTOMER_CREATION_SOURCES, STUDIO_FIELDS, STUDIO_TABLES } = await import(
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
        assert.ok(!(STUDIO_FIELDS.orders.shopifyProduct in fields));
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyProductId], "10243100311898");
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyVariantId], "445566778899");
        assert.equal(fields[STUDIO_FIELDS.orders.quantity], 3);
        assert.equal(
          fields[STUDIO_FIELDS.orders.configJson],
          JSON.stringify({
            product_name: "Build Your Rum Brand",
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
            product_name: "Build Your Rum Brand",
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
      quantity: 3,
    });

    assert.equal(record?.id, "recOrderSnapshotA");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("order record helper prefers Shopify line-item metadata over saved configuration fallbacks", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Orders & Fulfilment",
      assert: (call) => {
        const formula = call.url.searchParams.get("filterByFormula") || "";
        assert.equal(formula, "{Order ID}='7786446423111'");
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Orders & Fulfilment",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "7786446423111");
        assert.equal(fields[STUDIO_FIELDS.orders.sessionId], "session-line-item");
        assert.equal(fields[STUDIO_FIELDS.orders.displayName], "Order Display Name");
        assert.equal(fields[STUDIO_FIELDS.orders.bottleSelection], "Signature");
        assert.equal(fields[STUDIO_FIELDS.orders.liquidSelection], "Vodka");
        assert.equal(fields[STUDIO_FIELDS.orders.closureSelection], "Gold Stopper");
        assert.equal(fields[STUDIO_FIELDS.orders.waxSelection], "Emerald");
        assert.equal(fields[STUDIO_FIELDS.orders.internalSku], "SKU-LINE-ITEM");
        assert.ok(!(STUDIO_FIELDS.orders.shopifyProduct in fields));
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyProductId], "10243095429466");
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyVariantId], "6677889900");
        assert.equal(fields[STUDIO_FIELDS.orders.quantity], 2);
        assert.equal(
          fields[STUDIO_FIELDS.orders.previewImageUrl],
          "https://cdn.example.com/preview-from-line-item.png"
        );
        assert.equal(
          fields[STUDIO_FIELDS.orders.frontLabelUrl],
          "https://cdn.example.com/front-from-line-item.png"
        );
      },
      response: {
        records: [{ id: "recOrderLineItemPreferred", fields: {} }],
      },
    },
  ]);

  try {
    const record = await createOrderRecordForSavedConfiguration({
      orderId: "7786446423111",
      orderStatus: "Ordered",
      savedConfigurationRecordId: "recSavedConfigLineItem",
      savedConfigurationRecord: {
        id: "recSavedConfigLineItem",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.sessionId]: "session-airtable",
          [STUDIO_FIELDS.savedConfigurations.displayName]: "Fallback Display Name",
          [STUDIO_FIELDS.savedConfigurations.bottleSelection]: "Fallback Bottle",
          [STUDIO_FIELDS.savedConfigurations.liquidSelection]: "Fallback Liquid",
          [STUDIO_FIELDS.savedConfigurations.closureSelection]: "Fallback Closure",
          [STUDIO_FIELDS.savedConfigurations.waxSelection]: "Fallback Wax",
          [STUDIO_FIELDS.savedConfigurations.internalSku]: "SKU-FALLBACK",
          [STUDIO_FIELDS.savedConfigurations.shopifyProductId]: "11111111111111",
          [STUDIO_FIELDS.savedConfigurations.shopifyVariantId]: "2222222222",
          [STUDIO_FIELDS.savedConfigurations.configJson]: JSON.stringify({
            product_name: "Fallback Product",
            preview_url: "https://cdn.example.com/preview-fallback.png",
          }),
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelOutputImageUrl]:
            "https://cdn.example.com/front-fallback.png",
        },
      },
      customerRecordIds: ["recCanonicalCustomer"],
      lineItemMetadata: {
        session_id: "session-line-item",
        display_name: "Order Display Name",
        bottle: "Signature",
        liquid: "Vodka",
        closure: "Gold Stopper",
        wax: "Emerald",
        sku: "SKU-LINE-ITEM",
        product: "Build Your Vodka Brand",
        product_id: "10243095429466",
        variant_id: "6677889900",
        quantity: 2,
        preview_url: "https://cdn.example.com/preview-from-line-item.png",
        front_label_url: "https://cdn.example.com/front-from-line-item.png",
      },
    });

    assert.equal(record?.id, "recOrderLineItemPreferred");
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
        assert.equal(call.url.searchParams.get("filterByFormula"), null);
      },
      response: {
        records: [
          {
            id: "recAddressOtherOrder",
            fields: {
              [STUDIO_FIELDS.addresses.orders]: ["recDifferentOrder"],
            },
          },
        ],
      },
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
            product_id: 10243100311898,
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
        assert.equal(fields[STUDIO_FIELDS.orders.quantity], 2);
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
            product_id: 10243100311898,
            quantity: 2,
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

test("orders/create writes Shopify order details metafield when admin token is configured", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_metafield_sync_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookOrderMetafield1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigMeta",
      response: {
        id: "recSavedConfigMeta",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalMetaCustomer"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
          [STUDIO_FIELDS.savedConfigurations.displayName]: "Fallback Display",
          [STUDIO_FIELDS.savedConfigurations.sessionId]: "session-fallback-meta",
          [STUDIO_FIELDS.savedConfigurations.bottleSelection]: "Fallback Bottle",
          [STUDIO_FIELDS.savedConfigurations.liquidSelection]: "Fallback Liquid",
          [STUDIO_FIELDS.savedConfigurations.closureSelection]: "Fallback Closure",
          [STUDIO_FIELDS.savedConfigurations.waxSelection]: "Fallback Wax",
          [STUDIO_FIELDS.savedConfigurations.internalSku]: "SKU-FALLBACK-META",
          [STUDIO_FIELDS.savedConfigurations.shopifyProductId]: "10197521465690",
          [STUDIO_FIELDS.savedConfigurations.shopifyVariantId]: "555111222",
          [STUDIO_FIELDS.savedConfigurations.previewImageUrl]:
            "https://cdn.example.com/preview-fallback-meta.png",
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelOutputImageUrl]:
            "https://cdn.example.com/front-fallback-meta.png",
          [STUDIO_FIELDS.savedConfigurations.configJson]: JSON.stringify({
            product_name: "Fallback Product Meta",
          }),
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recCanonicalMetaCustomer",
      response: {
        id: "recCanonicalMetaCustomer",
        fields: {
          "Shopify ID": "9002",
          "First Name": "Marcus",
          "Last Name": "Smith",
          Source: "Shopify",
          "Shop Domain": "wnbrmm-sg.myshopify.com",
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigMeta",
      response: {
        id: "recSavedConfigMeta",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalMetaCustomer"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
          [STUDIO_FIELDS.savedConfigurations.displayName]: "Fallback Display",
          [STUDIO_FIELDS.savedConfigurations.sessionId]: "session-fallback-meta",
          [STUDIO_FIELDS.savedConfigurations.bottleSelection]: "Fallback Bottle",
          [STUDIO_FIELDS.savedConfigurations.liquidSelection]: "Fallback Liquid",
          [STUDIO_FIELDS.savedConfigurations.closureSelection]: "Fallback Closure",
          [STUDIO_FIELDS.savedConfigurations.waxSelection]: "Fallback Wax",
          [STUDIO_FIELDS.savedConfigurations.internalSku]: "SKU-FALLBACK-META",
          [STUDIO_FIELDS.savedConfigurations.shopifyProductId]: "10197521465690",
          [STUDIO_FIELDS.savedConfigurations.shopifyVariantId]: "555111222",
          [STUDIO_FIELDS.savedConfigurations.previewImageUrl]:
            "https://cdn.example.com/preview-fallback-meta.png",
          [STUDIO_FIELDS.savedConfigurations.currentFrontLabelOutputImageUrl]:
            "https://cdn.example.com/front-fallback-meta.png",
          [STUDIO_FIELDS.savedConfigurations.configJson]: JSON.stringify({
            product_name: "Fallback Product Meta",
          }),
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='7787000000001'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.displayName], "Custom Order Name");
        assert.equal(fields[STUDIO_FIELDS.orders.bottleSelection], "Heritage");
        assert.equal(fields[STUDIO_FIELDS.orders.liquidSelection], "Gin");
        assert.equal(fields[STUDIO_FIELDS.orders.closureSelection], "Natural Cork");
        assert.equal(fields[STUDIO_FIELDS.orders.waxSelection], "Crimson");
        assert.equal(fields[STUDIO_FIELDS.orders.internalSku], "SKU-META-001");
        assert.ok(!(STUDIO_FIELDS.orders.shopifyProduct in fields));
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyProductId], "10197521465690");
        assert.equal(fields[STUDIO_FIELDS.orders.shopifyVariantId], "555111222");
        assert.equal(fields[STUDIO_FIELDS.orders.quantity], 2);
      },
      response: { records: [{ id: "recOrderMeta1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.addresses,
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.addresses,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(
          fields[STUDIO_FIELDS.addresses.fullAddress],
          "Marcus Smith, 1 Test Street, London, SW1A 1AA, United Kingdom"
        );
        assert.deepEqual(fields[STUDIO_FIELDS.addresses.customer], [
          "recCanonicalMetaCustomer",
        ]);
        assert.deepEqual(fields[STUDIO_FIELDS.addresses.orders], ["recOrderMeta1"]);
      },
      response: { records: [{ id: "recOrderMetaAddress1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigMeta",
      response: {
        id: "recSavedConfigMeta",
        fields: {
          [STUDIO_FIELDS.savedConfigurations.customer]: ["recCanonicalMetaCustomer"],
          [STUDIO_FIELDS.savedConfigurations.labels]: [],
          [STUDIO_FIELDS.savedConfigurations.labelVersions]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.savedConfigurations,
      recordId: "recSavedConfigMeta",
      response: { records: [{ id: "recSavedConfigMeta", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recFrontVersionMeta",
      response: {
        id: "recFrontVersionMeta",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recFrontVersionMeta",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recSavedConfigMeta",
        ]);
      },
      response: { records: [{ id: "recFrontVersionMeta", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recBackVersionMeta",
      response: {
        id: "recBackVersionMeta",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Back",
          [STUDIO_FIELDS.labelVersions.savedConfigurations]: [],
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.labelVersions,
      recordId: "recBackVersionMeta",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.deepEqual(fields[STUDIO_FIELDS.labelVersions.savedConfigurations], [
          "recSavedConfigMeta",
        ]);
      },
      response: { records: [{ id: "recBackVersionMeta", fields: {} }] },
    },
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "wnbrmm-sg.myshopify.com");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.equal(
          call.headers["X-Shopify-Access-Token"],
          "shpat_test_admin_token"
        );
        assert.match(call.body?.query || "", /metafieldsSet/);
        const metafield = call.body?.variables?.metafields?.[0] || {};
        assert.equal(metafield.ownerId, "gid://shopify/Order/7787000000001");
        assert.equal(metafield.namespace, "ss");
        assert.equal(metafield.key, "order_details");
        assert.equal(metafield.type, "json");

        const value = JSON.parse(metafield.value);
        assert.equal(value.schema_version, 1);
        assert.equal(value.order_id, "7787000000001");
        assert.equal(value.customer_name, "Marcus Smith");
        assert.equal(value.shipping_address, "Marcus Smith, 1 Test Street, London, SW1A 1AA, United Kingdom");
        assert.equal(value.items.length, 1);
        assert.deepEqual(value.items[0], {
          airtable_order_record_id: "recOrderMeta1",
          saved_configuration_id: "recSavedConfigMeta",
          session_id: "session-meta-1",
          airtable_customer_id: "recCanonicalMetaCustomer",
          order_id: "7787000000001",
          customer_name: "Marcus Smith",
          shipping_address: "Marcus Smith, 1 Test Street, London, SW1A 1AA, United Kingdom",
          product: "Build Your Gin Brand",
          product_id: "10197521465690",
          variant_id: "555111222",
          sku: "SKU-META-001",
          quantity: 2,
          preview_image_url: "https://cdn.example.com/preview-meta.png",
          front_label_url: "https://cdn.example.com/front-meta.png",
          front_label_version_id: "recFrontVersionMeta",
          back_label_version_id: "recBackVersionMeta",
          display_name: "Custom Order Name",
          bottle: "Heritage",
          liquid: "Gin",
          closure: "Natural Cork",
          wax: "Crimson",
        });
      },
      response: {
        data: {
          metafieldsSet: {
            metafields: [{ id: "gid://shopify/Metafield/1", namespace: "ss", key: "order_details" }],
            userErrors: [],
          },
        },
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookOrderMetafield1",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "processed");
      },
      response: { records: [{ id: "recWebhookOrderMetafield1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_order_metafield_sync_1",
      payload: {
        id: 7787000000001,
        name: "#1001",
        customer: {
          id: 9002,
          first_name: "Marcus",
          last_name: "Smith",
        },
        shipping_address: {
          first_name: "Marcus",
          last_name: "Smith",
          address1: "1 Test Street",
          city: "London",
          zip: "SW1A 1AA",
          country: "United Kingdom",
        },
        line_items: [
          {
            id: 1,
            title: "Build Your Gin Brand",
            sku: "SKU-SHOPIFY-FALLBACK",
            product_id: 10197521465690,
            variant_id: 555111222,
            quantity: 2,
            properties: [
              { name: "_saved_configuration_id", value: "recSavedConfigMeta" },
              { name: "_session_id", value: "session-meta-1" },
              { name: "_ss_customer_airtable_id", value: "recCanonicalMetaCustomer" },
              { name: "_display_name", value: "Custom Order Name" },
              { name: "_preview_url", value: "https://cdn.example.com/preview-meta.png" },
              { name: "_front_label_url", value: "https://cdn.example.com/front-meta.png" },
              { name: "_sku", value: "SKU-META-001" },
              { name: "_label_front_version_id", value: "recFrontVersionMeta" },
              { name: "_label_back_version_id", value: "recBackVersionMeta" },
              { name: "Bottle", value: "Heritage" },
              { name: "Liquid", value: "Gin" },
              { name: "Closure", value: "Natural Cork" },
              { name: "Wax", value: "Crimson" },
            ],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.shopify_order_metafield_sync?.ok, true);
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
    fetchMock.restore();
  }
});

test("orders/create writes compliance metadata for non-studio orders", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_compliance_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookOrderCompliance1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recComplianceCustomer1",
      response: {
        id: "recComplianceCustomer1",
        fields: {
          "Shopify ID": "",
          Email: "",
          "First Name": "",
          "Last Name": "",
          Source: "Direct",
          "Shop Domain": "",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recComplianceCustomer1",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Email, "trade@example.com");
        assert.equal(fields["First Name"], "Trade");
        assert.equal(fields["Last Name"], "Buyer");
        assert.equal(fields.Source, "Shopify");
        assert.equal(fields["Shop Domain"], "wnbrmm-sg.myshopify.com");
      },
      response: { records: [{ id: "recComplianceCustomer1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='8899000011112'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "8899000011112");
        assert.equal(fields[STUDIO_FIELDS.orders.orderStatus], "Ordered");
        assert.equal(fields[STUDIO_FIELDS.orders.forResale], true);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], [
          "recComplianceCustomer1",
        ]);
      },
      response: { records: [{ id: "recComplianceOrder1", fields: {} }] },
    },
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "wnbrmm-sg.myshopify.com");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        const metafield = call.body?.variables?.metafields?.[0] || {};
        assert.equal(metafield.ownerId, "gid://shopify/Order/8899000011112");
        assert.equal(metafield.namespace, "ss");
        assert.equal(metafield.key, "compliance");
        assert.equal(metafield.type, "json");

        const value = JSON.parse(metafield.value);
        assert.equal(value.order_id, "8899000011112");
        assert.equal(value.customer_airtable_id, "recComplianceCustomer1");
        assert.equal(value.session_id, "session-compliance-1");
        assert.equal(value.for_resale, true);
        assert.equal(value.premise_licence, "PL-123");
        assert.equal(value.alcohol_licence, "AL-456");
        assert.equal(value.licence_type, "personal");
        assert.equal(value.personal_licence, "PERSONAL-789");
        assert.equal(value.is_business_purchase, true);
        assert.equal(value.company_name, "Spirits Studio Trade Ltd");
        assert.equal(value.trading_name, "SS Trade");
        assert.equal(value.company_number, "12345678");
        assert.equal(value.vat_number, "GB123456789");
      },
      response: {
        data: {
          metafieldsSet: {
            metafields: [{ id: "gid://shopify/Metafield/2", namespace: "ss", key: "compliance" }],
            userErrors: [],
          },
        },
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookOrderCompliance1",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "processed");
      },
      response: { records: [{ id: "recWebhookOrderCompliance1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_order_compliance_1",
      payload: {
        id: 8899000011112,
        name: "#1002",
        email: "trade@example.com",
        billing_address: {
          first_name: "Trade",
          last_name: "Buyer",
          address1: "2 Market Road",
          city: "London",
          zip: "SE1 1AA",
          country: "United Kingdom",
        },
        note_attributes: [
          { name: "_ss_for_resale", value: "true" },
          { name: "_ss_premise_licence", value: "PL-123" },
          { name: "_ss_alcohol_licence", value: "AL-456" },
          { name: "_ss_licence_type", value: "personal" },
          { name: "_ss_personal_licence", value: "PERSONAL-789" },
          { name: "_ss_is_business_purchase", value: "true" },
          { name: "_ss_company_name", value: "Spirits Studio Trade Ltd" },
          { name: "_ss_trading_name", value: "SS Trade" },
          { name: "_ss_company_number", value: "12345678" },
          { name: "_ss_vat_number", value: "GB123456789" },
          { name: "_ss_customer_airtable_id", value: "recComplianceCustomer1" },
          { name: "_ss_session_id", value: "session-compliance-1" },
        ],
        line_items: [
          {
            id: 1,
            title: "Trade Order",
            product_id: 555,
            variant_id: 777,
            quantity: 30,
            properties: [],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.deepEqual(res.body?.compliance_order_records, ["recComplianceOrder1"]);
    assert.equal(res.body?.shopify_order_metafield_sync?.skipped, true);
    assert.equal(res.body?.shopify_order_compliance_metafield_sync?.ok, true);
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
    fetchMock.restore();
  }
});

test("orders/create ignores stale compliance customer ids for signed-in customers", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_compliance_signed_in_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookOrderComplianceSignedIn1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.match(
          call.url.searchParams.get("filterByFormula") || "",
          /\{Email\}/
        );
      },
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.match(
          call.url.searchParams.get("filterByFormula") || "",
          /\{Shopify ID\}='9111'/
        );
      },
      response: {
        records: [
          {
            id: "recSignedInComplianceCustomer1",
            fields: {
              "Shopify ID": "9111",
              Email: "signedin@example.com",
              "First Name": "Signed",
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
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Order ID}='8899000011113'"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.orders,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.orders.orderId], "8899000011113");
        assert.equal(fields[STUDIO_FIELDS.orders.orderStatus], "Ordered");
        assert.equal(fields[STUDIO_FIELDS.orders.forResale], false);
        assert.deepEqual(fields[STUDIO_FIELDS.orders.customer], [
          "recSignedInComplianceCustomer1",
        ]);
      },
      response: { records: [{ id: "recComplianceOrderSignedIn1", fields: {} }] },
    },
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "wnbrmm-sg.myshopify.com");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        const metafield = call.body?.variables?.metafields?.[0] || {};
        assert.equal(metafield.ownerId, "gid://shopify/Order/8899000011113");
        assert.equal(metafield.namespace, "ss");
        assert.equal(metafield.key, "compliance");
        assert.equal(metafield.type, "json");

        const value = JSON.parse(metafield.value);
        assert.equal(value.customer_airtable_id, "recSignedInComplianceCustomer1");
        assert.equal(value.for_resale, false);
      },
      response: {
        data: {
          metafieldsSet: {
            metafields: [{ id: "gid://shopify/Metafield/3", namespace: "ss", key: "compliance" }],
            userErrors: [],
          },
        },
      },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookOrderComplianceSignedIn1",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Status, "processed");
      },
      response: { records: [{ id: "recWebhookOrderComplianceSignedIn1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_order_compliance_signed_in_1",
      payload: {
        id: 8899000011113,
        name: "#1003",
        email: "signedin@example.com",
        customer: {
          id: 9111,
          email: "signedin@example.com",
          first_name: "Signed",
          last_name: "Buyer",
        },
        note_attributes: [
          { name: "_ss_for_resale", value: "false" },
          { name: "_ss_customer_airtable_id", value: "recStaleGuestCustomer" },
          { name: "_ss_session_id", value: "session-signed-in-compliance-1" },
        ],
        line_items: [
          {
            id: 1,
            title: "Trade Order",
            product_id: 555,
            variant_id: 777,
            quantity: 30,
            properties: [],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, "recSignedInComplianceCustomer1");
    assert.deepEqual(res.body?.compliance_order_records, ["recComplianceOrderSignedIn1"]);
    assert.equal(res.body?.shopify_order_compliance_metafield_sync?.ok, true);
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
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
            product_id: 10243100311898,
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
            product_id: 10243100311898,
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

test("orders/create ignores unsupported Shopify product ids", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_order_unsupported_product_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookUnsupportedProduct1", fields: {} }] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookUnsupportedProduct1",
      response: { records: [{ id: "recWebhookUnsupportedProduct1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "orders/create",
      webhookId: "wh_order_unsupported_product_1",
      payload: {
        id: 5550001,
        email: null,
        customer: {},
        line_items: [
          {
            id: 1,
            product_id: 99999999999999,
            quantity: 1,
            properties: [
              { name: "_saved_configuration_id", value: "recSavedConfigA" },
            ],
          },
        ],
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookOrdersCreate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.deepEqual(res.body?.created_order_records, []);
    assert.deepEqual(res.body?.updated_saved_configurations, []);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("upsertCanonicalCustomer claims the preferred Airtable record before global Shopify id lookup", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recPreferredGuestCustomer",
      response: {
        id: "recPreferredGuestCustomer",
        fields: {
          Email: "stale-guest@example.com",
          Source: "Guest",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recPreferredGuestCustomer",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "10425417531738");
        assert.equal(fields.Email, "marcuswonder@hotmail.co.uk");
        assert.equal(fields["First Name"], "Marcus");
        assert.equal(fields["Last Name"], "Smith");
        assert.equal(fields.Source, "Shopify");
      },
      response: {
        records: [{ id: "recPreferredGuestCustomer", fields: {} }],
      },
    },
  ]);

  try {
    const result = await upsertCanonicalCustomer({
      shopifyId: "gid://shopify/Customer/10425417531738",
      email: "marcuswonder@hotmail.co.uk",
      firstName: "Marcus",
      lastName: "Smith",
      preferredCustomerRecordIds: ["recPreferredGuestCustomer"],
    });

    assert.equal(result.customerRecordId, "recPreferredGuestCustomer");
    assert.equal(result.created, false);
    assert.equal(result.matchedBy, "preferred_record");
    assert.equal(result.updated, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("upsertCanonicalCustomer guest fallback ignores stale preferred records and continues to email then phone", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recStalePreferredGuest",
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
          "({Email}='guest-fallback@example.com')"
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
          "({Phone}='+447700900555')"
        );
      },
      response: {
        records: [
          {
            id: "recCanonicalGuestByPhone",
            fields: {
              Phone: "+447700900555",
              Source: "Guest",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recCanonicalGuestByPhone",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields.Email, "guest-fallback@example.com");
        assert.equal(fields["First Name"], "Guest");
        assert.equal(fields["Last Name"], "Fallback");
        assert.equal(fields.Source, "Shopify");
      },
      response: {
        records: [{ id: "recCanonicalGuestByPhone", fields: {} }],
      },
    },
  ]);

  try {
    const result = await upsertCanonicalCustomer({
      email: "guest-fallback@example.com",
      firstName: "Guest",
      lastName: "Fallback",
      phone: "+447700900555",
      preferredCustomerRecordIds: ["recStalePreferredGuest"],
    });

    assert.equal(result.customerRecordId, "recCanonicalGuestByPhone");
    assert.equal(result.created, false);
    assert.equal(result.matchedBy, "phone");
    assert.equal(result.updated, true);
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("upsertCanonicalCustomer creates new customers with the supplied creation source", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='7010')"
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
          "({Shopify ID}='gid://shopify/Customer/7010')"
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
          "({Email}='created-via-order@example.com')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "7010");
        assert.equal(fields.Email, "created-via-order@example.com");
        assert.equal(fields.Source, "Shopify");
        assert.equal(
          fields["Creation Source"],
          CUSTOMER_CREATION_SOURCES.shopifyWebhookOrdersCreate
        );
      },
      response: {
        records: [{ id: "recCreatedViaOrdersCreateWebhook", fields: {} }],
      },
    },
  ]);

  try {
    const result = await upsertCanonicalCustomer({
      shopifyId: "gid://shopify/Customer/7010",
      email: "created-via-order@example.com",
      firstName: "Order",
      lastName: "Webhook",
      creationSource: CUSTOMER_CREATION_SOURCES.shopifyWebhookOrdersCreate,
    });

    assert.equal(result.customerRecordId, "recCreatedViaOrdersCreateWebhook");
    assert.equal(result.created, true);
    assert.equal(result.matchedBy, "shopify_id");
    assert.equal(result.updated, false);
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

test("customers/update falls back to phone before creating a new customer", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_customer_update_phone_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookCustomerUpdatePhone1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='7003')"
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
          "({Shopify ID}='gid://shopify/Customer/7003')"
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
          "({Email}='phone-match@example.com')"
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
          "({Phone}='+447700900123')"
        );
      },
      response: {
        records: [
          {
            id: "recCanonicalByPhone",
            fields: {
              Phone: "+447700900123",
              Source: "Guest",
            },
          },
        ],
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recCanonicalByPhone",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields["Shopify ID"], "7003");
        assert.equal(fields.Email, "phone-match@example.com");
        assert.equal(fields["First Name"], "Phone");
        assert.equal(fields["Last Name"], "Match");
        assert.equal(fields.Source, "Shopify");
      },
      response: { records: [{ id: "recCanonicalByPhone", fields: {} }] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookCustomerUpdatePhone1",
      response: { records: [{ id: "recWebhookCustomerUpdatePhone1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "customers/update",
      webhookId: "wh_customer_update_phone_1",
      payload: {
        id: 7003,
        email: "phone-match@example.com",
        phone: null,
        first_name: "Phone",
        last_name: "Match",
        default_address: {
          phone: "+447700900123",
        },
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookCustomersUpdate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, "recCanonicalByPhone");
    assert.equal(res.body?.created_customer, false);
    assert.equal(res.body?.matched_by, "phone");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("customers/update skips when no Airtable customer matches", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: "Webhook Events",
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Webhook ID}='wh_customer_update_skip_1')"
        );
      },
      response: { records: [] },
    },
    {
      method: "POST",
      table: "Webhook Events",
      response: { records: [{ id: "recWebhookCustomerUpdateSkip1", fields: {} }] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "({Shopify ID}='7999')"
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
          "({Shopify ID}='gid://shopify/Customer/7999')"
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
          "({Email}='missing-update@example.com')"
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
          "({Phone}='+447700900999')"
        );
      },
      response: { records: [] },
    },
    {
      method: "PATCH",
      table: "Webhook Events",
      recordId: "recWebhookCustomerUpdateSkip1",
      response: { records: [{ id: "recWebhookCustomerUpdateSkip1", fields: {} }] },
    },
  ]);

  try {
    const req = createWebhookRequest({
      topic: "customers/update",
      webhookId: "wh_customer_update_skip_1",
      payload: {
        id: 7999,
        email: "missing-update@example.com",
        first_name: "Missing",
        last_name: "Customer",
        default_address: {
          phone: "+447700900999",
        },
      },
    });
    const res = createWebhookResponse();

    await shopifyWebhookCustomersUpdate(req, res);

    assert.equal(res.statusCode, 200);
    assert.equal(res.body?.ok, true);
    assert.equal(res.body?.canonical_customer_record_id, null);
    assert.equal(res.body?.created_customer, false);
    assert.equal(res.body?.updated_customer, false);
    assert.equal(res.body?.matched_by, null);
    assert.equal(res.body?.skipped, true);
    assert.equal(res.body?.reason, "customer_not_found");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
