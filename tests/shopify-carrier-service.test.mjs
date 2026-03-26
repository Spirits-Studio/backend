import test from "node:test";
import assert from "node:assert/strict";

process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

const {
  listShopifyCarrierServices,
  upsertShopifyCarrierService,
} = await import("../netlify/functions/_lib/shopifyAdmin.js");

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
    const call = {
      method,
      url,
      headers: init.headers || {},
      body: parseMaybeJson(typeof init.body === "string" ? init.body : ""),
    };

    const step = steps[index];
    assert.ok(step, `Unexpected fetch #${index + 1}: ${method} ${url.toString()}`);
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
      assert.equal(index, steps.length, "Not all expected Shopify Admin calls were made.");
    },
  };
};

test("listShopifyCarrierServices fetches configured carrier services", async () => {
  const fetchMock = installFetchSequence([
    {
      assert: (call) => {
        assert.equal(call.url.host, "wnbrmm-sg.myshopify.com");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.equal(call.headers["X-Shopify-Access-Token"], "shpat_test_admin_token");
        assert.match(call.body?.query || "", /carrierServices/);
      },
      response: {
        data: {
          carrierServices: {
            nodes: [
              {
                id: "gid://shopify/DeliveryCarrierService/1",
                name: "Spirits Studio Shipping",
                callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
                active: true,
                supportsServiceDiscovery: true,
              },
            ],
          },
        },
      },
    },
  ]);

  try {
    const result = await listShopifyCarrierServices({
      shopDomain: "wnbrmm-sg.myshopify.com",
    });
    assert.equal(result.ok, true);
    assert.equal(result.services.length, 1);
    assert.equal(result.services[0].name, "Spirits Studio Shipping");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("upsertShopifyCarrierService creates a new carrier service when none exists", async () => {
  const fetchMock = installFetchSequence([
    {
      response: {
        data: {
          carrierServices: {
            nodes: [],
          },
        },
      },
    },
    {
      assert: (call) => {
        assert.match(call.body?.query || "", /carrierServiceCreate/);
        assert.deepEqual(call.body?.variables?.input, {
          name: "Spirits Studio Shipping",
          callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
          active: true,
          supportsServiceDiscovery: true,
        });
      },
      response: {
        data: {
          carrierServiceCreate: {
            carrierService: {
              id: "gid://shopify/DeliveryCarrierService/9",
              name: "Spirits Studio Shipping",
              callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
              active: true,
              supportsServiceDiscovery: true,
            },
            userErrors: [],
          },
        },
      },
    },
  ]);

  try {
    const result = await upsertShopifyCarrierService({
      shopDomain: "wnbrmm-sg.myshopify.com",
      name: "Spirits Studio Shipping",
      callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
    });
    assert.equal(result.ok, true);
    assert.equal(result.action, "created");
    assert.equal(result.carrierService?.id, "gid://shopify/DeliveryCarrierService/9");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("upsertShopifyCarrierService updates an existing carrier service by name", async () => {
  const fetchMock = installFetchSequence([
    {
      response: {
        data: {
          carrierServices: {
            nodes: [
              {
                id: "gid://shopify/DeliveryCarrierService/3",
                name: "Spirits Studio Shipping",
                callbackUrl: "https://old.example.com/rates",
                active: false,
                supportsServiceDiscovery: false,
              },
            ],
          },
        },
      },
    },
    {
      assert: (call) => {
        assert.match(call.body?.query || "", /carrierServiceUpdate/);
        assert.deepEqual(call.body?.variables?.input, {
          id: "gid://shopify/DeliveryCarrierService/3",
          name: "Spirits Studio Shipping",
          callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
          active: true,
          supportsServiceDiscovery: true,
        });
      },
      response: {
        data: {
          carrierServiceUpdate: {
            carrierService: {
              id: "gid://shopify/DeliveryCarrierService/3",
              name: "Spirits Studio Shipping",
              callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
              active: true,
              supportsServiceDiscovery: true,
            },
            userErrors: [],
          },
        },
      },
    },
  ]);

  try {
    const result = await upsertShopifyCarrierService({
      shopDomain: "wnbrmm-sg.myshopify.com",
      name: "Spirits Studio Shipping",
      callbackUrl: "https://example.com/.netlify/functions/calculate-shipping",
    });
    assert.equal(result.ok, true);
    assert.equal(result.action, "updated");
    assert.equal(result.carrierService?.callbackUrl, "https://example.com/.netlify/functions/calculate-shipping");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});
