import test from "node:test";
import assert from "node:assert/strict";

const { default: calculateShipping } = await import(
  "../netlify/functions/calculate-shipping.js"
);

const createRequest = (body, method = "POST") =>
  new Request("https://example.netlify.app/.netlify/functions/calculate-shipping", {
    method,
    headers: { "Content-Type": "application/json" },
    body: method === "POST" ? JSON.stringify(body) : undefined,
  });

const calculations = {
  currency: "GBP",
  carrierName: "Spirits Studio Courier",
  bottleWeightGrams: 1250,
  quantityGroups: [
    { quantity: 1, productIds: [1001, 1002] },
    { quantity: 30, productIds: [3001, 3002] },
    { quantity: 60, productIds: [6001] },
    { quantity: 120, productIds: [12001] },
    { quantity: 240, productIds: [24001] },
    { quantity: 480, productIds: [48001] },
  ],
  packagingWeights: [
    { units: 1, grams: 500 },
    { units: 6, grams: 400 },
    { units: 60, grams: 4000 },
    { units: 120, grams: 8000 },
    { units: 240, grams: 16000 },
    { units: 480, grams: 32000 },
    { units: 960, grams: 64000 },
    { units: 1920, grams: 128000 },
    { units: 3840, grams: 256000 },
  ],
  services: [
    {
      serviceName: "Standard",
      serviceCode: "standard",
      description: "Carrier-calculated shipping",
      currency: "GBP",
      rateTable: [
        { maxGrams: 50000, totalPrice: 1250 },
        { maxGrams: 100000, totalPrice: 2450 },
        { maxGrams: 200000, totalPrice: 3950 },
      ],
    },
  ],
};

test("calculate-shipping maps grouped product ids to shipment units and weight tiers", async () => {
  const previousCalculations = process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;
  process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON = JSON.stringify(calculations);

  try {
    const response = await calculateShipping(
      createRequest({
        rate: {
          destination: { country: "GB", postal_code: "SW1A 1AA" },
          currency: "GBP",
          items: [
            {
              product_id: 6001,
              quantity: 1,
              grams: 0,
              requires_shipping: true,
            },
            {
              product_id: 1001,
              quantity: 5,
              grams: 0,
              requires_shipping: true,
            },
            {
              product_id: 9999,
              quantity: 2,
              grams: 250,
              requires_shipping: true,
            },
          ],
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.deepEqual(payload, {
      rates: [
        {
          service_name: "Standard",
          service_code: "standard",
          description: "Carrier-calculated shipping",
          total_price: "2450",
          currency: "GBP",
        },
      ],
    });
  } finally {
    if (previousCalculations == null) {
      delete process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;
    } else {
      process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON = previousCalculations;
    }
  }
});

test("calculate-shipping supports grouped lookup from arrays of product ids", async () => {
  const previousCalculations = process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;
  process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON = JSON.stringify(calculations);

  try {
    const response = await calculateShipping(
      createRequest({
        rate: {
          destination: { country: "GB", postal_code: "SW1A 1AA" },
          currency: "GBP",
          items: [
            {
              product_id: 3002,
              quantity: 1,
              grams: 0,
              requires_shipping: true,
            },
          ],
        },
      })
    );

    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload?.rates?.[0]?.total_price, "1250");
    assert.equal(payload?.rates?.[0]?.service_code, "standard");
  } finally {
    if (previousCalculations == null) {
      delete process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;
    } else {
      process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON = previousCalculations;
    }
  }
});

test("calculate-shipping returns 503 when calculations JSON is missing", async () => {
  const previousCalculations = process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;
  delete process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;

  try {
    const response = await calculateShipping(
      createRequest({
        rate: {
          items: [],
        },
      })
    );

    assert.equal(response.status, 503);
    const payload = await response.json();
    assert.equal(payload.error, "invalid_shipping_configuration");
  } finally {
    if (previousCalculations == null) {
      delete process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON;
    } else {
      process.env.SHOPIFY_SHIPPING_CALCULATIONS_JSON = previousCalculations;
    }
  }
});
