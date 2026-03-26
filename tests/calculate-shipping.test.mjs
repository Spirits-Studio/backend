import test from "node:test";
import assert from "node:assert/strict";

const { default: calculateShipping } = await import(
  "../netlify/functions/calculate-shipping.js"
);

const createRequest = (body, method = "POST") =>
  new Request(
    "https://example.netlify.app/.netlify/functions/calculate-shipping",
    {
      method,
      headers: { "Content-Type": "application/json" },
      body: method === "POST" ? JSON.stringify(body) : undefined,
    }
  );

const calculations = {
  packagingTypes: {
    single_box: 500,
    six_box: 400,
    twelve_box: 700,
    pallet: 3000,
  },
  carrierRates: [
    {
      carrierName: "Street Wise",
      carrierCode: "SW",
      description: "Street Wise standard shipping",
      currency: "GBP",
      rateTable: [
        { maxGrams: 2000, totalPrice: 5.99 },
        { maxGrams: 10000, totalPrice: 12.5 },
        { maxGrams: 15000, totalPrice: 18.99 },
      ],
    },
    {
      carrierName: "Parcel Fast",
      carrierCode: "PF",
      description: "Parcel Fast economy shipping",
      currency: "GBP",
      rateTable: [
        { maxGrams: 2000, totalPrice: 4.99 },
        { maxGrams: 10000, totalPrice: 11.99 },
        { maxGrams: 15000, totalPrice: 17.49 },
      ],
    },
  ],
  calculations: [
    {
      bottles: [
        {
          id: 1001,
          name: "Build Your Brand",
          weight: 1250,
        },
        {
          id: 1002,
          name: "Build Your Gin Brand",
          weight: 1250,
        },
      ],
      rates: [
        { quantity: 1, packaging: { single_box: 1 } },
        { quantity: 2, packaging: { single_box: 2 } },
        { quantity: 6, packaging: { six_box: 1 } },
        { quantity: 7, packaging: { six_box: 1, single_box: 1 } },
        { quantity: 12, packaging: { twelve_box: 1 } },
      ],
    },
    {
      samples: [
        {
          id: 2001,
          name: "Build Your Brand Sample",
          weight: 1500,
        },
      ],
      rates: [
        { quantity: 1, packaging: { single_box: 1 } },
        { quantity: 2, packaging: { single_box: 2 } },
      ],
    },
  ],
};

test("calculate-shipping parses catalog packaging rules and returns all matching carrier services", async () => {
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
              product_id: 1001,
              quantity: 7,
              grams: 0,
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
          service_name: "Street Wise",
          service_code: "SW",
          description: "Street Wise standard shipping",
          total_price: "1250",
          currency: "GBP",
        },
        {
          service_name: "Parcel Fast",
          service_code: "PF",
          description: "Parcel Fast economy shipping",
          total_price: "1199",
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

test("calculate-shipping combines multiple catalogs and fallback line weights", async () => {
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
              product_id: 1002,
              quantity: 6,
              grams: 0,
              requires_shipping: true,
            },
            {
              product_id: 2001,
              quantity: 1,
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
          service_name: "Street Wise",
          service_code: "SW",
          description: "Street Wise standard shipping",
          total_price: "1899",
          currency: "GBP",
        },
        {
          service_name: "Parcel Fast",
          service_code: "PF",
          description: "Parcel Fast economy shipping",
          total_price: "1749",
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

test("calculate-shipping fails when a catalog quantity has no packaging rule", async () => {
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
              product_id: 2001,
              quantity: 3,
              grams: 0,
              requires_shipping: true,
            },
          ],
        },
      })
    );

    assert.equal(response.status, 502);
    const payload = await response.json();
    assert.equal(payload.error, "shipping_calculation_failed");
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
