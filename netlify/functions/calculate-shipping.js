import { parseBody, sendJson } from "./_lib/studio.js";
import {
  buildCarrierRates,
  resolveShippingCalculations,
} from "./_lib/shopifyCarrierRates.js";

const SHIPPING_CALCULATIONS_ENV_KEY = "SHOPIFY_SHIPPING_CALCULATIONS_JSON";

const isV2Request = (arg) =>
  Boolean(arg && typeof arg.method === "string" && !("httpMethod" in arg));

const getMethod = (arg) =>
  String(arg?.method || arg?.httpMethod || "GET").toUpperCase();

const buildDebugSummary = ({ shipment, rate }) => ({
  destination: {
    country: rate?.destination?.country || null,
    province:
      rate?.destination?.province ??
      rate?.destination?.state ??
      null,
    postal_code:
      rate?.destination?.postal_code ??
      rate?.destination?.zip ??
      null,
  },
  currency: rate?.currency || null,
  grouped_units: shipment.groupedUnits,
  grouped_product_grams: shipment.groupedProductGrams,
  fallback_product_grams: shipment.fallbackProductGrams,
  packaging_grams: shipment.packagingGrams,
  catalogs: shipment.catalogs,
  total_grams: shipment.totalGrams,
});

export default async (arg) => {
  const method = getMethod(arg);

  if (method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        Allow: "POST, OPTIONS",
        "Cache-Control": "no-store",
      },
    });
  }

  if (method !== "POST") {
    return sendJson(405, {
      error: "method_not_allowed",
      message: "Use POST.",
    });
  }

  const requestBody =
    (await parseBody(arg, method, isV2Request(arg))) || {};
  const rate = requestBody?.rate;

  if (!rate || !Array.isArray(rate.items)) {
    return sendJson(400, {
      error: "invalid_rate_request",
      message: "Expected Shopify carrier-service payload with rate.items.",
    });
  }

  let calculations;
  try {
    calculations = resolveShippingCalculations(
      process.env[SHIPPING_CALCULATIONS_ENV_KEY]
    );
  } catch (error) {
    console.error("[calculate-shipping] invalid configuration", error);
    return sendJson(503, {
      error: "invalid_shipping_configuration",
      message: error?.message || "Shipping calculations are not configured.",
      env_key: SHIPPING_CALCULATIONS_ENV_KEY,
    });
  }

  try {
    const result = buildCarrierRates({ rate, calculations });
    const debugEnabled = ["1", "true", "yes"].includes(
      String(process.env.SHOPIFY_SHIPPING_DEBUG || "").trim().toLowerCase()
    );

    if (debugEnabled) {
      console.log(
        "[calculate-shipping] shipment",
        JSON.stringify(buildDebugSummary({ shipment: result.shipment, rate }))
      );
    }

    if (!result.shipment.hasShippableItems) {
      return sendJson(200, { rates: [] });
    }

    if (!result.rates.length) {
      return sendJson(404, {
        error: "no_matching_shipping_rate",
        message:
          "No configured shipping rate matched the calculated shipment weight.",
      });
    }

    return sendJson(200, { rates: result.rates });
  } catch (error) {
    console.error("[calculate-shipping] failed", error);
    return sendJson(502, {
      error: "shipping_calculation_failed",
      message: error?.message || "Failed to calculate shipping rates.",
    });
  }
};
