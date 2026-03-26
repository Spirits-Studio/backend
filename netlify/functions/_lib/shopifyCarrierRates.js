const DEFAULT_BOTTLE_WEIGHT_GRAMS = 1250;
const DEFAULT_SERVICE_NAME = "Shipping";
const DEFAULT_SERVICE_CODE = "shipping";
const DEFAULT_DESCRIPTION = "Carrier-calculated shipping.";

const packagingCache = new Map();

const normalizeText = (value) => {
  if (value == null) return null;
  const text = String(value).trim();
  return text || null;
};

const normalizeId = (value) => {
  const text = normalizeText(value);
  return text ? text.replace(/\.0+$/, "") : null;
};

const toInteger = (value) => {
  if (value == null || value === "") return null;
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  return Math.trunc(number);
};

const toPositiveInteger = (value) => {
  const number = toInteger(value);
  return number && number > 0 ? number : null;
};

const toNonNegativeInteger = (value) => {
  const number = toInteger(value);
  return number != null && number >= 0 ? number : null;
};

const createConfigError = (message) => {
  const error = new Error(message);
  error.code = "invalid_shipping_calculations";
  return error;
};

const slugify = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const sortAscendingByMaxGrams = (left, right) => left.maxGrams - right.maxGrams;

const normalizeRateTable = (value) => {
  if (!Array.isArray(value)) return [];

  return value
    .map((entry) => {
      const maxGrams = toPositiveInteger(
        entry?.maxGrams ?? entry?.upToGrams ?? entry?.weightToGrams
      );
      const totalPrice = toNonNegativeInteger(
        entry?.totalPrice ?? entry?.price ?? entry?.amountSubunits
      );
      if (!maxGrams || totalPrice == null) return null;
      return { maxGrams, totalPrice };
    })
    .filter(Boolean)
    .sort(sortAscendingByMaxGrams);
};

const normalizeQuantityGroups = ({
  rawGroups,
  fallbackBottleWeightGrams,
}) => {
  const productIdToGroup = new Map();
  const quantityGroups = [];

  for (const rawGroup of rawGroups) {
    const quantity = toPositiveInteger(
      rawGroup?.quantity ?? rawGroup?.units ?? rawGroup?.packSize
    );
    const unitWeightGrams =
      toPositiveInteger(rawGroup?.unitWeightGrams) || fallbackBottleWeightGrams;
    const productIds = Array.isArray(rawGroup?.productIds)
      ? rawGroup.productIds.map(normalizeId).filter(Boolean)
      : [];

    if (!quantity || !productIds.length) continue;

    const group = {
      quantity,
      unitWeightGrams,
      productIds,
    };

    for (const productId of productIds) {
      if (productIdToGroup.has(productId)) {
        throw createConfigError(
          `Duplicate product id "${productId}" in SHOPIFY_SHIPPING_CALCULATIONS_JSON quantityGroups.`
        );
      }
      productIdToGroup.set(productId, group);
    }

    quantityGroups.push(group);
  }

  return { quantityGroups, productIdToGroup };
};

const normalizePackagingWeights = (value) => {
  if (!Array.isArray(value)) return [];

  return value
    .map((entry) => {
      const units = toPositiveInteger(
        entry?.units ?? entry?.quantity ?? entry?.packSize
      );
      const grams = toPositiveInteger(entry?.grams ?? entry?.weightGrams);
      if (!units || !grams) return null;
      return { units, grams };
    })
    .filter(Boolean)
    .sort((left, right) => left.units - right.units);
};

const normalizeServices = (source) => {
  const rawServices =
    Array.isArray(source?.services) && source.services.length
      ? source.services
      : [source];

  const fallbackCurrency = normalizeText(source?.currency) || "GBP";
  const fallbackCarrierName = normalizeText(source?.carrierName);
  const services = [];

  for (const rawService of rawServices) {
    const rateTable = normalizeRateTable(
      rawService?.rateTable ??
        rawService?.rates ??
        (rawService === source ? source?.rateTable ?? source?.rates : null)
    );
    if (!rateTable.length) continue;

    const serviceName =
      normalizeText(rawService?.serviceName ?? rawService?.name) ||
      DEFAULT_SERVICE_NAME;
    const serviceCode =
      normalizeText(rawService?.serviceCode ?? rawService?.code) ||
      slugify(serviceName) ||
      DEFAULT_SERVICE_CODE;
    const description =
      normalizeText(rawService?.description) ||
      (fallbackCarrierName
        ? `Rates via ${fallbackCarrierName}.`
        : DEFAULT_DESCRIPTION);
    const currency =
      normalizeText(rawService?.currency) || fallbackCurrency;
    const phoneRequired = rawService?.phoneRequired === true;

    services.push({
      serviceName,
      serviceCode,
      description,
      currency,
      phoneRequired,
      rateTable,
    });
  }

  return services;
};

export const resolveShippingCalculations = (rawValue) => {
  const rawText =
    typeof rawValue === "string" ? rawValue.trim() : rawValue;

  if (!rawText) {
    throw createConfigError(
      "Missing SHOPIFY_SHIPPING_CALCULATIONS_JSON."
    );
  }

  let parsed;
  try {
    parsed = typeof rawText === "string" ? JSON.parse(rawText) : rawText;
  } catch (error) {
    throw createConfigError(
      `SHOPIFY_SHIPPING_CALCULATIONS_JSON is not valid JSON: ${error.message}`
    );
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must be a JSON object."
    );
  }

  const bottleWeightGrams =
    toPositiveInteger(parsed?.bottleWeightGrams) ||
    DEFAULT_BOTTLE_WEIGHT_GRAMS;
  const { quantityGroups, productIdToGroup } = normalizeQuantityGroups({
    rawGroups:
      parsed?.quantityGroups ??
      parsed?.products ??
      parsed?.productGroups ??
      [],
    fallbackBottleWeightGrams: bottleWeightGrams,
  });
  const packagingWeights = normalizePackagingWeights(
    parsed?.packagingWeights ?? parsed?.packaging ?? []
  );
  const services = normalizeServices(parsed);

  if (!quantityGroups.length) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must include quantityGroups with productIds."
    );
  }
  if (!packagingWeights.length) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must include packagingWeights."
    );
  }
  if (!services.length) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must include at least one rate table."
    );
  }

  return {
    bottleWeightGrams,
    quantityGroups,
    productIdToGroup,
    packagingWeights,
    services,
  };
};

const isBetterExactCandidate = (candidate, current) => {
  if (!current) return true;
  if (candidate.grams !== current.grams) return candidate.grams < current.grams;
  return candidate.packageCount < current.packageCount;
};

const createPackagingCacheKey = (totalUnits, packagingWeights) =>
  `${totalUnits}:${packagingWeights
    .map((entry) => `${entry.units}:${entry.grams}`)
    .join(",")}`;

export const calculatePackaging = (totalUnits, packagingWeights) => {
  if (!totalUnits) {
    return {
      grams: 0,
      coveredUnits: 0,
      packages: [],
    };
  }

  const cacheKey = createPackagingCacheKey(totalUnits, packagingWeights);
  const cached = packagingCache.get(cacheKey);
  if (cached) return cached;

  const maxPackageUnits = packagingWeights.reduce(
    (current, entry) => Math.max(current, entry.units),
    0
  );
  const limit = totalUnits + maxPackageUnits;
  const dp = new Array(limit + 1).fill(null);
  dp[0] = {
    grams: 0,
    packageCount: 0,
    counts: new Array(packagingWeights.length).fill(0),
  };

  for (let coveredUnits = 1; coveredUnits <= limit; coveredUnits += 1) {
    let best = null;

    for (let index = 0; index < packagingWeights.length; index += 1) {
      const packaging = packagingWeights[index];
      const previous = coveredUnits - packaging.units;
      if (previous < 0 || !dp[previous]) continue;

      const counts = dp[previous].counts.slice();
      counts[index] += 1;

      const candidate = {
        grams: dp[previous].grams + packaging.grams,
        packageCount: dp[previous].packageCount + 1,
        counts,
      };

      if (isBetterExactCandidate(candidate, best)) {
        best = candidate;
      }
    }

    dp[coveredUnits] = best;
  }

  let bestCoverage = null;
  let bestState = null;
  for (let coveredUnits = totalUnits; coveredUnits <= limit; coveredUnits += 1) {
    const state = dp[coveredUnits];
    if (!state) continue;

    const isBetter =
      !bestState ||
      state.grams < bestState.grams ||
      (state.grams === bestState.grams && coveredUnits < bestCoverage) ||
      (state.grams === bestState.grams &&
        coveredUnits === bestCoverage &&
        state.packageCount < bestState.packageCount);

    if (isBetter) {
      bestCoverage = coveredUnits;
      bestState = state;
    }
  }

  if (!bestState) {
    throw new Error("Unable to resolve packaging for the requested quantity.");
  }

  const result = {
    grams: bestState.grams,
    coveredUnits: bestCoverage,
    packages: packagingWeights
      .map((packaging, index) => {
        const count = bestState.counts[index];
        if (!count) return null;
        return {
          units: packaging.units,
          grams: packaging.grams,
          count,
        };
      })
      .filter(Boolean)
      .sort((left, right) => right.units - left.units),
  };

  packagingCache.set(cacheKey, result);
  return result;
};

const findQuantityGroup = (item, calculations) => {
  const productId = normalizeId(item?.product_id ?? item?.productId);
  if (!productId) return null;
  return calculations.productIdToGroup.get(productId) || null;
};

const calculateMatchedItemWeight = (item, group, lineQuantity) => {
  const unitsForLine = group.quantity * lineQuantity;
  return {
    unitsForLine,
    gramsForLine: unitsForLine * group.unitWeightGrams,
  };
};

export const calculateShipment = ({ rate, calculations }) => {
  const items = Array.isArray(rate?.items) ? rate.items : [];

  let groupedUnits = 0;
  let groupedProductGrams = 0;
  let fallbackProductGrams = 0;
  let hasShippableItems = false;

  for (const item of items) {
    if (item?.requires_shipping === false) continue;

    const lineQuantity = toPositiveInteger(item?.quantity);
    if (!lineQuantity) continue;

    hasShippableItems = true;

    const quantityGroup = findQuantityGroup(item, calculations);
    if (quantityGroup) {
      const matched = calculateMatchedItemWeight(
        item,
        quantityGroup,
        lineQuantity
      );
      groupedUnits += matched.unitsForLine;
      groupedProductGrams += matched.gramsForLine;
      continue;
    }

    const gramsPerItem = toNonNegativeInteger(item?.grams) || 0;
    fallbackProductGrams += gramsPerItem * lineQuantity;
  }

  const packaging = calculatePackaging(groupedUnits, calculations.packagingWeights);
  const totalGrams =
    groupedProductGrams + fallbackProductGrams + packaging.grams;

  return {
    hasShippableItems,
    groupedUnits,
    groupedProductGrams,
    fallbackProductGrams,
    packaging,
    totalGrams,
  };
};

const findRateBracket = (service, totalGrams) =>
  service.rateTable.find((entry) => totalGrams <= entry.maxGrams) || null;

export const buildCarrierRates = ({ rate, calculations }) => {
  const shipment = calculateShipment({ rate, calculations });

  const rates = calculations.services
    .map((service) => {
      const matchedBracket = findRateBracket(service, shipment.totalGrams);
      if (!matchedBracket) return null;

      const response = {
        service_name: service.serviceName,
        service_code: service.serviceCode,
        description: service.description,
        total_price: String(matchedBracket.totalPrice),
        currency: service.currency,
      };

      if (service.phoneRequired) {
        response.phone_required = true;
      }

      return response;
    })
    .filter(Boolean);

  return { rates, shipment };
};
