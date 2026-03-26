const DEFAULT_SERVICE_NAME = "Shipping";
const DEFAULT_SERVICE_CODE = "shipping";
const DEFAULT_DESCRIPTION = "Carrier-calculated shipping.";
const DEFAULT_CURRENCY = "GBP";

const RESERVED_CATALOG_KEYS = new Set([
  "name",
  "type",
  "catalog",
  "products",
  "items",
  "rates",
  "packagingByQuantity",
  "packaging_by_quantity",
  "packagingRules",
]);

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

const toMoneySubunits = (value) => {
  if (value == null || value === "") return null;
  const normalized =
    typeof value === "string"
      ? value.replace(/[^0-9.-]/g, "")
      : value;
  const number = Number(normalized);
  if (!Number.isFinite(number)) return null;
  return Math.round(number * 100);
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

const normalizePackagingTypes = (value) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must include packagingTypes as an object."
    );
  }

  const packagingTypes = {};

  for (const [key, rawWeight] of Object.entries(value)) {
    const name = normalizeText(key);
    const grams = toPositiveInteger(rawWeight);
    if (!name || !grams) continue;
    packagingTypes[name] = grams;
  }

  if (!Object.keys(packagingTypes).length) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON packagingTypes must include at least one package."
    );
  }

  return packagingTypes;
};

const normalizePackagingRecipe = (value, packagingTypes, contextLabel) => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw createConfigError(`${contextLabel} must include a packaging object.`);
  }

  const packaging = {};

  for (const [key, rawCount] of Object.entries(value)) {
    const packageName = normalizeText(key);
    const count = toNonNegativeInteger(rawCount);
    if (!packageName || !count) continue;

    if (!Object.hasOwn(packagingTypes, packageName)) {
      throw createConfigError(
        `${contextLabel} references unknown packaging type "${packageName}".`
      );
    }

    packaging[packageName] = count;
  }

  if (!Object.keys(packaging).length) {
    throw createConfigError(`${contextLabel} must define at least one package count.`);
  }

  return packaging;
};

const normalizePackagingByQuantity = (value, packagingTypes, contextLabel) => {
  const packagingByQuantity = new Map();

  if (Array.isArray(value)) {
    for (const entry of value) {
      const quantity = toPositiveInteger(entry?.quantity);
      if (!quantity) continue;

      const packaging = normalizePackagingRecipe(
        entry?.packaging,
        packagingTypes,
        `${contextLabel} quantity ${quantity}`
      );
      packagingByQuantity.set(String(quantity), packaging);
    }
  } else if (value && typeof value === "object") {
    for (const [rawQuantity, rawPackaging] of Object.entries(value)) {
      const quantity = toPositiveInteger(rawQuantity);
      if (!quantity) continue;

      const packaging = normalizePackagingRecipe(
        rawPackaging,
        packagingTypes,
        `${contextLabel} quantity ${quantity}`
      );
      packagingByQuantity.set(String(quantity), packaging);
    }
  }

  if (!packagingByQuantity.size) {
    throw createConfigError(
      `${contextLabel} must include quantity-based packaging rules.`
    );
  }

  return packagingByQuantity;
};

const normalizeRateTable = (value, contextLabel) => {
  if (!Array.isArray(value)) return [];

  return value
    .map((entry) => {
      const maxGrams = toPositiveInteger(
        entry?.maxGrams ?? entry?.upToGrams ?? entry?.weightToGrams
      );
      const totalPriceSubunits =
        toNonNegativeInteger(
          entry?.totalPriceSubunits ??
            entry?.priceSubunits ??
            entry?.amountSubunits
        ) ??
        toMoneySubunits(entry?.totalPrice ?? entry?.price ?? entry?.amount);

      if (!maxGrams || totalPriceSubunits == null) return null;
      return { maxGrams, totalPriceSubunits };
    })
    .filter(Boolean)
    .sort(sortAscendingByMaxGrams);
};

const normalizeCarrierRates = (value) => {
  if (!Array.isArray(value) || !value.length) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must include carrierRates."
    );
  }

  const carrierRates = [];

  for (const entry of value) {
    const carrierName =
      normalizeText(entry?.carrierName ?? entry?.name ?? entry?.serviceName) ||
      DEFAULT_SERVICE_NAME;
    const carrierCode =
      normalizeText(entry?.carrierCode ?? entry?.code ?? entry?.serviceCode) ||
      slugify(carrierName) ||
      DEFAULT_SERVICE_CODE;
    const serviceName =
      normalizeText(entry?.serviceName) || carrierName;
    const serviceCode =
      normalizeText(entry?.serviceCode) || carrierCode;
    const description =
      normalizeText(entry?.description) ||
      (carrierName ? `Rates via ${carrierName}.` : DEFAULT_DESCRIPTION);
    const currency =
      normalizeText(entry?.currency) || DEFAULT_CURRENCY;
    const phoneRequired = entry?.phoneRequired === true;
    const rateTable = normalizeRateTable(
      entry?.rateTable ?? entry?.rates,
      carrierName
    );

    if (!rateTable.length) {
      throw createConfigError(
        `carrierRates entry "${carrierName}" must include a rateTable.`
      );
    }

    carrierRates.push({
      carrierName,
      carrierCode,
      serviceName,
      serviceCode,
      description,
      currency,
      phoneRequired,
      rateTable,
    });
  }

  return carrierRates;
};

const extractCatalogEntries = (value) => {
  if (!value) return [];

  if (!Array.isArray(value) && typeof value === "object") {
    return Object.entries(value).map(([catalogName, config]) => ({
      catalogName,
      config,
    }));
  }

  if (!Array.isArray(value)) return [];

  const entries = [];

  for (const rawEntry of value) {
    if (!rawEntry || typeof rawEntry !== "object" || Array.isArray(rawEntry)) {
      continue;
    }

    const explicitCatalogName = normalizeText(
      rawEntry?.name ?? rawEntry?.type ?? rawEntry?.catalog
    );
    const explicitProducts = Array.isArray(rawEntry?.products)
      ? rawEntry.products
      : Array.isArray(rawEntry?.items)
        ? rawEntry.items
        : null;

    if (explicitCatalogName && explicitProducts) {
      entries.push({
        catalogName: explicitCatalogName,
        config: { ...rawEntry, products: explicitProducts },
      });
      continue;
    }

    const candidateKeys = Object.keys(rawEntry).filter(
      (key) => !RESERVED_CATALOG_KEYS.has(key)
    );

    if (candidateKeys.length === 1) {
      const candidateKey = candidateKeys[0];
      const candidateValue = rawEntry[candidateKey];

      if (Array.isArray(candidateValue)) {
        entries.push({
          catalogName: candidateKey,
          config: { ...rawEntry, products: candidateValue },
        });
        continue;
      }

      if (candidateValue && typeof candidateValue === "object" && !Array.isArray(candidateValue)) {
        entries.push({
          catalogName: candidateKey,
          config: candidateValue,
        });
      }
    }
  }

  return entries;
};

const normalizeProducts = (value, catalogLabel) => {
  if (!Array.isArray(value) || !value.length) {
    throw createConfigError(`${catalogLabel} must include a product list.`);
  }

  return value
    .map((entry) => {
      const id = normalizeId(entry?.id ?? entry?.productId);
      const name = normalizeText(entry?.name ?? entry?.title);
      const unitWeightGrams = toPositiveInteger(
        entry?.unitWeightGrams ?? entry?.weightGrams ?? entry?.weight ?? entry?.grams
      );

      if (!id || !unitWeightGrams) return null;

      return {
        id,
        name: name || id,
        unitWeightGrams,
      };
    })
    .filter(Boolean);
};

const normalizeCatalogs = (value, packagingTypes) => {
  const rawCatalogs = extractCatalogEntries(value);
  if (!rawCatalogs.length) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON must include calculations."
    );
  }

  const catalogs = new Map();
  const productIdToCatalog = new Map();

  for (const { catalogName: rawCatalogName, config } of rawCatalogs) {
    const catalogName = normalizeText(rawCatalogName);
    if (!catalogName) continue;

    const products = normalizeProducts(
      config?.products ?? config?.items,
      `calculations.${catalogName}`
    );
    const packagingByQuantity = normalizePackagingByQuantity(
      config?.packagingByQuantity ??
        config?.packaging_by_quantity ??
        config?.rates ??
        config?.packagingRules,
      packagingTypes,
      `calculations.${catalogName}`
    );

    const catalog = {
      name: catalogName,
      products,
      packagingByQuantity,
    };

    catalogs.set(catalogName, catalog);

    for (const product of products) {
      if (productIdToCatalog.has(product.id)) {
        throw createConfigError(
          `Duplicate product id "${product.id}" found across calculations.`
        );
      }

      productIdToCatalog.set(product.id, {
        catalogName,
        product,
      });
    }
  }

  if (!catalogs.size) {
    throw createConfigError(
      "SHOPIFY_SHIPPING_CALCULATIONS_JSON calculations did not produce any valid catalogs."
    );
  }

  return {
    catalogs,
    productIdToCatalog,
  };
};

const computePackagingGrams = (packaging, packagingTypes) =>
  Object.entries(packaging).reduce((total, [packageName, count]) => {
    const grams = packagingTypes[packageName] || 0;
    return total + grams * count;
  }, 0);

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

  const packagingTypes = normalizePackagingTypes(
    parsed?.packagingTypes ?? parsed?.packaging_types
  );
  const carrierRates = normalizeCarrierRates(
    parsed?.carrierRates ?? parsed?.carriers
  );
  const { catalogs, productIdToCatalog } = normalizeCatalogs(
    parsed?.calculations ?? parsed?.catalogs,
    packagingTypes
  );

  return {
    packagingTypes,
    carrierRates,
    catalogs,
    productIdToCatalog,
  };
};

export const calculateShipment = ({ rate, calculations }) => {
  const items = Array.isArray(rate?.items) ? rate.items : [];
  const catalogTotals = new Map();

  let hasShippableItems = false;
  let groupedUnits = 0;
  let groupedProductGrams = 0;
  let fallbackProductGrams = 0;
  let packagingGrams = 0;

  for (const item of items) {
    if (item?.requires_shipping === false) continue;

    const lineQuantity = toPositiveInteger(item?.quantity);
    if (!lineQuantity) continue;

    hasShippableItems = true;

    const productId = normalizeId(item?.product_id ?? item?.productId);
    const match = productId ? calculations.productIdToCatalog.get(productId) : null;

    if (!match) {
      const gramsPerItem = toNonNegativeInteger(item?.grams) || 0;
      fallbackProductGrams += gramsPerItem * lineQuantity;
      continue;
    }

    const key = match.catalogName;
    const current = catalogTotals.get(key) || {
      catalogName: key,
      quantity: 0,
      productGrams: 0,
    };

    current.quantity += lineQuantity;
    current.productGrams += match.product.unitWeightGrams * lineQuantity;
    catalogTotals.set(key, current);
  }

  const catalogs = [];

  for (const catalogTotal of catalogTotals.values()) {
    const catalogConfig = calculations.catalogs.get(catalogTotal.catalogName);
    const packaging =
      catalogConfig?.packagingByQuantity.get(String(catalogTotal.quantity)) || null;

    if (!packaging) {
      throw new Error(
        `No packaging rule configured for catalog "${catalogTotal.catalogName}" quantity ${catalogTotal.quantity}.`
      );
    }

    const catalogPackagingGrams = computePackagingGrams(
      packaging,
      calculations.packagingTypes
    );

    groupedUnits += catalogTotal.quantity;
    groupedProductGrams += catalogTotal.productGrams;
    packagingGrams += catalogPackagingGrams;

    catalogs.push({
      catalog: catalogTotal.catalogName,
      quantity: catalogTotal.quantity,
      productGrams: catalogTotal.productGrams,
      packaging,
      packagingGrams: catalogPackagingGrams,
    });
  }

  const totalGrams =
    groupedProductGrams + fallbackProductGrams + packagingGrams;

  return {
    hasShippableItems,
    groupedUnits,
    groupedProductGrams,
    fallbackProductGrams,
    packagingGrams,
    catalogs,
    totalGrams,
  };
};

const findRateBracket = (carrierRate, totalGrams) =>
  carrierRate.rateTable.find((entry) => totalGrams <= entry.maxGrams) || null;

export const buildCarrierRates = ({ rate, calculations }) => {
  const shipment = calculateShipment({ rate, calculations });

  const rates = calculations.carrierRates
    .map((carrierRate) => {
      const matchedBracket = findRateBracket(carrierRate, shipment.totalGrams);
      if (!matchedBracket) return null;

      const response = {
        service_name: carrierRate.serviceName,
        service_code: carrierRate.serviceCode,
        description: carrierRate.description,
        total_price: String(matchedBracket.totalPriceSubunits),
        currency: carrierRate.currency,
      };

      if (carrierRate.phoneRequired) {
        response.phone_required = true;
      }

      return response;
    })
    .filter(Boolean);

  return { rates, shipment };
};
