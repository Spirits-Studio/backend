import {
  listShopifyCarrierServices,
  upsertShopifyCarrierService,
} from "../netlify/functions/_lib/shopifyAdmin.js";

const normalizeText = (value) => {
  if (value == null) return null;
  const text = String(value).trim();
  return text || null;
};

const parseArgs = (argv) => {
  const args = {
    callbackUrl: null,
    name: null,
    active: null,
    supportsServiceDiscovery: null,
    listOnly: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--callback-url") {
      args.callbackUrl = argv[index + 1] || null;
      index += 1;
      continue;
    }
    if (token === "--name") {
      args.name = argv[index + 1] || null;
      index += 1;
      continue;
    }
    if (token === "--active") {
      args.active = argv[index + 1] || null;
      index += 1;
      continue;
    }
    if (token === "--supports-service-discovery") {
      args.supportsServiceDiscovery = argv[index + 1] || null;
      index += 1;
      continue;
    }
    if (token === "--list") {
      args.listOnly = true;
    }
  }

  return args;
};

const parseBoolean = (value, fallback) => {
  const text = normalizeText(value);
  if (!text) return fallback;
  if (["1", "true", "yes", "on"].includes(text.toLowerCase())) return true;
  if (["0", "false", "no", "off"].includes(text.toLowerCase())) return false;
  return fallback;
};

const args = parseArgs(process.argv.slice(2));
const shopDomain = normalizeText(process.env.SHOPIFY_STORE_DOMAIN);
const callbackUrl =
  normalizeText(args.callbackUrl) ||
  normalizeText(process.env.SHOPIFY_CARRIER_SERVICE_CALLBACK_URL);
const carrierServiceName =
  normalizeText(args.name) ||
  normalizeText(process.env.SHOPIFY_CARRIER_SERVICE_NAME) ||
  "Spirits Studio Shipping";
const active = parseBoolean(
  args.active ?? process.env.SHOPIFY_CARRIER_SERVICE_ACTIVE,
  true
);
const supportsServiceDiscovery = parseBoolean(
  args.supportsServiceDiscovery ??
    process.env.SHOPIFY_CARRIER_SERVICE_SUPPORTS_SERVICE_DISCOVERY,
  true
);

if (!shopDomain) {
  console.error("Missing SHOPIFY_STORE_DOMAIN.");
  process.exitCode = 1;
} else if (args.listOnly) {
  const result = await listShopifyCarrierServices({ shopDomain });
  console.log(
    JSON.stringify(
      {
        ok: true,
        shopDomain,
        services: result.services,
      },
      null,
      2
    )
  );
} else if (!callbackUrl) {
  console.error(
    "Missing callback URL. Set SHOPIFY_CARRIER_SERVICE_CALLBACK_URL or pass --callback-url."
  );
  process.exitCode = 1;
} else {
  const result = await upsertShopifyCarrierService({
    shopDomain,
    name: carrierServiceName,
    callbackUrl,
    active,
    supportsServiceDiscovery,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        action: result.action,
        shopDomain,
        carrierService: result.carrierService,
      },
      null,
      2
    )
  );
}
