import { createShopifyWebhookCustomersHandler } from "./_lib/shopifyWebhookCustomers.js";

const shopifyWebhookCustomersUpdate = createShopifyWebhookCustomersHandler({
  endpoint: "shopify-webhook-customers-update",
  expectedTopic: "customers/update",
});

export default shopifyWebhookCustomersUpdate;
