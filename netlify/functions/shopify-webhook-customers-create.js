import { createShopifyWebhookCustomersHandler } from "./_lib/shopifyWebhookCustomers.js";

const shopifyWebhookCustomersCreate = createShopifyWebhookCustomersHandler({
  endpoint: "shopify-webhook-customers-create",
  expectedTopic: "customers/create",
});

export default shopifyWebhookCustomersCreate;
