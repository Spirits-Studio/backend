import { createShopifyWebhookOrdersHandler } from "./shopify-webhook-orders-create.js";

const shopifyWebhookOrdersPaid = createShopifyWebhookOrdersHandler({
  endpoint: "shopify-webhook-orders-paid",
  expectedTopic: "orders/paid",
  orderStatus: "Paid",
});

export default shopifyWebhookOrdersPaid;
