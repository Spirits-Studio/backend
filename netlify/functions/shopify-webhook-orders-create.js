import crypto from "crypto";
import {
  STUDIO_TABLES,
  STUDIO_FIELDS,
  normalizeRecordId,
  createResilient,
  updateResilient,
  getRecordOrNull,
} from "./_lib/studio.js";

function verifyHmac(req, rawBody) {
  const digest = crypto
    .createHmac("sha256", process.env.SHOPIFY_WEBHOOK_SECRET)
    .update(rawBody, "utf8")
    .digest("base64");
  return req.headers["x-shopify-hmac-sha256"] === digest;
}

const getLineItemProperties = (lineItem) =>
  (lineItem?.properties || []).reduce((acc, pair) => {
    if (pair?.name) acc[pair.name] = pair.value;
    return acc;
  }, {});

export default async (req, res) => {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";

  if (!verifyHmac(req, raw)) {
    res.status(401).end("invalid hmac");
    return;
  }

  const order = JSON.parse(raw);
  const orderId = String(order?.id || "");
  const userEmail = order?.email || order?.customer?.email || null;

  const touchedConfigIds = [];

  for (const lineItem of order?.line_items || []) {
    const props = getLineItemProperties(lineItem);
    const savedConfigurationRecordId = normalizeRecordId(
      props["_saved_configuration_id"] || props["Config ID"] || ""
    );
    if (!savedConfigurationRecordId) continue;

    touchedConfigIds.push(savedConfigurationRecordId);

    const orderRecord = await createResilient(
      STUDIO_TABLES.orders,
      {},
      {
        orderId,
        shopifyOrderId: orderId,
        email: userEmail || undefined,
        configId: savedConfigurationRecordId,
        totalPrice: order?.total_price || undefined,
        createdAt: new Date().toISOString(),
        "Saved Configuration": [savedConfigurationRecordId],
        savedConfigurationId: savedConfigurationRecordId,
      }
    );

    const savedConfig = await getRecordOrNull(
      STUDIO_TABLES.savedConfigurations,
      savedConfigurationRecordId
    );
    if (!savedConfig) continue;

    await updateResilient(
      STUDIO_TABLES.savedConfigurations,
      savedConfigurationRecordId,
      {},
      {
        [STUDIO_FIELDS.savedConfigurations.status]: "Ordered",
        [STUDIO_FIELDS.savedConfigurations.order]: orderRecord?.id
          ? [orderRecord.id]
          : undefined,
      }
    );
  }

  res.status(200).json({
    ok: true,
    order_id: orderId,
    updated_saved_configurations: touchedConfigIds,
  });
};
