const TABLE_ID_ENV_KEYS = [
  "AIRTABLE_CUSTOMERS_TABLE_ID",
  "AIRTABLE_LABEL_VERSIONS_TABLE_ID",
  "AIRTABLE_LABELS_TABLE_ID",
  "AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID",
  "AIRTABLE_ORDERS_TABLE_ID",
  "AIRTABLE_SAVED_CONFIGS_TABLE_ID",
];

const REQUIRED_ENV_KEYS = ["AIRTABLE_BASE_ID", "AIRTABLE_TOKEN"];

const sendJson = (status, body, extraHeaders = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      ...extraHeaders,
    },
  });

const getMethod = (arg) =>
  String(arg?.method || arg?.httpMethod || "GET").toUpperCase();

const readEnv = (key) => String(process.env[key] || "").trim();

const getConfiguredTableIds = () =>
  Array.from(
    new Set(
      TABLE_ID_ENV_KEYS.map((key) => readEnv(key)).filter(Boolean)
    )
  );

const toSchemaTable = (table) => {
  const { id = "", name = "", ...schema } = table || {};
  return { id, name, schema };
};

const fetchBaseSchema = async ({ baseId, token }) => {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });

  const raw = await response.text();
  let json;
  try {
    json = raw ? JSON.parse(raw) : null;
  } catch {
    json = null;
  }

  if (!response.ok) {
    const err = new Error(
      `Airtable schema request failed with ${response.status} ${response.statusText}`
    );
    err.status = response.status;
    err.details = json || raw || null;
    throw err;
  }

  return Array.isArray(json?.tables) ? json.tables : [];
};

export default async (arg) => {
  const method = getMethod(arg);
  if (method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: { Allow: "GET, OPTIONS", "Cache-Control": "no-store" },
    });
  }
  if (method !== "GET") {
    return sendJson(405, { error: "method_not_allowed", message: "Use GET." }, { Allow: "GET, OPTIONS" });
  }

  const missingEnv = REQUIRED_ENV_KEYS.filter((key) => !readEnv(key));
  if (missingEnv.length) {
    return sendJson(500, {
      error: "missing_env",
      message: "Missing required Airtable environment variables.",
      missing: missingEnv,
    });
  }

  const baseId = readEnv("AIRTABLE_BASE_ID");
  const token = readEnv("AIRTABLE_TOKEN");
  const configuredTableIds = getConfiguredTableIds();

  try {
    const baseTables = await fetchBaseSchema({ baseId, token });
    const tableById = new Map(baseTables.map((table) => [table.id, table]));

    const selectedTables = configuredTableIds.length
      ? configuredTableIds.map((tableId) => tableById.get(tableId)).filter(Boolean)
      : baseTables;

    const missingTableIds = configuredTableIds.filter((tableId) => !tableById.has(tableId));
    if (missingTableIds.length) {
      return sendJson(502, {
        error: "table_ids_not_found",
        message: "Some configured Airtable table ids were not found in the base schema.",
        missingTableIds,
      });
    }

    return sendJson(200, {
      tables: selectedTables.map((table) => toSchemaTable(table)),
    });
  } catch (error) {
    console.error("[airtable-schema] failed", error);
    return sendJson(error?.status || 502, {
      error: "airtable_schema_fetch_failed",
      message: error?.message || "Failed to load Airtable schema.",
      details: error?.details || null,
    });
  }
};
