const base = () => `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}`;
const auth = { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` };

const encodePath = (path) => {
  if (Array.isArray(path)) {
    return path.map((part) => encodeURIComponent(String(part))).join("/");
  }
  return encodeURIComponent(String(path));
};

export const escapeFormulaValue = (value) =>
  String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'");

/**
 * Internal helper to call Airtable and surface verbose errors.
 * Returns parsed JSON on success, otherwise throws an Error
 * with status, url, method and responseText for browser logging.
 */
async function airtableRequest(path, { method = "GET", headers = {}, body } = {}) {
  const url = `${base()}/${encodePath(path)}`;
  const response = await fetch(url, {
    method,
    headers: { ...auth, ...headers },
    body
  });

  const responseText = await response.text();
  let json;
  try { json = responseText ? JSON.parse(responseText) : null; } catch { json = null; }

  if (!response.ok) {
    const err = new Error(
      `Airtable ${method} ${url} failed with ${response.status} ${response.statusText}: ${responseText}`
    );
    err.status = response.status;
    err.url = url;
    err.method = method;
    err.responseText = responseText;
    err.requestBody = body;
    throw err;
  }

  return json;
}

/**
 * Find the first record in `table` where {field} == value
 * Returns the record object or null.
 */
export async function findOneBy(table, field, value) {
  try {
    console.log("[airtable] findOneBy", { table, field, value });

    const url = new URL(`${base()}/${encodePath(table)}`);
    url.searchParams.set("maxRecords", "1");
    // Escape single quotes in the value for Airtable formula syntax
    const safe = escapeFormulaValue(value);
    url.searchParams.set("filterByFormula", `({${field}}='${safe}')`);

    const response = await fetch(url, { headers: auth });
    const json = await response.json();

    const record = json.records?.[0] || null;
    console.log("[airtable] findOneBy result", { found: !!record, id: record?.id });
    return record;
  } catch (err) {
    // Ensure verbose error bubbles up to the caller (and browser)
    console.error("[airtable] findOneBy error", err);
    throw err;
  }
}

/**
 * Get a record by Airtable record id.
 */
export async function getOne(table, recordId) {
  try {
    console.log("[airtable] getOne", { table, recordId });
    const record = await airtableRequest([table, recordId], { method: "GET" });
    return record || null;
  } catch (err) {
    if (err?.status === 404) return null;
    console.error("[airtable] getOne error", err);
    throw err;
  }
}

/**
 * List records from a table, with optional formula/query modifiers.
 * Returns the raw Airtable payload: { records, offset }.
 */
export async function listRecords(
  table,
  { filterByFormula, maxRecords, pageSize, sort, fields, offset, view } = {}
) {
  try {
    const url = new URL(`${base()}/${encodePath(table)}`);
    if (filterByFormula) url.searchParams.set("filterByFormula", String(filterByFormula));
    if (maxRecords != null) url.searchParams.set("maxRecords", String(maxRecords));
    if (pageSize != null) url.searchParams.set("pageSize", String(pageSize));
    if (offset) url.searchParams.set("offset", String(offset));
    if (view) url.searchParams.set("view", String(view));

    if (Array.isArray(fields)) {
      fields.forEach((field) => {
        if (field) url.searchParams.append("fields[]", String(field));
      });
    }

    if (Array.isArray(sort)) {
      sort.forEach((entry, idx) => {
        if (!entry?.field) return;
        url.searchParams.set(`sort[${idx}][field]`, String(entry.field));
        if (entry.direction) {
          url.searchParams.set(`sort[${idx}][direction]`, String(entry.direction));
        }
      });
    }

    const response = await fetch(url, { headers: auth });
    const responseText = await response.text();
    let json;
    try {
      json = responseText ? JSON.parse(responseText) : {};
    } catch {
      json = {};
    }

    if (!response.ok) {
      const err = new Error(
        `Airtable GET ${url.toString()} failed with ${response.status} ${response.statusText}: ${responseText}`
      );
      err.status = response.status;
      err.url = url.toString();
      err.method = "GET";
      err.responseText = responseText;
      throw err;
    }

    return json;
  } catch (err) {
    console.error("[airtable] listRecords error", err);
    throw err;
  }
}

/**
 * Create a record in `table` with `fields`.
 * Returns the created record object.
 */
export async function createOne(table, fields) {
  try {
    console.log("[airtable] createOne", { table, fields });

    const json = await airtableRequest(table, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ records: [{ fields }] /*, typecast: true*/ })
    });

    const record = json.records?.[0];
    console.log("[airtable] createOne result", { id: record?.id });
    return record;
  } catch (err) {
    console.error("[airtable] createOne error", err);
    throw err;
  }
}

/**
 * Update a record by id in `table` with `fields`.
 * Returns the updated record object.
 */
export async function updateOne(table, recordId, fields) {
  try {
    console.log("[airtable] updateOne", { table, recordId, fields });

    const json = await airtableRequest(table, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ records: [{ id: recordId, fields }] /*, typecast: true*/ })
    });

    const record = json.records?.[0];
    console.log("[airtable] updateOne result", { id: record?.id });
    return record;
  } catch (err) {
    console.error("[airtable] updateOne error", err);
    throw err;
  }
}
