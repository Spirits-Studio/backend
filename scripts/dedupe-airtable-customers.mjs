import { listRecords, updateOne } from "../src/lib/airtable.js";

const args = new Set(process.argv.slice(2));
const APPLY = args.has("--apply");
const VERBOSE = args.has("--verbose");

const TABLES = {
  customers: process.env.AIRTABLE_CUSTOMERS_TABLE_ID || "Customers",
  savedConfigurations:
    process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID || "Saved Configurations",
  labels: process.env.AIRTABLE_LABELS_TABLE_ID || "Labels",
  labelVersions:
    process.env.AIRTABLE_LABEL_VERSIONS_TABLE_ID || "Label Versions",
  orders:
    process.env.AIRTABLE_ORDERS_FULFILLMENT_TABLE_ID ||
    process.env.AIRTABLE_ORDERS_TABLE ||
    "Orders & Fulfilment",
};

const LINK_FIELDS_BY_TABLE = {
  [TABLES.savedConfigurations]: ["Customer"],
  [TABLES.labels]: ["Customers"],
  [TABLES.labelVersions]: ["Customer", "Customers"],
  [TABLES.orders]: ["Customer", "userId", "User"],
};

const CUSTOMER_FIELDS = {
  shopifyId: "Shopify ID",
  email: "Email",
  firstName: "First Name",
  lastName: "Last Name",
  phone: "Phone",
  source: "Source",
  shopDomain: "Shop Domain",
};

const requiredEnv = ["AIRTABLE_BASE_ID", "AIRTABLE_TOKEN"];
const missing = requiredEnv.filter((key) => !process.env[key]);
if (missing.length) {
  console.error("Missing required Airtable env keys:", missing.join(", "));
  process.exit(1);
}

const isRecordId = (value) => {
  const text = String(value || "").trim();
  return text.startsWith("rec") ? text : null;
};

const normalizeEmail = (value) => {
  const text = String(value || "").trim().toLowerCase();
  return text.includes("@") ? text : null;
};

const isLegacyAlias = (value) =>
  /^legacy_airtable_record:/i.test(String(value || "").trim());

const normalizeShopifyId = (value) => {
  const text = String(value || "").trim();
  if (!text) return null;
  if (isRecordId(text)) return null;
  if (isLegacyAlias(text)) return null;
  return text;
};

async function listAllRecords(table, options = {}) {
  const out = [];
  let offset = null;
  do {
    const page = await listRecords(table, {
      ...options,
      pageSize: 100,
      offset,
    });
    const rows = Array.isArray(page?.records) ? page.records : [];
    out.push(...rows);
    offset = page?.offset || null;
  } while (offset);
  return out;
}

class UnionFind {
  constructor() {
    this.parent = new Map();
    this.rank = new Map();
  }

  makeSet(x) {
    if (this.parent.has(x)) return;
    this.parent.set(x, x);
    this.rank.set(x, 0);
  }

  find(x) {
    this.makeSet(x);
    const p = this.parent.get(x);
    if (p !== x) {
      const root = this.find(p);
      this.parent.set(x, root);
      return root;
    }
    return p;
  }

  union(a, b) {
    const ra = this.find(a);
    const rb = this.find(b);
    if (ra === rb) return;

    const rankA = this.rank.get(ra) || 0;
    const rankB = this.rank.get(rb) || 0;

    if (rankA < rankB) {
      this.parent.set(ra, rb);
    } else if (rankA > rankB) {
      this.parent.set(rb, ra);
    } else {
      this.parent.set(rb, ra);
      this.rank.set(ra, rankA + 1);
    }
  }
}

function customerScore(record) {
  const fields = record?.fields || {};
  const shopifyId = normalizeShopifyId(fields[CUSTOMER_FIELDS.shopifyId]);
  const email = normalizeEmail(fields[CUSTOMER_FIELDS.email]);
  const firstName = String(fields[CUSTOMER_FIELDS.firstName] || "").trim();
  const lastName = String(fields[CUSTOMER_FIELDS.lastName] || "").trim();
  const phone = String(fields[CUSTOMER_FIELDS.phone] || "").trim();
  let score = 0;
  if (shopifyId) score += 10;
  if (email) score += 6;
  if (firstName) score += 2;
  if (lastName) score += 2;
  if (phone) score += 1;
  return score;
}

function chooseCanonical(records) {
  return [...records].sort((a, b) => {
    const scoreDiff = customerScore(b) - customerScore(a);
    if (scoreDiff !== 0) return scoreDiff;

    const aCreated = Date.parse(a?.createdTime || "") || 0;
    const bCreated = Date.parse(b?.createdTime || "") || 0;
    if (aCreated !== bCreated) return aCreated - bCreated;

    return String(a?.id || "").localeCompare(String(b?.id || ""));
  })[0];
}

function buildCustomerMergePatch(canonicalRecord, duplicateRecords) {
  const canonicalFields = canonicalRecord?.fields || {};
  const patch = {};

  const read = (row, field) => {
    const value = row?.fields?.[field];
    if (value == null) return "";
    return String(value).trim();
  };

  const canonicalShopify = normalizeShopifyId(
    canonicalFields[CUSTOMER_FIELDS.shopifyId]
  );

  if (!canonicalShopify) {
    for (const row of duplicateRecords) {
      const value = normalizeShopifyId(read(row, CUSTOMER_FIELDS.shopifyId));
      if (value) {
        patch[CUSTOMER_FIELDS.shopifyId] = value;
        break;
      }
    }
  }

  const canonicalEmail = normalizeEmail(canonicalFields[CUSTOMER_FIELDS.email]);
  if (!canonicalEmail) {
    for (const row of duplicateRecords) {
      const value = normalizeEmail(read(row, CUSTOMER_FIELDS.email));
      if (value) {
        patch[CUSTOMER_FIELDS.email] = value;
        break;
      }
    }
  }

  const passthrough = [
    CUSTOMER_FIELDS.firstName,
    CUSTOMER_FIELDS.lastName,
    CUSTOMER_FIELDS.phone,
    CUSTOMER_FIELDS.source,
    CUSTOMER_FIELDS.shopDomain,
  ];
  for (const field of passthrough) {
    if (String(canonicalFields[field] || "").trim()) continue;
    for (const row of duplicateRecords) {
      const value = read(row, field);
      if (!value) continue;
      patch[field] = value;
      break;
    }
  }

  return patch;
}

function rewriteLinkedIds(value, replacementMap) {
  if (!Array.isArray(value)) return { changed: false, value };

  const rewritten = [];
  let changed = false;
  for (const entry of value) {
    const recordId = isRecordId(entry);
    if (!recordId) continue;
    const next = replacementMap.get(recordId) || recordId;
    if (next !== recordId) changed = true;
    if (!rewritten.includes(next)) rewritten.push(next);
  }

  if (!changed && rewritten.length === value.length) {
    let same = true;
    for (let i = 0; i < rewritten.length; i += 1) {
      if (rewritten[i] !== value[i]) {
        same = false;
        break;
      }
    }
    if (same) return { changed: false, value };
  }

  return { changed: true, value: rewritten };
}

async function relinkTable({ table, fields, replacementMap }) {
  const rows = await listAllRecords(table);
  let changed = 0;

  for (const row of rows) {
    const patch = {};
    for (const fieldName of fields) {
      if (!Object.hasOwn(row?.fields || {}, fieldName)) continue;
      const current = row?.fields?.[fieldName];
      const rewritten = rewriteLinkedIds(current, replacementMap);
      if (rewritten.changed) patch[fieldName] = rewritten.value;
    }

    if (!Object.keys(patch).length) continue;

    changed += 1;
    if (VERBOSE) {
      console.log("[dedupe] relink", {
        table,
        recordId: row.id,
        patch,
      });
    }

    if (APPLY) {
      await updateOne(table, row.id, patch);
    }
  }

  return changed;
}

async function main() {
  console.log(`[dedupe] mode=${APPLY ? "apply" : "dry-run"}`);
  const customers = await listAllRecords(TABLES.customers);
  console.log(`[dedupe] loaded customers=${customers.length}`);

  const uf = new UnionFind();
  const keyToRecord = new Map();
  const keyedRecords = [];

  for (const row of customers) {
    uf.makeSet(row.id);

    const shopifyId = normalizeShopifyId(row?.fields?.[CUSTOMER_FIELDS.shopifyId]);
    const email = normalizeEmail(row?.fields?.[CUSTOMER_FIELDS.email]);
    const keys = [];
    if (shopifyId) keys.push(`shopify:${shopifyId}`);
    if (email) keys.push(`email:${email}`);
    if (!keys.length) continue;

    keyedRecords.push(row.id);

    for (const key of keys) {
      const existing = keyToRecord.get(key);
      if (existing) {
        uf.union(existing, row.id);
      } else {
        keyToRecord.set(key, row.id);
      }
    }
  }

  const groups = new Map();
  for (const row of customers) {
    const root = uf.find(row.id);
    const arr = groups.get(root) || [];
    arr.push(row);
    groups.set(root, arr);
  }

  const duplicateGroups = [...groups.values()].filter((rows) => rows.length > 1);
  console.log(`[dedupe] duplicate groups=${duplicateGroups.length}`);

  if (!duplicateGroups.length) {
    console.log("[dedupe] no duplicate customers found");
    return;
  }

  const replacementMap = new Map();
  const canonicalMerges = [];

  for (const rows of duplicateGroups) {
    const canonical = chooseCanonical(rows);
    const duplicates = rows.filter((row) => row.id !== canonical.id);

    for (const dup of duplicates) {
      replacementMap.set(dup.id, canonical.id);
    }

    const mergePatch = buildCustomerMergePatch(canonical, duplicates);
    if (Object.keys(mergePatch).length) {
      canonicalMerges.push({
        recordId: canonical.id,
        patch: mergePatch,
      });
    }

    console.log("[dedupe] group", {
      canonical: canonical.id,
      duplicates: duplicates.map((r) => r.id),
      canonical_patch_keys: Object.keys(mergePatch),
    });
  }

  let canonicalPatched = 0;
  for (const row of canonicalMerges) {
    canonicalPatched += 1;
    if (VERBOSE) {
      console.log("[dedupe] canonical-merge", row);
    }
    if (APPLY) {
      await updateOne(TABLES.customers, row.recordId, row.patch);
    }
  }

  const relinkSummary = {};
  for (const [table, fields] of Object.entries(LINK_FIELDS_BY_TABLE)) {
    const changed = await relinkTable({ table, fields, replacementMap });
    relinkSummary[table] = changed;
  }

  const duplicatesCount = replacementMap.size;
  console.log("[dedupe] summary", {
    mode: APPLY ? "apply" : "dry-run",
    duplicate_customer_records: duplicatesCount,
    canonical_records_patched: canonicalPatched,
    relinked_records_by_table: relinkSummary,
  });

  if (!APPLY) {
    console.log(
      "[dedupe] dry-run complete. Re-run with --apply to persist Airtable updates."
    );
  }
}

main().catch((error) => {
  console.error("[dedupe] failed", error);
  process.exit(1);
});
