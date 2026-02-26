#!/usr/bin/env node

import { execSync } from "node:child_process";
import { listRecords, updateOne } from "../src/lib/airtable.js";

const COMMIT = process.argv.includes("--commit");

const readNetlifyEnv = () => {
  try {
    const raw = execSync("npx netlify env:list --json", {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return JSON.parse(raw);
  } catch {
    return {};
  }
};

const NETLIFY_ENV = readNetlifyEnv();
const getEnv = (key, fallback = "") => process.env[key] || NETLIFY_ENV[key] || fallback;

[
  "AIRTABLE_BASE_ID",
  "AIRTABLE_TOKEN",
  "AIRTABLE_SAVED_CONFIGS_TABLE_ID",
  "AIRTABLE_LABELS_TABLE_ID",
  "AIRTABLE_LABEL_VERSIONS_TABLE_ID",
].forEach((key) => {
  if (!process.env[key] && NETLIFY_ENV[key]) {
    process.env[key] = NETLIFY_ENV[key];
  }
});

const STUDIO_TABLES = {
  savedConfigurations: getEnv(
    "AIRTABLE_SAVED_CONFIGS_TABLE_ID",
    "Saved Configurations"
  ),
  labels: getEnv("AIRTABLE_LABELS_TABLE_ID", "Labels"),
  labelVersions: getEnv("AIRTABLE_LABEL_VERSIONS_TABLE_ID", "Label Versions"),
};

const STUDIO_FIELDS = {
  savedConfigurations: {
    sessionId: "Session ID",
    labels: "Labels",
  },
  labels: {
    sessionId: "Session ID",
  },
  labelVersions: {
    sessionId: "Session ID",
    labels: "Labels",
  },
};

const ensureEnv = () => {
  const missing = ["AIRTABLE_BASE_ID", "AIRTABLE_TOKEN"].filter((key) => !getEnv(key));
  if (!missing.length) return;
  console.error(`Missing required env: ${missing.join(", ")}`);
  process.exit(2);
};

const asText = (value) => {
  const text = String(value || "").trim();
  return text || null;
};

const getLinkedIds = (record, fieldName) => {
  const raw = record?.fields?.[fieldName];
  if (!Array.isArray(raw)) return [];
  return raw
    .map((id) => asText(id))
    .filter((id) => id && id.startsWith("rec"));
};

const listAll = async (table, { fields } = {}) => {
  const out = [];
  let offset = null;
  do {
    const page = await listRecords(table, {
      fields,
      pageSize: 100,
      offset,
    });
    const rows = Array.isArray(page?.records) ? page.records : [];
    out.push(...rows);
    offset = page?.offset || null;
  } while (offset);
  return out;
};

const buildLabelSessionMap = async () => {
  const labels = await listAll(STUDIO_TABLES.labels, {
    fields: [STUDIO_FIELDS.labels.sessionId],
  });
  const map = new Map();
  labels.forEach((record) => {
    const sessionId = asText(record?.fields?.[STUDIO_FIELDS.labels.sessionId]);
    if (sessionId) map.set(record.id, sessionId);
  });
  return map;
};

const backfillTable = async ({
  tableName,
  sessionField,
  linkedLabelsField,
  labelSessionById,
}) => {
  const rows = await listAll(tableName, {
    fields: [sessionField, linkedLabelsField],
  });
  let scanned = 0;
  let missing = 0;
  let resolvable = 0;
  let updated = 0;

  for (const row of rows) {
    scanned += 1;
    const existing = asText(row?.fields?.[sessionField]);
    if (existing) continue;
    missing += 1;

    const linkedLabelIds = getLinkedIds(row, linkedLabelsField);
    if (!linkedLabelIds.length) continue;

    const resolved = linkedLabelIds
      .map((id) => labelSessionById.get(id))
      .find(Boolean);
    if (!resolved) continue;
    resolvable += 1;

    if (!COMMIT) continue;
    await updateOne(tableName, row.id, { [sessionField]: resolved });
    updated += 1;
  }

  return { scanned, missing, resolvable, updated };
};

const main = async () => {
  ensureEnv();
  console.log(
    COMMIT
      ? "Running session-id backfill in COMMIT mode."
      : "Running session-id backfill in DRY-RUN mode."
  );

  const labelSessionById = await buildLabelSessionMap();
  console.log(`Labels with session ids: ${labelSessionById.size}`);

  const savedConfigurations = await backfillTable({
    tableName: STUDIO_TABLES.savedConfigurations,
    sessionField: STUDIO_FIELDS.savedConfigurations.sessionId,
    linkedLabelsField: STUDIO_FIELDS.savedConfigurations.labels,
    labelSessionById,
  });

  const labelVersions = await backfillTable({
    tableName: STUDIO_TABLES.labelVersions,
    sessionField: STUDIO_FIELDS.labelVersions.sessionId,
    linkedLabelsField: STUDIO_FIELDS.labelVersions.labels,
    labelSessionById,
  });

  console.log("Saved Configurations:", savedConfigurations);
  console.log("Label Versions:", labelVersions);
  if (!COMMIT) {
    console.log("Dry run complete. Re-run with --commit to apply updates.");
  }
};

main().catch((error) => {
  console.error(error?.stack || String(error));
  process.exit(2);
});
