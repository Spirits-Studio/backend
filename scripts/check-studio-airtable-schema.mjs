#!/usr/bin/env node

import { execSync } from "node:child_process";

const REQUIRED_ENV_KEYS = [
  "AIRTABLE_BASE_ID",
  "AIRTABLE_TOKEN",
  "AIRTABLE_CUSTOMERS_TABLE_ID",
  "AIRTABLE_SAVED_CONFIGS_TABLE_ID",
];

const SCHEMA_SPEC = {
  customers: {
    envKey: "AIRTABLE_CUSTOMERS_TABLE_ID",
    tableName: "Customers",
    fields: [
      { name: "Shopify ID", type: "singleLineText" },
      { name: "Email", type: "email" },
      { name: "First Name", type: "singleLineText" },
      { name: "Last Name", type: "singleLineText" },
      { name: "Phone", type: "phoneNumber" },
      { name: "Source", type: "singleSelect" },
      { name: "Shop Domain", type: "singleSelect" },
      { name: "Creation Source", type: "singleSelect" },
    ],
  },
  savedConfigurations: {
    envKey: "AIRTABLE_SAVED_CONFIGS_TABLE_ID",
    tableName: "Saved Configurations",
    fields: [
      { name: "Configuration ID", type: "singleLineText" },
      { name: "Customer", type: "multipleRecordLinks" },
      { name: "Session ID", type: "singleLineText" },
      { name: "Labels", type: "multipleRecordLinks" },
      { name: "Config JSON", type: "multilineText" },
      { name: "Status", type: "singleSelect" },
      { name: "Display Name", type: "singleLineText" },
      {
        name: "Current Front Label Versions",
        type: "multipleRecordLinks",
        legacyNames: ["Current Front Label Version"],
      },
      {
        name: "Current Back Label Versions",
        type: "multipleRecordLinks",
        legacyNames: ["Current Back Label Version"],
      },
    ],
  },
  labels: {
    envKey: "AIRTABLE_LABELS_TABLE_ID",
    tableName: "Labels",
    fields: [
      { name: "Customers", type: "multipleRecordLinks" },
      { name: "Display Name", type: "singleLineText" },
      { name: "Label Versions", type: "multipleRecordLinks" },
      { name: "Saved Configurations", type: "multipleRecordLinks" },
      { name: "Session ID", type: "singleLineText" },
      {
        name: "Current Front Label Versions",
        type: "multipleRecordLinks",
        legacyNames: ["Current Front Label Version"],
      },
      {
        name: "Current Back Label Versions",
        type: "multipleRecordLinks",
        legacyNames: ["Current Back Label Version"],
      },
    ],
  },
  labelVersions: {
    envKey: "AIRTABLE_LABEL_VERSIONS_TABLE_ID",
    tableName: "Label Versions",
    fields: [
      { name: "Name", type: "singleLineText" },
      { name: "Accepted", type: "checkbox" },
      { name: "Design Side", type: "singleSelect" },
      { name: "Version Kind", type: "singleSelect" },
      { name: "Version Number", type: "number" },
      { name: "Prompt Text", type: "multilineText" },
      { name: "Edit Prompt Text", type: "multilineText" },
      { name: "Session ID", type: "singleLineText" },
      { name: "Labels", type: "multipleRecordLinks" },
      {
        name: "Saved Configurations",
        type: "multipleRecordLinks",
        legacyNames: [
          "Saved Configurations - Current Front Label",
          "Saved Configurations - Current Back Label",
        ],
      },
      { name: "Previous Label Version", type: "multipleRecordLinks" },
      { name: "Created At", type: "createdTime", legacyNames: ["Created Date"] },
    ],
    checks: ["previousLabelVersionSelfLink"],
  },
};

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

const loadEnv = () => {
  const netlifyEnv = readNetlifyEnv();
  return REQUIRED_ENV_KEYS.reduce((acc, key) => {
    acc[key] = process.env[key] || netlifyEnv[key] || "";
    return acc;
  }, {});
};

const ensureEnv = (env) => {
  const missing = REQUIRED_ENV_KEYS.filter((key) => !env[key]);
  if (!missing.length) return;
  console.error("Missing required Airtable env keys:", missing.join(", "));
  process.exit(2);
};

const findTable = (tables, envTableId, fallbackName) => {
  return (
    tables.find((table) => table.id === envTableId) ||
    tables.find((table) => table.name === fallbackName) ||
    null
  );
};

const resolveField = (table, fieldSpec) => {
  const names = [fieldSpec.name, ...(fieldSpec.legacyNames || [])];
  for (const name of names) {
    const field = table.fields.find((candidate) => candidate.name === name);
    if (field) {
      return {
        field,
        usesLegacyName: name !== fieldSpec.name,
      };
    }
  }
  return null;
};

const checkField = (table, fieldSpec, findings) => {
  const resolved = resolveField(table, fieldSpec);
  if (!resolved) {
    findings.errors.push(
      `[${table.name}] Missing field "${fieldSpec.name}".`
    );
    return;
  }

  const { field, usesLegacyName } = resolved;
  if (field.type !== fieldSpec.type) {
    findings.errors.push(
      `[${table.name}] Field "${field.name}" has type "${field.type}" (expected "${fieldSpec.type}").`
    );
  }
  if (usesLegacyName) {
    findings.warnings.push(
      `[${table.name}] Using legacy field name "${field.name}" (target "${fieldSpec.name}").`
    );
  }
};

const checkPreviousLabelVersionSelfLink = (table, findings) => {
  const field = table.fields.find(
    (candidate) => candidate.name === "Previous Label Version"
  );
  if (!field) return;
  if (field.type !== "multipleRecordLinks") return;
  const linkedTableId = field.options?.linkedTableId || "";
  if (linkedTableId !== table.id) {
    findings.errors.push(
      `[${table.name}] "Previous Label Version" must link to "${table.name}" (linkedTableId=${linkedTableId || "unknown"}).`
    );
  }
};

const main = async () => {
  const env = loadEnv();
  ensureEnv(env);

  const response = await fetch(
    `https://api.airtable.com/v0/meta/bases/${env.AIRTABLE_BASE_ID}/tables`,
    {
      headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}` },
    }
  );

  if (!response.ok) {
    const body = await response.text();
    console.error(`Failed to load Airtable metadata (${response.status}).`);
    console.error(body.slice(0, 300));
    process.exit(2);
  }

  const payload = await response.json();
  const tables = Array.isArray(payload?.tables) ? payload.tables : [];

  const findings = { errors: [], warnings: [] };
  for (const spec of Object.values(SCHEMA_SPEC)) {
    const table = findTable(tables, env[spec.envKey], spec.tableName);
    if (!table) {
      findings.errors.push(`Missing table "${spec.tableName}".`);
      continue;
    }

    spec.fields.forEach((fieldSpec) => checkField(table, fieldSpec, findings));
    if (spec.checks?.includes("previousLabelVersionSelfLink")) {
      checkPreviousLabelVersionSelfLink(table, findings);
    }
  }

  if (findings.warnings.length) {
    console.log("WARNINGS");
    findings.warnings.forEach((warning) => console.log(`- ${warning}`));
  }

  if (findings.errors.length) {
    console.log("ERRORS");
    findings.errors.forEach((error) => console.log(`- ${error}`));
    process.exit(1);
  }

  console.log("Studio Airtable schema check passed.");
  if (findings.warnings.length) {
    console.log("Warnings indicate legacy names still in use.");
  }
};

main().catch((error) => {
  console.error(error?.stack || String(error));
  process.exit(2);
});
