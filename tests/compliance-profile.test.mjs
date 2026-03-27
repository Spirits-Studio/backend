import test from "node:test";
import assert from "node:assert/strict";
import crypto from "crypto";

process.env.SHOPIFY_API_SECRET = "test-shopify-secret";
process.env.SHOPIFY_STORE_DOMAIN = "spiritsstudio.co.uk";
process.env.AIRTABLE_BASE_ID = "appTestBase";
process.env.AIRTABLE_TOKEN = "patTestToken";
process.env.AIRTABLE_CUSTOMERS_TABLE_ID = "Customers";
process.env.AIRTABLE_SAVED_CONFIGS_TABLE_ID = "Saved Configurations";

const { default: complianceProfile } = await import(
  "../netlify/functions/compliance-profile.js"
);
const { STUDIO_FIELDS, STUDIO_TABLES } = await import(
  "../netlify/functions/_lib/studio.js"
);

const signProxyQuery = (query, secret = process.env.SHOPIFY_API_SECRET) => {
  const message = Object.keys(query)
    .sort()
    .map((key) => `${key}=${query[key] ?? ""}`)
    .join("");
  const signature = crypto
    .createHmac("sha256", secret)
    .update(message, "utf8")
    .digest("hex");
  return { ...query, signature };
};

const createProxyEvent = ({ method = "GET", query = {}, body = null } = {}) => {
  const signedQuery = signProxyQuery({
    shop: process.env.SHOPIFY_STORE_DOMAIN,
    timestamp: "1700000000",
    ...query,
  });

  return {
    httpMethod: method,
    headers: {
      origin: "https://spiritsstudio.co.uk",
      ...(body != null ? { "content-type": "application/json" } : {}),
    },
    queryStringParameters: signedQuery,
    body: body == null ? "" : JSON.stringify(body),
  };
};

const parseMaybeJson = (raw) => {
  if (typeof raw !== "string" || !raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
};

const installFetchSequence = (steps) => {
  const originalFetch = global.fetch;
  let index = 0;

  global.fetch = async (input, init = {}) => {
    const method = String(init.method || "GET").toUpperCase();
    const inputUrl =
      typeof input === "string" ? input : input?.url || String(input || "");
    const url = new URL(inputUrl);
    const pathParts = url.pathname.split("/").filter(Boolean);
    const table = decodeURIComponent(pathParts[2] || "");
    const recordId = decodeURIComponent(pathParts[3] || "") || null;
    const call = {
      method,
      url,
      table,
      recordId,
      body: parseMaybeJson(typeof init.body === "string" ? init.body : ""),
      headers: init.headers || {},
    };

    const step = steps[index];
    assert.ok(step, `Unexpected fetch #${index + 1}: ${method} ${url.toString()}`);
    if (step.method) {
      assert.equal(method, step.method, `Fetch #${index + 1} method mismatch`);
    }
    if (Object.hasOwn(step, "table")) {
      assert.equal(table, step.table, `Fetch #${index + 1} table mismatch`);
    }
    if (Object.hasOwn(step, "recordId")) {
      if (recordId == null && method === "PATCH") {
        const bodyRecordId = call.body?.records?.[0]?.id || null;
        assert.equal(
          bodyRecordId,
          step.recordId,
          `Fetch #${index + 1} record id mismatch`
        );
      } else {
        assert.equal(recordId, step.recordId, `Fetch #${index + 1} record id mismatch`);
      }
    }
    if (typeof step.assert === "function") {
      await step.assert(call);
    }

    index += 1;
    return new Response(JSON.stringify(step.response || {}), {
      status: step.status || 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  return {
    restore() {
      global.fetch = originalFetch;
    },
    assertDone() {
      assert.equal(index, steps.length, "Not all expected fetch calls were made.");
    },
  };
};

test("compliance-profile GET returns saved Airtable compliance details", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recComplianceCustomerA",
      response: {
        id: "recComplianceCustomerA",
        fields: {
          [STUDIO_FIELDS.customers.premiseLicence]: "PL-111",
          [STUDIO_FIELDS.customers.alcoholLicence]: "AL-222",
          [STUDIO_FIELDS.customers.personalLicence]: "PERSONAL-333",
          [STUDIO_FIELDS.customers.companyName]: "Trade Co Ltd",
          [STUDIO_FIELDS.customers.tradingName]: "Trade Co",
          [STUDIO_FIELDS.customers.companyNumber]: "12345678",
          [STUDIO_FIELDS.customers.vatNumber]: "GB123",
        },
      },
    },
  ]);

  try {
    const response = await complianceProfile(
      createProxyEvent({
        method: "GET",
        query: { airtable_id: "recComplianceCustomerA" },
      })
    );
    const json = await response.json();

    assert.equal(response.status, 200);
    assert.equal(json.ok, true);
    assert.equal(json.airtableId, "recComplianceCustomerA");
    assert.equal(json.hasProfile, true);
    assert.equal(json.profile.for_resale, true);
    assert.equal(json.profile.premise_licence, "PL-111");
    assert.equal(json.profile.licence_type, "personal");
    assert.equal(json.profile.company_name, "Trade Co Ltd");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("compliance-profile GET prefers signed-in Shopify customer linkage over stale airtable id", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "spiritsstudio.co.uk");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.match(call.body?.query || "", /GetCustomerComplianceMetafields/);
        assert.equal(call.body?.variables?.id, "gid://shopify/Customer/9010");
      },
      response: {
        data: {
          customer: {
            id: "gid://shopify/Customer/9010",
            airtableId: { value: "recSignedInCustomerA" },
            complianceProfile: null,
          },
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recSignedInCustomerA",
      response: {
        id: "recSignedInCustomerA",
        fields: {
          [STUDIO_FIELDS.customers.premiseLicence]: "PL-9010",
          [STUDIO_FIELDS.customers.alcoholLicence]: "AL-9010",
          [STUDIO_FIELDS.customers.personalLicence]: "PERSONAL-9010",
        },
      },
    },
  ]);

  try {
    const response = await complianceProfile(
      createProxyEvent({
        method: "GET",
        query: {
          airtable_id: "recStaleGuestCustomer",
          customer_id: "9010",
        },
      })
    );
    const json = await response.json();

    assert.equal(response.status, 200);
    assert.equal(json.ok, true);
    assert.equal(json.airtableId, "recSignedInCustomerA");
    assert.equal(json.profile.premise_licence, "PL-9010");
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
    fetchMock.restore();
  }
});

test("compliance-profile GET ignores explicit airtable id fallback when proxy auth shows a signed-in customer", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "spiritsstudio.co.uk");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.match(call.body?.query || "", /GetCustomerComplianceMetafields/);
        assert.equal(call.body?.variables?.id, "gid://shopify/Customer/9011");
      },
      response: {
        data: {
          customer: {
            id: "gid://shopify/Customer/9011",
            airtableId: null,
            complianceProfile: null,
          },
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      response: { records: [] },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      response: { records: [] },
    },
  ]);

  try {
    const response = await complianceProfile(
      createProxyEvent({
        method: "GET",
        query: {
          airtable_id: "recStaleGuestCustomer",
          logged_in_customer_id: "gid://shopify/Customer/9011",
        },
      })
    );
    const json = await response.json();

    assert.equal(response.status, 200);
    assert.equal(json.ok, true);
    assert.equal(json.airtableId, null);
    assert.equal(json.shopifyCustomerId, "9011");
    assert.equal(json.hasProfile, false);
    assert.equal(json.profile, null);
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
    fetchMock.restore();
  }
});

test("compliance-profile GET returns non-resale Shopify metafield profiles with hasProfile=true", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "spiritsstudio.co.uk");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.match(call.body?.query || "", /GetCustomerComplianceMetafields/);
        assert.equal(call.body?.variables?.id, "gid://shopify/Customer/9012");
      },
      response: {
        data: {
          customer: {
            id: "gid://shopify/Customer/9012",
            airtableId: null,
            complianceProfile: {
              value: JSON.stringify({
                schema_version: 1,
                for_resale: false,
                saved_at: "2026-03-27T12:34:56.000Z",
              }),
            },
          },
        },
      },
    },
  ]);

  try {
    const response = await complianceProfile(
      createProxyEvent({
        method: "GET",
        query: {
          customer_id: "9012",
        },
      })
    );
    const json = await response.json();

    assert.equal(response.status, 200);
    assert.equal(json.ok, true);
    assert.equal(json.airtableId, null);
    assert.equal(json.shopifyCustomerId, "9012");
    assert.equal(json.hasProfile, true);
    assert.equal(json.source, "shopify");
    assert.equal(json.profile?.for_resale, false);
    assert.equal(json.profile?.premise_licence, null);
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
    fetchMock.restore();
  }
});

test("compliance-profile GET falls back to saved configuration session linkage", async () => {
  const fetchMock = installFetchSequence([
    {
      method: "GET",
      table: STUDIO_TABLES.savedConfigurations,
      assert: (call) => {
        assert.equal(
          call.url.searchParams.get("filterByFormula"),
          "{Session ID}='session-compliance-lookup'"
        );
      },
      response: {
        records: [
          {
            id: "recSavedConfigCompliance1",
            fields: {
              [STUDIO_FIELDS.savedConfigurations.customer]: [
                "recComplianceCustomerSession",
              ],
            },
          },
        ],
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recComplianceCustomerSession",
      response: {
        id: "recComplianceCustomerSession",
        fields: {
          [STUDIO_FIELDS.customers.premiseLicence]: "PL-444",
          [STUDIO_FIELDS.customers.alcoholLicence]: "AL-555",
          [STUDIO_FIELDS.customers.tenLicence]: "TEN-666",
        },
      },
    },
  ]);

  try {
    const response = await complianceProfile(
      createProxyEvent({
        method: "GET",
        query: { session_id: "session-compliance-lookup" },
      })
    );
    const json = await response.json();

    assert.equal(response.status, 200);
    assert.equal(json.ok, true);
    assert.equal(json.airtableId, "recComplianceCustomerSession");
    assert.equal(json.profile.licence_type, "ten");
    assert.equal(json.profile.ten_licence, "TEN-666");
    fetchMock.assertDone();
  } finally {
    fetchMock.restore();
  }
});

test("compliance-profile PATCH persists Airtable fields and Shopify customer metafields", async () => {
  const previousAdminToken = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = "shpat_test_admin_token";

  const fetchMock = installFetchSequence([
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "spiritsstudio.co.uk");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.match(call.body?.query || "", /GetCustomerComplianceMetafields/);
        assert.equal(call.body?.variables?.id, "gid://shopify/Customer/9010");
      },
      response: {
        data: {
          customer: {
            id: "gid://shopify/Customer/9010",
            airtableId: null,
            complianceProfile: null,
          },
        },
      },
    },
    {
      method: "GET",
      table: STUDIO_TABLES.customers,
      recordId: "recComplianceCustomerWrite",
      response: {
        id: "recComplianceCustomerWrite",
        fields: {
          "Shopify ID": "9010",
          Source: "Shopify",
          "Shop Domain": "spiritsstudio.co.uk",
        },
      },
    },
    {
      method: "PATCH",
      table: STUDIO_TABLES.customers,
      recordId: "recComplianceCustomerWrite",
      assert: (call) => {
        const fields = call.body?.records?.[0]?.fields || {};
        assert.equal(fields[STUDIO_FIELDS.customers.premiseLicence], "PL-777");
        assert.equal(fields[STUDIO_FIELDS.customers.alcoholLicence], "AL-888");
        assert.equal(fields[STUDIO_FIELDS.customers.personalLicence], null);
        assert.equal(fields[STUDIO_FIELDS.customers.tenLicence], "TEN-999");
        assert.equal(fields[STUDIO_FIELDS.customers.companyName], null);
        assert.equal(fields[STUDIO_FIELDS.customers.companyNumber], null);
      },
      response: {
        records: [{ id: "recComplianceCustomerWrite", fields: {} }],
      },
    },
    {
      method: "POST",
      assert: (call) => {
        assert.equal(call.url.host, "spiritsstudio.co.uk");
        assert.equal(call.url.pathname, "/admin/api/2025-07/graphql.json");
        assert.match(call.body?.query || "", /metafieldsSet/);
        const metafields = call.body?.variables?.metafields || [];
        assert.equal(metafields.length, 2);
        assert.equal(metafields[0].ownerId, "gid://shopify/Customer/9010");
        assert.equal(metafields[0].namespace, "SS");
        assert.equal(metafields[0].key, "airtable_id");
        assert.equal(metafields[0].type, "single_line_text_field");
        assert.equal(metafields[0].value, "recComplianceCustomerWrite");
        assert.equal(metafields[1].key, "compliance_profile");
        const value = JSON.parse(metafields[1].value);
        assert.equal(value.for_resale, true);
        assert.equal(value.licence_type, "ten");
        assert.equal(value.ten_licence, "TEN-999");
      },
      response: {
        data: {
          metafieldsSet: {
            metafields: [
              { id: "gid://shopify/Metafield/10", namespace: "SS", key: "airtable_id" },
              { id: "gid://shopify/Metafield/11", namespace: "SS", key: "compliance_profile" },
            ],
            userErrors: [],
          },
        },
      },
    },
  ]);

  try {
    const response = await complianceProfile(
      createProxyEvent({
        method: "PATCH",
        body: {
          airtableId: "recComplianceCustomerWrite",
          customer_id: "9010",
          for_resale: true,
          premise_licence: "PL-777",
          alcohol_licence: "AL-888",
          licence_type: "ten",
          ten_licence: "TEN-999",
          is_business_purchase: false,
        },
      })
    );
    const json = await response.json();

    assert.equal(response.status, 200);
    assert.equal(json.ok, true);
    assert.equal(json.airtableId, "recComplianceCustomerWrite");
    assert.equal(json.profile.for_resale, true);
    assert.equal(json.profile.licence_type, "ten");
    assert.equal(json.shopifySync.ok, true);
    fetchMock.assertDone();
  } finally {
    if (previousAdminToken == null) {
      delete process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    } else {
      process.env.SHOPIFY_ADMIN_ACCESS_TOKEN = previousAdminToken;
    }
    fetchMock.restore();
  }
});
