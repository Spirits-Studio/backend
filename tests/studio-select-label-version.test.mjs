import test from "node:test";
import assert from "node:assert/strict";

process.env.SHOPIFY_API_SECRET = process.env.SHOPIFY_API_SECRET || "test-shopify-secret";
process.env.SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN || "spiritsstudio.co.uk";

const { createStudioSelectLabelVersionHandler } = await import(
  "../netlify/functions/studio-select-label-version.js"
);
const { STUDIO_TABLES, STUDIO_FIELDS } = await import(
  "../netlify/functions/_lib/studio.js"
);

const createHarness = ({
  body,
  getRecordOrNullImpl,
  findSavedConfigurationBySessionIdImpl,
  updateResilientImpl,
} = {}) => {
  const handler = createStudioSelectLabelVersionHandler({
    parseBodyImpl: async () => body || {},
    sendJsonImpl: (status, payload) => ({ status, payload }),
    getRecordOrNullImpl,
    findSavedConfigurationBySessionIdImpl,
    updateResilientImpl,
    mapErrorResponseImpl: (error) => ({
      ok: false,
      error: error?.message || "unexpected_error",
    }),
  });

  return handler({}, { isV2: false, method: "POST" });
};

test("select-label-version returns deferred success when session has no saved configuration", async () => {
  let updateCalls = 0;
  const response = await createHarness({
    body: {
      sessionId: "session-new-user",
      designSide: "front",
      labelVersionRecordId: "recLabelVersionA",
      source: "configurator-carousel",
    },
    getRecordOrNullImpl: async (table, recordId) => {
      if (table === STUDIO_TABLES.labelVersions && recordId === "recLabelVersionA") {
        return {
          id: "recLabelVersionA",
          fields: {
            [STUDIO_FIELDS.labelVersions.sessionId]: "session-new-user",
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
            [STUDIO_FIELDS.labelVersions.versionNumber]: 2,
            [STUDIO_FIELDS.labelVersions.versionKind]: "Edit",
          },
        };
      }
      return null;
    },
    findSavedConfigurationBySessionIdImpl: async () => null,
    updateResilientImpl: async () => {
      updateCalls += 1;
    },
  });

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(response.payload.deferred, true);
  assert.equal(response.payload.idempotent, false);
  assert.equal(response.payload.sessionId, "session-new-user");
  assert.equal(response.payload.designSide, "front");
  assert.equal(response.payload.selectedLabelVersion?.recordId, "recLabelVersionA");
  assert.equal(updateCalls, 0);
});

test("select-label-version enforces ownership when customer_record_id is provided", async () => {
  const response = await createHarness({
    body: {
      sessionId: "session-owned",
      designSide: "front",
      labelVersionRecordId: "recLabelVersionOwned",
      saved_configuration_record_id: "recSavedConfigOwned",
      customer_record_id: "recCustomerA",
      source: "configurator-carousel",
    },
    getRecordOrNullImpl: async (table, recordId) => {
      if (table === STUDIO_TABLES.labelVersions && recordId === "recLabelVersionOwned") {
        return {
          id: "recLabelVersionOwned",
          fields: {
            [STUDIO_FIELDS.labelVersions.sessionId]: "session-owned",
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          },
        };
      }
      if (
        table === STUDIO_TABLES.savedConfigurations &&
        recordId === "recSavedConfigOwned"
      ) {
        return {
          id: "recSavedConfigOwned",
          fields: {
            [STUDIO_FIELDS.savedConfigurations.customer]: ["recCustomerB"],
            [STUDIO_FIELDS.savedConfigurations.sessionId]: "session-owned",
          },
        };
      }
      return null;
    },
    findSavedConfigurationBySessionIdImpl: async () => {
      throw new Error("session resolver should not be called when explicit id is present");
    },
    updateResilientImpl: async () => {
      throw new Error("update should not happen on ownership mismatch");
    },
  });

  assert.equal(response.status, 403);
  assert.equal(response.payload.ok, false);
  assert.equal(response.payload.error, "forbidden");
});

test("select-label-version updates explicit saved configuration id successfully", async () => {
  const updateCalls = [];
  const response = await createHarness({
    body: {
      sessionId: "session-explicit",
      designSide: "front",
      labelVersionRecordId: "recLabelVersionExplicit",
      saved_configuration_record_id: "recSavedConfigExplicit",
      customer_record_id: "recCustomerA",
      source: "configurator-carousel",
      selectedAt: "2026-03-18T12:00:00.000Z",
    },
    getRecordOrNullImpl: async (table, recordId) => {
      if (
        table === STUDIO_TABLES.labelVersions &&
        recordId === "recLabelVersionExplicit"
      ) {
        return {
          id: "recLabelVersionExplicit",
          fields: {
            [STUDIO_FIELDS.labelVersions.sessionId]: "session-explicit",
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
            [STUDIO_FIELDS.labelVersions.versionNumber]: 5,
            [STUDIO_FIELDS.labelVersions.versionKind]: "Edit",
            [STUDIO_FIELDS.labelVersions.outputImageUrl]: "https://cdn.example.com/front.png",
          },
        };
      }
      if (
        table === STUDIO_TABLES.savedConfigurations &&
        recordId === "recSavedConfigExplicit"
      ) {
        return {
          id: "recSavedConfigExplicit",
          fields: {
            [STUDIO_FIELDS.savedConfigurations.customer]: ["recCustomerA"],
            [STUDIO_FIELDS.savedConfigurations.sessionId]: "session-explicit",
            selectedFrontLabelVersionRecordId: "recLabelVersionPrevious",
          },
        };
      }
      return null;
    },
    findSavedConfigurationBySessionIdImpl: async () => {
      throw new Error("session resolver should not be called when explicit id is present");
    },
    updateResilientImpl: async (table, recordId, _opts, fields) => {
      updateCalls.push({ table, recordId, fields });
      return { id: recordId, fields };
    },
  });

  assert.equal(response.status, 200);
  assert.equal(response.payload.ok, true);
  assert.equal(response.payload.idempotent, false);
  assert.equal(response.payload.saved_configuration_record_id, "recSavedConfigExplicit");
  assert.equal(response.payload.selectedLabelVersion?.recordId, "recLabelVersionExplicit");

  assert.equal(updateCalls.length, 1);
  assert.equal(updateCalls[0].table, STUDIO_TABLES.savedConfigurations);
  assert.equal(updateCalls[0].recordId, "recSavedConfigExplicit");
  assert.equal(
    updateCalls[0].fields.selectedFrontLabelVersionRecordId,
    "recLabelVersionExplicit"
  );
  assert.equal(
    updateCalls[0].fields.selectedFrontLabelVersionAt,
    "2026-03-18T12:00:00.000Z"
  );
  assert.equal(
    updateCalls[0].fields.selectedFrontLabelVersionBy,
    "configurator-carousel"
  );
});

test("select-label-version returns session_mismatch when selected version belongs to another session", async () => {
  const response = await createHarness({
    body: {
      sessionId: "session-one",
      designSide: "front",
      labelVersionRecordId: "recLabelVersionMismatch",
      source: "configurator-carousel",
    },
    getRecordOrNullImpl: async (table, recordId) => {
      if (
        table === STUDIO_TABLES.labelVersions &&
        recordId === "recLabelVersionMismatch"
      ) {
        return {
          id: "recLabelVersionMismatch",
          fields: {
            [STUDIO_FIELDS.labelVersions.sessionId]: "session-two",
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          },
        };
      }
      return null;
    },
    findSavedConfigurationBySessionIdImpl: async () => {
      throw new Error("session resolver should not be called on early mismatch");
    },
    updateResilientImpl: async () => {
      throw new Error("update should not happen on session mismatch");
    },
  });

  assert.equal(response.status, 409);
  assert.equal(response.payload.ok, false);
  assert.equal(response.payload.error, "session_mismatch");
  assert.equal(response.payload.expectedSessionId, "session-two");
});

test("select-label-version returns design_side_mismatch when selected version belongs to another side", async () => {
  const response = await createHarness({
    body: {
      sessionId: "session-side",
      designSide: "front",
      labelVersionRecordId: "recLabelVersionSideMismatch",
      source: "configurator-carousel",
    },
    getRecordOrNullImpl: async (table, recordId) => {
      if (
        table === STUDIO_TABLES.labelVersions &&
        recordId === "recLabelVersionSideMismatch"
      ) {
        return {
          id: "recLabelVersionSideMismatch",
          fields: {
            [STUDIO_FIELDS.labelVersions.sessionId]: "session-side",
            [STUDIO_FIELDS.labelVersions.designSide]: "Back",
          },
        };
      }
      return null;
    },
    findSavedConfigurationBySessionIdImpl: async () => {
      throw new Error("session resolver should not be called on early mismatch");
    },
    updateResilientImpl: async () => {
      throw new Error("update should not happen on side mismatch");
    },
  });

  assert.equal(response.status, 409);
  assert.equal(response.payload.ok, false);
  assert.equal(response.payload.error, "design_side_mismatch");
  assert.equal(response.payload.expectedDesignSide, "back");
});
