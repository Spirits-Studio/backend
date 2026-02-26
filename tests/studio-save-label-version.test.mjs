import test from "node:test";
import assert from "node:assert/strict";

import {
  createStudioSaveLabelVersionHandler,
} from "../netlify/functions/studio-save-label-version.js";
import { STUDIO_FIELDS, STUDIO_TABLES } from "../netlify/functions/_lib/studio.js";

const CUSTOMER_ID = "recCustomerA";
const OTHER_CUSTOMER_ID = "recCustomerB";

const sendJsonForTests = (status, body) => ({ status, body });

const baseBody = () => ({
  customer_record_id: CUSTOMER_ID,
  design_side: "front",
  version_kind: "Edit",
  accepted: true,
  output_image_url: "https://example.com/output.png",
});

test("explicit label id path rejects label owned by another customer", async () => {
  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      label_record_id: "recLabelOther",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async (table, recordId) => {
      assert.equal(table, STUDIO_TABLES.labels);
      assert.equal(recordId, "recLabelOther");
      return {
        id: "recLabelOther",
        fields: {
          [STUDIO_FIELDS.labels.customers]: [OTHER_CUSTOMER_ID],
        },
      };
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });
  assert.equal(result.status, 403);
  assert.equal(result.body.error, "label_customer_mismatch");
});

test("inferred label path rejects previous version linked to another customer's label", async () => {
  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      previous_label_version_record_id: "recPrevOther",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async (table, recordId) => {
      if (table === STUDIO_TABLES.labelVersions) {
        assert.equal(recordId, "recPrevOther");
        return {
          id: "recPrevOther",
          fields: {
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
            [STUDIO_FIELDS.labelVersions.labels]: ["recLabelOther"],
          },
        };
      }
      if (table === STUDIO_TABLES.labels) {
        assert.equal(recordId, "recLabelOther");
        return {
          id: "recLabelOther",
          fields: {
            [STUDIO_FIELDS.labels.customers]: [OTHER_CUSTOMER_ID],
          },
        };
      }
      throw new Error(`Unexpected lookup: ${table}/${recordId}`);
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });
  assert.equal(result.status, 403);
  assert.equal(result.body.error, "label_customer_mismatch");
});

test("inferred label path allows same-customer fork/edit", async () => {
  const createCalls = [];
  const updateCalls = [];

  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      previous_label_version_record_id: "recPrevOwned",
      session_id: "session-123",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async (table, recordId) => {
      if (table === STUDIO_TABLES.labelVersions) {
        assert.equal(recordId, "recPrevOwned");
        return {
          id: "recPrevOwned",
          fields: {
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
            [STUDIO_FIELDS.labelVersions.labels]: ["recLabelOwned"],
          },
        };
      }
      if (table === STUDIO_TABLES.labels) {
        assert.equal(recordId, "recLabelOwned");
        return {
          id: "recLabelOwned",
          fields: {
            [STUDIO_FIELDS.labels.customers]: [CUSTOMER_ID],
            [STUDIO_FIELDS.labels.labelVersions]: ["recVersionOld"],
          },
          createdTime: "2026-02-26T00:00:00.000Z",
        };
      }
      throw new Error(`Unexpected lookup: ${table}/${recordId}`);
    },
    computeNextVersionNumberImpl: async () => 2,
    createResilientImpl: async (table, requiredFields, optionalFields) => {
      createCalls.push({ table, requiredFields, optionalFields });
      if (table === STUDIO_TABLES.labels) {
        throw new Error("Label head should be inferred and reused, not newly created.");
      }
      if (table === STUDIO_TABLES.labelVersions) {
        return { id: "recVersionNew", createdTime: "2026-02-26T01:00:00.000Z" };
      }
      throw new Error(`Unexpected create: ${table}`);
    },
    updateResilientImpl: async (table, recordId, requiredFields, optionalFields) => {
      updateCalls.push({ table, recordId, requiredFields, optionalFields });
      return { id: recordId };
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });

  assert.equal(result.status, 200);
  assert.equal(result.body.customer_record_id, CUSTOMER_ID);
  assert.equal(result.body.label_record_id, "recLabelOwned");
  assert.equal(result.body.label_version_record_id, "recVersionNew");
  assert.equal(createCalls.length, 1);
  assert.equal(createCalls[0].table, STUDIO_TABLES.labelVersions);
  assert.equal(
    createCalls[0].optionalFields[STUDIO_FIELDS.labelVersions.sessionId],
    "session-123"
  );
  assert.equal(updateCalls.length, 1);
  assert.equal(updateCalls[0].table, STUDIO_TABLES.labels);
  assert.equal(updateCalls[0].recordId, "recLabelOwned");
  assert.equal(
    updateCalls[0].optionalFields[STUDIO_FIELDS.labels.sessionId],
    "session-123"
  );
});

test("rejected attempts are not persisted", async () => {
  let createCalls = 0;
  let updateCalls = 0;

  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      accepted: false,
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    createResilientImpl: async () => {
      createCalls += 1;
      return { id: "recUnexpectedCreate" };
    },
    updateResilientImpl: async () => {
      updateCalls += 1;
      return { id: "recUnexpectedUpdate" };
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });
  assert.equal(result.status, 400);
  assert.equal(result.body.error, "not_accepted");
  assert.equal(createCalls, 0);
  assert.equal(updateCalls, 0);
});

test("inferred label path backfills session id from linked label when omitted", async () => {
  const createCalls = [];
  const updateCalls = [];

  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      previous_label_version_record_id: "recPrevOwned",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async (table, recordId) => {
      if (table === STUDIO_TABLES.labelVersions) {
        assert.equal(recordId, "recPrevOwned");
        return {
          id: "recPrevOwned",
          fields: {
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
            [STUDIO_FIELDS.labelVersions.labels]: ["recLabelOwned"],
          },
        };
      }
      if (table === STUDIO_TABLES.labels) {
        assert.equal(recordId, "recLabelOwned");
        return {
          id: "recLabelOwned",
          fields: {
            [STUDIO_FIELDS.labels.customers]: [CUSTOMER_ID],
            [STUDIO_FIELDS.labels.sessionId]: "session-from-label",
          },
          createdTime: "2026-02-26T00:00:00.000Z",
        };
      }
      throw new Error(`Unexpected lookup: ${table}/${recordId}`);
    },
    computeNextVersionNumberImpl: async () => 3,
    createResilientImpl: async (table, requiredFields, optionalFields) => {
      createCalls.push({ table, requiredFields, optionalFields });
      if (table === STUDIO_TABLES.labelVersions) {
        return { id: "recVersionNew", createdTime: "2026-02-26T01:00:00.000Z" };
      }
      throw new Error(`Unexpected create: ${table}`);
    },
    updateResilientImpl: async (table, recordId, requiredFields, optionalFields) => {
      updateCalls.push({ table, recordId, requiredFields, optionalFields });
      return { id: recordId };
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });

  assert.equal(result.status, 200);
  assert.equal(result.body.session_id, "session-from-label");
  assert.equal(createCalls.length, 1);
  assert.equal(
    createCalls[0].optionalFields[STUDIO_FIELDS.labelVersions.sessionId],
    "session-from-label"
  );
  assert.equal(updateCalls.length, 1);
  assert.equal(
    updateCalls[0].optionalFields[STUDIO_FIELDS.labels.sessionId],
    "session-from-label"
  );
});

test("rejects data-url input_logo_url blobs", async () => {
  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      input_logo_url: "data:image/png;base64,AAAABBBB",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async () => null,
    createResilientImpl: async (table) => {
      if (table === STUDIO_TABLES.labels) {
        return {
          id: "recLabelCreated",
          fields: {
            [STUDIO_FIELDS.labels.customers]: [CUSTOMER_ID],
          },
        };
      }
      throw new Error("Version create should not be reached for invalid input refs.");
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });
  assert.equal(result.status, 400);
  assert.equal(result.body.error, "invalid_input_logo_url");
});

test("persists valid input logo/character URL pointers", async () => {
  const createCalls = [];

  const handler = createStudioSaveLabelVersionHandler({
    parseBodyImpl: async () => ({
      ...baseBody(),
      input_logo_url: "https://cdn.example.com/logo.png",
      input_character_url: "https://cdn.example.com/character.png",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async () => null,
    computeNextVersionNumberImpl: async () => 1,
    createResilientImpl: async (table, requiredFields, optionalFields) => {
      createCalls.push({ table, requiredFields, optionalFields });
      if (table === STUDIO_TABLES.labels) {
        return {
          id: "recLabelCreated",
          fields: {
            [STUDIO_FIELDS.labels.customers]: [CUSTOMER_ID],
            [STUDIO_FIELDS.labels.labelVersions]: [],
          },
          createdTime: "2026-02-26T00:00:00.000Z",
        };
      }
      if (table === STUDIO_TABLES.labelVersions) {
        return { id: "recVersionCreated", createdTime: "2026-02-26T01:00:00.000Z" };
      }
      throw new Error(`Unexpected create: ${table}`);
    },
    updateResilientImpl: async (table, recordId) => ({ table, recordId }),
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });
  assert.equal(result.status, 200);

  const versionCreate = createCalls.find((c) => c.table === STUDIO_TABLES.labelVersions);
  assert.ok(versionCreate, "Expected label version create call");
  assert.equal(
    versionCreate.optionalFields[STUDIO_FIELDS.labelVersions.inputLogoUrl],
    "https://cdn.example.com/logo.png"
  );
  assert.equal(
    versionCreate.optionalFields[STUDIO_FIELDS.labelVersions.inputCharacterUrl],
    "https://cdn.example.com/character.png"
  );
});
