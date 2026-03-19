import test from "node:test";
import assert from "node:assert/strict";

import {
  createStudioResetLabelLineageHandler,
} from "../netlify/functions/studio-reset-label-lineage.js";
import { STUDIO_FIELDS, STUDIO_TABLES } from "../netlify/functions/_lib/studio.js";

const CUSTOMER_ID = "recCustomerA";
const OTHER_CUSTOMER_ID = "recCustomerB";

const sendJsonForTests = (status, body) => ({ status, body });

const createHandler = (overrides = {}) =>
  createStudioResetLabelLineageHandler({
    resolveCustomerRecordIdOrCreateImpl: async ({ providedCustomerRecordId }) => ({
      customerRecordId: providedCustomerRecordId,
      recovered: false,
    }),
    ...overrides,
  });

test("reset-label-lineage deletes prior lineage and returns a new label head", async () => {
  const deleteCalls = [];
  const createCalls = [];

  const handler = createHandler({
    parseBodyImpl: async () => ({
      customer_record_id: CUSTOMER_ID,
      design_side: "front",
      label_record_id: "recLabelOwned",
      previous_label_version_record_id: "recVersion2",
      session_id: "session-123",
      label_display_name: "Bottle QA Front",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async (table, recordId) => {
      if (table === STUDIO_TABLES.labels) {
        assert.equal(recordId, "recLabelOwned");
        return {
          id: "recLabelOwned",
          fields: {
            [STUDIO_FIELDS.labels.customers]: [CUSTOMER_ID],
            [STUDIO_FIELDS.labels.displayName]: "Old Front Label",
            [STUDIO_FIELDS.labels.sessionId]: "session-from-label",
          },
        };
      }
      if (table === STUDIO_TABLES.labelVersions) {
        assert.equal(recordId, "recVersion2");
        return {
          id: "recVersion2",
          fields: {
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
            [STUDIO_FIELDS.labelVersions.labels]: ["recLabelOwned"],
            [STUDIO_FIELDS.labelVersions.sessionId]: "session-from-version",
          },
        };
      }
      throw new Error(`Unexpected lookup: ${table}/${recordId}`);
    },
    listRecordsByLinkedRecordIdsImpl: async (table, { fieldName, linkedRecordIds }) => {
      assert.equal(table, STUDIO_TABLES.labelVersions);
      assert.equal(fieldName, STUDIO_FIELDS.labelVersions.labels);
      assert.equal(linkedRecordIds, "recLabelOwned");
      return [
        {
          id: "recVersion1",
          fields: {
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          },
        },
        {
          id: "recVersion2",
          fields: {
            [STUDIO_FIELDS.labelVersions.designSide]: "Front",
          },
        },
      ];
    },
    deleteRecordOrNullImpl: async (table, recordId) => {
      deleteCalls.push({ table, recordId });
      return { id: recordId, deleted: true };
    },
    createResilientImpl: async (table, _requiredFields, optionalFields) => {
      createCalls.push({ table, optionalFields });
      assert.equal(table, STUDIO_TABLES.labels);
      assert.deepEqual(optionalFields[STUDIO_FIELDS.labels.customers], [CUSTOMER_ID]);
      assert.equal(optionalFields[STUDIO_FIELDS.labels.displayName], "Bottle QA Front");
      assert.equal(optionalFields[STUDIO_FIELDS.labels.sessionId], "session-123");
      return { id: "recLabelFresh", createdTime: "2026-03-17T12:00:00.000Z" };
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });

  assert.equal(result.status, 200);
  assert.equal(result.body.ok, true);
  assert.equal(result.body.new_label_record_id, "recLabelFresh");
  assert.equal(result.body.deleted_label_record_id, "recLabelOwned");
  assert.deepEqual(result.body.deleted_label_version_record_ids, ["recVersion1", "recVersion2"]);
  assert.deepEqual(deleteCalls, [
    { table: STUDIO_TABLES.labelVersions, recordId: "recVersion1" },
    { table: STUDIO_TABLES.labelVersions, recordId: "recVersion2" },
    { table: STUDIO_TABLES.labels, recordId: "recLabelOwned" },
  ]);
  assert.equal(createCalls.length, 1);
});

test("reset-label-lineage rejects labels owned by another customer", async () => {
  let deleteCalls = 0;
  let createCalls = 0;

  const handler = createHandler({
    parseBodyImpl: async () => ({
      customer_record_id: CUSTOMER_ID,
      design_side: "front",
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
    deleteRecordOrNullImpl: async () => {
      deleteCalls += 1;
      return null;
    },
    createResilientImpl: async () => {
      createCalls += 1;
      return { id: "recUnexpected" };
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });

  assert.equal(result.status, 403);
  assert.equal(result.body.error, "label_customer_mismatch");
  assert.equal(deleteCalls, 0);
  assert.equal(createCalls, 0);
});

test("reset-label-lineage returns stale-id error when previous version no longer exists", async () => {
  const handler = createHandler({
    parseBodyImpl: async () => ({
      customer_record_id: CUSTOMER_ID,
      design_side: "front",
      previous_label_version_record_id: "recMissingPrev",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    getRecordOrNullImpl: async (table, recordId) => {
      assert.equal(table, STUDIO_TABLES.labelVersions);
      assert.equal(recordId, "recMissingPrev");
      return null;
    },
  });

  const result = await handler({}, { qs: {}, isV2: false, method: "POST" });

  assert.equal(result.status, 404);
  assert.equal(result.body.error, "previous_version_not_found");
});

test("reset-label-lineage is idempotent per customer+side+idempotency key", async () => {
  const idempotencyCache = new Map();
  let lookupCalls = 0;
  let deleteCalls = 0;
  let createCalls = 0;

  const handler = createHandler({
    parseBodyImpl: async () => ({
      customer_record_id: CUSTOMER_ID,
      design_side: "front",
      label_record_id: "recLabelOwned",
      idempotency_key: "reset-front-1",
    }),
    assertPayloadSizeImpl: () => {},
    sendJsonImpl: sendJsonForTests,
    idempotencyCache,
    nowImpl: () => Date.UTC(2026, 2, 17, 12, 0, 0),
    getRecordOrNullImpl: async (table, recordId) => {
      lookupCalls += 1;
      assert.equal(table, STUDIO_TABLES.labels);
      assert.equal(recordId, "recLabelOwned");
      return {
        id: "recLabelOwned",
        fields: {
          [STUDIO_FIELDS.labels.customers]: [CUSTOMER_ID],
        },
      };
    },
    listRecordsByLinkedRecordIdsImpl: async () => [
      {
        id: "recVersion1",
        fields: {
          [STUDIO_FIELDS.labelVersions.designSide]: "Front",
        },
      },
    ],
    deleteRecordOrNullImpl: async (_table, recordId) => {
      deleteCalls += 1;
      return { id: recordId, deleted: true };
    },
    createResilientImpl: async () => {
      createCalls += 1;
      return { id: "recLabelFresh", createdTime: "2026-03-17T12:01:00.000Z" };
    },
  });

  const first = await handler({}, { qs: {}, isV2: false, method: "POST" });
  const second = await handler({}, { qs: {}, isV2: false, method: "POST" });

  assert.equal(first.status, 200);
  assert.equal(first.body.idempotent, undefined);
  assert.equal(first.body.new_label_record_id, "recLabelFresh");

  assert.equal(second.status, 200);
  assert.equal(second.body.idempotent, true);
  assert.equal(second.body.new_label_record_id, "recLabelFresh");

  assert.equal(lookupCalls, 1);
  assert.equal(deleteCalls, 2);
  assert.equal(createCalls, 1);
});
