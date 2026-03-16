const AIRTABLE_API_BASE = 'https://api.airtable.com/v0';
const VALID_SIDES = new Set(['front', 'back']);

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type': 'application/json',
};

const LABEL_VERSION_FIELDS = {
  sessionId: ['Session ID', 'session_id', 'sessionId', 'SessionId'],
  designSide: ['Design Side', 'design_side', 'designSide', 'Side'],
  versionNumber: ['Version Number', 'version_number', 'versionNumber'],
  versionKind: ['Version Kind', 'version_kind', 'versionKind'],
  outputImageUrl: ['Output Image URL', 'output_image_url', 'outputImageUrl'],
  outputPdfUrl: ['Output PDF URL', 'output_pdf_url', 'outputPdfUrl'],
};

const SAVED_CONFIG_FIELDS = {
  sessionId: ['Session ID', 'session_id', 'sessionId', 'SessionId'],
  front: {
    selectedId: [
      'selectedFrontLabelVersionRecordId',
      'Selected Front Label Version Record ID',
      'selected_front_label_version_record_id',
    ],
    selectedAt: [
      'selectedFrontLabelVersionAt',
      'Selected Front Label Version At',
      'selected_front_label_version_at',
    ],
    selectedBy: [
      'selectedFrontLabelVersionBy',
      'Selected Front Label Version By',
      'selected_front_label_version_by',
    ],
  },
  back: {
    selectedId: [
      'selectedBackLabelVersionRecordId',
      'Selected Back Label Version Record ID',
      'selected_back_label_version_record_id',
    ],
    selectedAt: [
      'selectedBackLabelVersionAt',
      'Selected Back Label Version At',
      'selected_back_label_version_at',
    ],
    selectedBy: [
      'selectedBackLabelVersionBy',
      'Selected Back Label Version By',
      'selected_back_label_version_by',
    ],
  },
};

const toJson = (statusCode, body) => ({
  statusCode,
  headers: CORS_HEADERS,
  body: JSON.stringify(body),
});

const normalizeSide = (value) => String(value || '').trim().toLowerCase();

const pickFirstValue = (fields, candidates) => {
  if (!fields || !Array.isArray(candidates)) return undefined;
  for (const key of candidates) {
    if (Object.prototype.hasOwnProperty.call(fields, key)) {
      return fields[key];
    }
  }
  return undefined;
};

const pickFieldName = (fields, candidates, fallback) => {
  for (const key of candidates) {
    if (Object.prototype.hasOwnProperty.call(fields || {}, key)) {
      return key;
    }
  }
  return fallback;
};

const asString = (value) => {
  if (Array.isArray(value)) return String(value[0] || '').trim();
  return String(value || '').trim();
};

const asNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const escapeFormulaString = (value) =>
  String(value || '')
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'");

const envConfig = () => ({
  token:
    process.env.AIRTABLE_PERSONAL_ACCESS_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_TOKEN ||
    '',
  baseId: process.env.AIRTABLE_BASE_ID || '',
  labelVersionsTable: process.env.AIRTABLE_TABLE_LABEL_VERSIONS || 'Label Versions',
  savedConfigurationsTable:
    process.env.AIRTABLE_TABLE_SAVED_CONFIGURATIONS || 'Saved Configurations',
  selectionAuditTable:
    process.env.AIRTABLE_TABLE_LABEL_VERSION_SELECTIONS || '',
});

const airtableRequest = async ({
  config,
  table,
  method = 'GET',
  recordId = '',
  query = undefined,
  body = undefined,
}) => {
  const pathname = `${AIRTABLE_API_BASE}/${encodeURIComponent(config.baseId)}/${encodeURIComponent(table)}${recordId ? `/${encodeURIComponent(recordId)}` : ''}`;
  const url = new URL(pathname);

  if (query && typeof query === 'object') {
    Object.entries(query).forEach(([key, value]) => {
      if (value == null) return;
      url.searchParams.set(key, String(value));
    });
  }

  const response = await fetch(url.toString(), {
    method,
    headers: {
      Authorization: `Bearer ${config.token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(json?.error?.message || `Airtable request failed: ${response.status}`);
    error.statusCode = response.status;
    error.payload = json;
    throw error;
  }

  return json;
};

const findSavedConfigurationBySession = async (config, sessionId) => {
  for (const fieldName of SAVED_CONFIG_FIELDS.sessionId) {
    const formula = `{${fieldName}}='${escapeFormulaString(sessionId)}'`;
    try {
      const res = await airtableRequest({
        config,
        table: config.savedConfigurationsTable,
        query: { maxRecords: 1, filterByFormula: formula },
      });
      if (Array.isArray(res.records) && res.records.length > 0) {
        return { record: res.records[0], matchedSessionField: fieldName };
      }
    } catch (error) {
      if (error?.statusCode === 422) {
        // Unknown field name on this base variant; try next candidate.
        continue;
      }
      throw error;
    }
  }
  return { record: null, matchedSessionField: null };
};

const buildSelectedLabelVersionPayload = (record, selectedAt, source, designSide) => {
  const fields = record?.fields || {};
  return {
    recordId: record?.id || null,
    versionNumber: asNumber(pickFirstValue(fields, LABEL_VERSION_FIELDS.versionNumber)),
    versionKind: asString(pickFirstValue(fields, LABEL_VERSION_FIELDS.versionKind)) || null,
    outputImageUrl: asString(pickFirstValue(fields, LABEL_VERSION_FIELDS.outputImageUrl)) || null,
    outputPdfUrl: asString(pickFirstValue(fields, LABEL_VERSION_FIELDS.outputPdfUrl)) || null,
    selectedAt,
    source,
    designSide,
  };
};

const writeSelectionAudit = async ({
  config,
  sessionId,
  designSide,
  labelVersionRecordId,
  source,
  selectedAt,
  savedConfigurationRecordId,
}) => {
  if (!config.selectionAuditTable) return;
  try {
    await airtableRequest({
      config,
      table: config.selectionAuditTable,
      method: 'POST',
      body: {
        records: [
          {
            fields: {
              sessionId,
              designSide,
              labelVersionRecordId,
              source,
              selectedAt,
              savedConfigurationRecordId,
            },
          },
        ],
      },
    });
  } catch (error) {
    console.warn('[select-label-version] Optional selection audit write failed', error?.message || error);
  }
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return toJson(405, { ok: false, error: 'method_not_allowed' });
  }

  const config = envConfig();
  if (!config.token || !config.baseId) {
    return toJson(500, { ok: false, error: 'airtable_env_missing' });
  }

  let payload;
  try {
    payload = event.body ? JSON.parse(event.body) : {};
  } catch {
    return toJson(400, { ok: false, error: 'invalid_json' });
  }

  const sessionId = asString(payload.sessionId);
  const designSide = normalizeSide(payload.designSide);
  const labelVersionRecordId = asString(payload.labelVersionRecordId);
  const source = asString(payload.source);
  const selectedAt = asString(payload.selectedAt) || new Date().toISOString();

  console.info('[trace:s3:backend:select-label-version:request]', {
    sessionId: sessionId || null,
    designSide: designSide || null,
    labelVersionRecordId: labelVersionRecordId || null,
    source: source || null,
    selectedAt,
  });

  if (!sessionId) return toJson(400, { ok: false, error: 'missing_session_id' });
  if (!VALID_SIDES.has(designSide)) return toJson(400, { ok: false, error: 'invalid_design_side' });
  if (!labelVersionRecordId) return toJson(400, { ok: false, error: 'missing_label_version_record_id' });
  if (!source) return toJson(400, { ok: false, error: 'missing_source' });

  try {
    const labelVersionRecord = await airtableRequest({
      config,
      table: config.labelVersionsTable,
      recordId: labelVersionRecordId,
    });

    const lvFields = labelVersionRecord?.fields || {};
    const lvSessionId = asString(pickFirstValue(lvFields, LABEL_VERSION_FIELDS.sessionId));
    const lvDesignSide = normalizeSide(pickFirstValue(lvFields, LABEL_VERSION_FIELDS.designSide));

    if (lvSessionId && lvSessionId !== sessionId) {
      return toJson(409, {
        ok: false,
        error: 'session_mismatch',
        expectedSessionId: lvSessionId,
      });
    }
    if (lvDesignSide && lvDesignSide !== designSide) {
      return toJson(409, {
        ok: false,
        error: 'design_side_mismatch',
        expectedDesignSide: lvDesignSide,
      });
    }

    const { record: savedConfigRecord } = await findSavedConfigurationBySession(config, sessionId);
    if (!savedConfigRecord) {
      return toJson(404, { ok: false, error: 'saved_configuration_not_found' });
    }

    const savedFields = savedConfigRecord.fields || {};
    const sideFields = SAVED_CONFIG_FIELDS[designSide];
    const selectedIdField = pickFieldName(savedFields, sideFields.selectedId, sideFields.selectedId[0]);
    const selectedAtField = pickFieldName(savedFields, sideFields.selectedAt, sideFields.selectedAt[0]);
    const selectedByField = pickFieldName(savedFields, sideFields.selectedBy, sideFields.selectedBy[0]);
    const currentSelectedId = asString(savedFields[selectedIdField]);

    const selectedLabelVersion = buildSelectedLabelVersionPayload(
      labelVersionRecord,
      selectedAt,
      source,
      designSide
    );
    console.info('[trace:s3:backend:select-label-version:resolved]', {
      sessionId,
      designSide,
      labelVersionRecordId,
      currentSelectedId: currentSelectedId || null,
      candidateOutputImageUrl: selectedLabelVersion?.outputImageUrl || null,
      candidateOutputPdfUrl: selectedLabelVersion?.outputPdfUrl || null,
    });

    if (currentSelectedId === labelVersionRecordId) {
      console.info('[trace:s3:backend:select-label-version:idempotent]', {
        sessionId,
        designSide,
        labelVersionRecordId,
        currentSelectedId: currentSelectedId || null,
        outputImageUrl: selectedLabelVersion?.outputImageUrl || null,
      });
      return toJson(200, {
        ok: true,
        idempotent: true,
        sessionId,
        designSide,
        selectedLabelVersion,
      });
    }

    const updateFields = {
      [selectedIdField]: labelVersionRecordId,
      [selectedAtField]: selectedAt,
      [selectedByField]: source,
    };

    try {
      await airtableRequest({
        config,
        table: config.savedConfigurationsTable,
        method: 'PATCH',
        recordId: savedConfigRecord.id,
        body: { fields: updateFields },
      });
    } catch (error) {
      // Some bases may keep selected version as a linked-record field.
      if (error?.statusCode === 422) {
        await airtableRequest({
          config,
          table: config.savedConfigurationsTable,
          method: 'PATCH',
          recordId: savedConfigRecord.id,
          body: {
            fields: {
              ...updateFields,
              [selectedIdField]: [labelVersionRecordId],
            },
          },
        });
      } else {
        throw error;
      }
    }

    await writeSelectionAudit({
      config,
      sessionId,
      designSide,
      labelVersionRecordId,
      source,
      selectedAt,
      savedConfigurationRecordId: savedConfigRecord.id,
    });

    console.info('[trace:s3:backend:select-label-version:updated]', {
      sessionId,
      designSide,
      previousSelectedId: currentSelectedId || null,
      nextSelectedId: labelVersionRecordId,
      outputImageUrl: selectedLabelVersion?.outputImageUrl || null,
      outputPdfUrl: selectedLabelVersion?.outputPdfUrl || null,
      savedConfigurationRecordId: savedConfigRecord.id,
    });

    return toJson(200, {
      ok: true,
      idempotent: false,
      sessionId,
      designSide,
      selectedLabelVersion,
    });
  } catch (error) {
    const statusCode = Number(error?.statusCode) || 500;
    if (statusCode === 404) {
      return toJson(404, { ok: false, error: 'label_version_not_found' });
    }
    console.error('[select-label-version] Unhandled error', error);
    return toJson(statusCode >= 400 && statusCode < 600 ? statusCode : 500, {
      ok: false,
      error: 'select_label_version_failed',
      message: error?.message || 'Unknown error',
    });
  }
};
