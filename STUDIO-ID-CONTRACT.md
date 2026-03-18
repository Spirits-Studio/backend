# Studio ID Contract

This contract defines canonical Airtable record ID keys and allowed fallbacks across `shopify-site`, `zakeke-full`, and `backend`.

## 1) Canonical Keys (write path)

Use these keys for all outbound requests and persisted state writes.

- `customer_record_id`: Airtable `Customers` record id (`rec...`).
- `saved_configuration_record_id`: Airtable `Saved Configurations` record id (`rec...`).
- `label_record_id`: Airtable `Labels` record id (`rec...`).
- `label_version_record_id`: Airtable `Label Versions` record id (`rec...`).
- `session_id` (or `sessionId` only where API already requires camelCase): Studio session identity.

Guardrail:
- IDs must pass Airtable-record normalization (`rec...`) before write.
- Do not write ambiguous generic keys like `airtableId`.

## 2) Allowed Read Fallbacks (compatibility only)

These are accepted only when parsing legacy payloads/responses.

- Customer: `customerRecordId`.
- Saved config: `savedConfigurationRecordId`, legacy `airtableId` (response parsing only).
- Label: `labelRecordId`.
- Label version: `labelVersionRecordId`, `labelVersionId`, `recordId` (selection payload parsing only where legacy UI can emit variants).

Guardrail:
- Parse permissively, write canonically.

## 3) Ownership + Security Rules

- Any operation targeting an existing saved configuration should enforce ownership when `customer_record_id` is known.
- For `select-label-version`:
  - If `saved_configuration_record_id` is present: resolve directly and enforce ownership.
  - Else: resolve by `sessionId`.
  - If no saved configuration is found by session: return `200` with `{ ok: true, deferred: true }`.
- Label-version selection must continue validating:
  - session consistency (`session_mismatch`),
  - side consistency (`design_side_mismatch`).

## 4) Deterministic Session Resolution

When resolving saved configurations by `sessionId`, if multiple records match:
- deterministically choose the latest (created-time descending; record-id tiebreaker),
- log collision details for diagnostics.

## 5) Shopify Data Strategy

- Do not expand new cart-property usage of `_ss_customer_airtable_id`.
- Canonical customer linkage should come from resolved identity + saved configuration linkage.
- Keep legacy webhook parsing for `_ss_customer_airtable_id` backward compatibility only.

## 6) Endpoint Contract Notes

`POST /apps/ss/studio/select-label-version`
- Required: `sessionId`, `designSide`, `labelVersionRecordId`, `source`.
- Optional: `customer_record_id`, `saved_configuration_record_id`, `selectedAt`.
- Success shapes:
  - Updated/idempotent existing config: `{ ok: true, idempotent, sessionId, designSide, selectedLabelVersion, saved_configuration_record_id }`
  - No config yet (pre-save flow): `{ ok: true, deferred: true, ... }`
