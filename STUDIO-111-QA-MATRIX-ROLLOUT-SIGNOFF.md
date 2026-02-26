# STUDIO-111 QA Matrix + Rollout Signoff (MS-12)

Last updated: 2026-02-26
Owners: FE (zakeke-full + shopify-site), BE (backend), QA, PM
Priority: P0

## Scope
This runbook is the release gate for My Studio critical paths across:
- `zakeke-full` (iframe event contract)
- `shopify-site` (Shopify host UI, cart properties, My Studio page)
- `backend` (Netlify studio APIs, webhook, Airtable persistence)

## Release Gate (Blocker Rules)
Release is blocked if any of the following is true:
- Any P0/P1 matrix scenario fails.
- Any critical path loses data or writes to the wrong customer/configuration.
- A single hard refresh of `/pages/my-studio` does not reflect latest saved state.
- Evidence package is missing request/response samples or Airtable screenshots/record IDs.

## Requirement Mapping
| Requirement | Validation method | Evidence required |
| --- | --- | --- |
| Guest and signed-in journeys, incl. two-device continuity | Execute matrix scenarios `MS12-01` through `MS12-05` | HAR/screenshot + record IDs |
| Webhook updates `Saved Configurations.Status = Ordered` via `_saved_configuration_id` | Execute `MS12-07` and `MS12-08` | Webhook payload/response + Airtable screenshot |
| Rejected attempts are not persisted | Execute `MS12-02` | 400 response sample + proof no new `Label Versions` record |
| Cart properties remain compact | Execute `MS12-06` | `/cart/add.js` payload sample + byte count |
| Fork lineage + reset-new-head behavior | Execute `MS12-09` and `MS12-10` | `save-label-version` request sample + My Studio lineage screenshot |

## Environment + Preconditions
- Test store points to current `shopify-site` theme changes.
- App proxy routes active for `/apps/ss/studio/*` and `/apps/ss/create-airtable-customer`.
- Backend deployed with Airtable env vars for `Customers`, `Saved Configurations`, `Labels`, `Label Versions`, and `Orders`.
- QA has Airtable access to capture screenshots and record IDs.
- Use two browsers/devices for continuity scenarios (Device A + Device B).

## Scenario Matrix
| ID | Sev | Journey | Auth state | Devices | Expected result |
| --- | --- | --- | --- | --- | --- |
| MS12-01 | P0 | Save configuration + accepted label | Guest | A | Saved config + label version persist for guest customer record; My Studio nav appears when content exists |
| MS12-02 | P0 | Rejected label save attempt | Guest or signed-in | A | `save-label-version` returns `400 not_accepted`; no new `Label Versions` record |
| MS12-03 | P0 | Guest-to-signed-in claim path | Guest then signed-in | A | Existing customer record is claimed/linked; prior studio data remains accessible |
| MS12-04 | P0 | Signed-in save + My Studio refresh | Signed-in | A | Saved config + labels visible after one refresh cycle |
| MS12-05 | P0 | Two-device continuity | Signed-in | A + B | Data saved on Device A appears on Device B after one refresh |
| MS12-06 | P0 | Add to cart properties size/compactness | Guest or signed-in | A | Cart payload uses compact key set; property JSON stays under threshold |
| MS12-07 | P0 | Order webhook status update | Guest or signed-in | A | Webhook updates matching `Saved Configurations` record to `Ordered` using `_saved_configuration_id` |
| MS12-08 | P1 | Webhook ignores bad/missing config id | Guest or signed-in | A | No unintended status updates when ID missing/invalid |
| MS12-09 | P0 | Reset-new-head fork (front) | Guest or signed-in | A | Reset triggers `force_new_label_head=true`; My Studio shows fork lineage |
| MS12-10 | P1 | Reset-new-head fork (back) | Guest or signed-in | A | Same as front-side behavior for back-side lineage |
| MS12-11 | P1 | Offline queue -> reconnect flush | Guest or signed-in | A | Queued studio saves flush once; no duplicated or lost records |
| MS12-12 | P0 | Post-order refresh correctness | Guest or signed-in | A | One refresh of `/pages/my-studio` reflects `Ordered` status |

## Detailed Test Steps
### MS12-01 Save configuration + accepted label (guest)
1. On configurator page as guest, generate/accept a front label and add to cart flow.
2. Capture request/response for:
- `POST /apps/ss/studio/save-label-version`
- `POST /apps/ss/studio/save-configuration`
3. Confirm response includes `saved_configuration_record_id` and `label_version_record_id`.
4. Open `/pages/my-studio`, refresh once, verify saved product/label appears.

### MS12-02 Rejected attempts are not persisted
1. From storefront console, run a direct request with `accepted: false`:
```js
await fetch('/apps/ss/studio/save-label-version', {
  method: 'POST',
  credentials: 'include',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    customer_record_id: window.currentCustomer?.ss_customer_airtable_id,
    design_side: 'front',
    version_kind: 'Edit',
    accepted: false,
    output_image_url: 'https://example.com/rejected-test.png'
  })
}).then(async (r) => ({ status: r.status, body: await r.json() }));
```
2. Verify response is `400` with `error: "not_accepted"`.
3. Confirm no new `Label Versions` Airtable row was created for this attempt.

### MS12-03 Guest -> signed-in claim continuity
1. As guest on Device A, create saved configuration/label.
2. Sign in to Shopify account on same device.
3. Capture `create-airtable-customer` request/response showing record claim/update.
4. Verify existing studio content remains visible in My Studio.

### MS12-04 Signed-in save + one refresh
1. As signed-in user, create/edit labels and save configuration.
2. Open `/pages/my-studio`, refresh once.
3. Confirm latest display name/status/version lineage is reflected.

### MS12-05 Two-device continuity (signed-in)
1. Device A (signed-in): create/save config + labels.
2. Device B (same signed-in account): open `/pages/my-studio`, refresh once.
3. Confirm same records are visible with matching Airtable record IDs.

### MS12-06 Cart properties compactness
1. Capture `POST /cart/add.js` payload from Add to Cart.
2. Validate `properties` key set is compact and expected:
- `_saved_configuration_id`
- `_session_id`
- `_label_front_version_id`
- `_label_back_version_id`
- `_preview_url`
- `Wood Closure`
- `Wax Seal`
3. Threshold check:
- `JSON.stringify(properties).length <= 1500`
- No unexpected large blobs/base64 fields in properties.

### MS12-07 Webhook ordered update by `_saved_configuration_id`
1. Complete checkout (test order) for cart item containing `_saved_configuration_id`.
2. Capture webhook request sample and webhook function response.
3. Verify Airtable:
- `Saved Configurations` record ID equals `_saved_configuration_id`
- `Status = Ordered`
- `Order` link populated.

### MS12-08 Webhook ignores missing/invalid IDs
1. Submit order payload case(s) where line item has no `_saved_configuration_id` or invalid value.
2. Verify webhook response does not report updates for invalid entries.
3. Confirm no unrelated `Saved Configurations` records were changed.

### MS12-09 / MS12-10 Fork lineage + reset-new-head
1. Create accepted label version for selected side.
2. Trigger Reset in label flow.
3. Create another accepted version for same side.
4. Capture `save-label-version` payload and verify:
- `force_new_label_head: true`
- `label_record_id: null`
5. In `/pages/my-studio` Labels tab, confirm:
- Thread shows `Forked head from ...`
- Version shows parent lineage (`Parent` or `Forked from` with previous record id).

### MS12-11 Queue flush resilience
1. Temporarily disable network and perform label/config save actions.
2. Re-enable network and trigger Add to Cart flow.
3. Verify queued operations flush successfully and do not duplicate records.

### MS12-12 One-refresh post-order correctness
1. After webhook update to `Ordered`, open `/pages/my-studio`.
2. Hard refresh once.
3. Confirm status text/pill reflects `Ordered` state.

## Evidence Checklist (Required for Signoff)
- [ ] Request/response sample: `save-label-version` accepted path
- [ ] Request/response sample: `save-label-version` rejected (`not_accepted`) path
- [ ] Request/response sample: `save-configuration`
- [ ] Request sample: `/cart/add.js` with property byte count
- [ ] Webhook request/response sample for order create
- [ ] Airtable screenshot + record ID: `Saved Configurations` (Saved -> Ordered)
- [ ] Airtable screenshot + record ID: `Label Versions` lineage chain
- [ ] Airtable screenshot + record ID: `Orders` linked record
- [ ] My Studio screenshot after one refresh with latest state
- [ ] Two-device continuity screenshot pair (Device A + Device B)

## Defect Policy
- Log every failure with scenario ID, severity, repro steps, and evidence links.
- Release decision:
- Any open P0/P1 defect => `NO-GO`.
- P2+ defects require PM + Eng explicit risk acceptance.

## Signoff Template
- Test run date:
- Environment (theme/backend build IDs):
- QA owner:
- Engineering owner:
- Matrix result: `__ / 12 passed`
- Open defects (P0/P1):
- Evidence package link:
- Final decision: `GO` / `NO-GO`

## Repo Checkpoints (for triage)
### zakeke-full
- `src/components/selector.tsx` (`AddToCart` postMessage, `labelReset` postMessage)

### shopify-site
- `sections/ss-configurator.liquid` (save-label-version/save-configuration calls, resetPending, cart properties)
- `sections/ss-order-your-bottle.liquid` (cart properties parity)
- `sections/ss-my-studio.liquid` (list refresh + lineage rendering)
- `snippets/ss-my-studio-nav.liquid` (nav visibility + refresh)
- `snippets/ss-airtable-bootstrap.liquid` (guest/signed-in customer linking)

### backend
- `netlify/functions/studio-save-label-version.js` (acceptance gate + force_new_label_head behavior)
- `netlify/functions/studio-save-configuration.js` (saved configuration persistence)
- `netlify/functions/studio-list.js` (My Studio read model)
- `netlify/functions/shopify-webhook-orders-create.js` (order -> status Ordered transition)
- `netlify/functions/_lib/studio.js` (Airtable table/field mapping)
- `tests/studio-save-label-version.test.mjs` (backend guardrails)
