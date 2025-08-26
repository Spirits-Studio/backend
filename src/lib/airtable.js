const base = () => `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}`;
const auth = { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` };

export async function findOneBy(table, field, value) {
  console.log("airtable findOneBy hit")
  console.log("airtable findOneBy table", table)
  console.log("airtable findOneBy field", field)
  console.log("airtable findOneBy value", value)
  const url = new URL(`${base()}/${encodeURIComponent(table)}`);
  url.searchParams.set('maxRecords', '1');
  url.searchParams.set('filterByFormula', `({${field}}='${String(value).replace(/'/g,"\\'")}')`);
  const r = await fetch(url, { headers: auth });
  const j = await r.json();
  return j.records?.[0] || null;
}

export async function createOne(table, fields) {
  console.log("airtable createOne hit")
  console.log("airtable createOne table", table)
  console.log("airtable createOne fields", fields)
  const r = await fetch(`${base()}/${encodeURIComponent(table)}`, {
    method: 'POST',
    headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({ records: [{ fields }] })
  });
  if (!r.ok) throw new Error(`Airtable create failed: ${await r.text()}`);
  const j = await r.json();
  return j.records[0];
}

export async function updateOne(table, recordId, fields) {
  console.log("airtable updateOne hit")
  console.log("airtable updateOne table", table)
  console.log("airtable updateOne recordId", recordId)
  console.log("airtable updateOne fields", fields)
  const r = await fetch(`${base()}/${encodeURIComponent(table)}`, {
    method: 'PATCH',
    headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({ records: [{ id: recordId, fields }] })
  });
  if (!r.ok) throw new Error(`Airtable update failed: ${await r.text()}`);
  const j = await r.json();
  return j.records[0];
}