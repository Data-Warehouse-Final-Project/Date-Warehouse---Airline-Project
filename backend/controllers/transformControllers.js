const { supabase } = require('../config/supabaseClient');
const { produce } = require('../kafkaProducer');
const fs = require('fs');

async function insertIntoTable(table, rows, onConflict) {
  if (!rows || rows.length === 0) return { inserted: 0 };
  if (onConflict) {
    // use upsert when an onConflict key is provided
    const { data, error } = await supabase.from(table).upsert(rows, { onConflict });
    if (error) throw error;
    return { inserted: Array.isArray(data) ? data.length : 0 };
  } else {
    const { data, error } = await supabase.from(table).insert(rows);
    if (error) throw error;
    return { inserted: Array.isArray(data) ? data.length : 0 };
  }
}

/**
 * Basic SCD2 handler. Expects config: { dimension_table, natural_key }
 * This implementation is generic and assumes dimension rows contain the
 * natural key and that the dimension has `valid_from` and `valid_to` columns.
 */
async function handleSCD2(dimensionTable, naturalKey, rows) {
  const now = new Date().toISOString();
  for (const r of rows) {
    const keyValue = r[naturalKey];
    if (keyValue === undefined) continue;

    // find active dimension rows
    const { data: existing, error: selErr } = await supabase
      .from(dimensionTable)
      .select('*')
      .eq(naturalKey, keyValue)
      .is('valid_to', null)
      .limit(1);
    if (selErr) throw selErr;

    if (!existing || existing.length === 0) {
      // insert new active row
      const newRow = Object.assign({}, r, { valid_from: now, valid_to: null });
      const { error: insErr } = await supabase.from(dimensionTable).insert(newRow);
      if (insErr) throw insErr;
    } else {
      const cur = existing[0];
      // naive comparison: stringify and compare. In practice, use CRC or specific monitored columns.
      const curJson = JSON.stringify(cur);
      const newJson = JSON.stringify(r);
      if (curJson.includes('_deleted')) {
        // skip deleted markers
      }
      if (curJson !== newJson) {
        // close existing row
        const { error: updErr } = await supabase
          .from(dimensionTable)
          .update({ valid_to: now })
          .eq('id', cur.id);
        if (updErr) throw updErr;

        // insert new row
        const newRow = Object.assign({}, r, { valid_from: now, valid_to: null });
        const { error: insErr } = await supabase.from(dimensionTable).insert(newRow);
        if (insErr) throw insErr;
      }
    }
  }
}

/**
 * Orchestrate a simplified ETL flow: staging -> pre-fact -> dims -> facts
 * `config` should have staging_table, pre_fact_table, dimension_table, fact_table, natural_key, scdType
 */
async function runFullETL(config, rows) {
  if (!config) throw new Error('ETL config required');
  const logs = [];

  // 1) Load into staging (may upsert when config provides `staging_on_conflict`)
  try {
    const s = await insertIntoTable(config.staging_table, rows, config.staging_on_conflict);
    logs.push({ step: 'staging', inserted: s.inserted });
  } catch (err) {
    logs.push({ step: 'staging', error: err.message });
    throw err;
  }

  // 2) Pre-fact: copy from staging to pre-fact (simplified)
  try {
    // For simplicity we re-use the same rows. Allow upsert if configured.
    const p = await insertIntoTable(config.pre_fact_table, rows, config.pre_fact_on_conflict);
    logs.push({ step: 'pre_fact', inserted: p.inserted });
  } catch (err) {
    logs.push({ step: 'pre_fact', error: err.message });
    throw err;
  }

  // 3) Dimensions (SCD handling)
  try {
    if (config.scdType === 2) {
      await handleSCD2(config.dimension_table, config.natural_key, rows);
      logs.push({ step: 'dimensions', scd: 'type2' });
    } else {
      // Type 1 upsert
      const { error: upErr } = await supabase.from(config.dimension_table).upsert(rows, { onConflict: config.natural_key });
      if (upErr) throw upErr;
      logs.push({ step: 'dimensions', scd: 'type1' });
    }
  } catch (err) {
    logs.push({ step: 'dimensions', error: err.message });
    throw err;
  }

  // 4) Facts: insert into fact table
  try {
    const f = await insertIntoTable(config.fact_table, rows, config.fact_on_conflict);
    logs.push({ step: 'facts', inserted: f.inserted });
  } catch (err) {
    logs.push({ step: 'facts', error: err.message });
    throw err;
  }

  // 5) Produce Kafka event that ETL finished for this table
  try {
    await produce('etl-events', { table: config.fact_table, status: 'completed', timestamp: new Date().toISOString() });
    logs.push({ step: 'kafka', produced: true });
  } catch (err) {
    logs.push({ step: 'kafka', error: err.message });
  }

  return logs;
}

module.exports = { runFullETL };
