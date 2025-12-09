const { supabase } = require('../config/supabaseClient');
const { parse } = require('csv-parse/sync');

/**
 * Upload and parse CSV file, then insert rows into Fact_BookingSales.
 * Expects a multipart form file named `file`.
 */
exports.uploadCSV = async (req, res) => {
  try {
    if (!req.files || !req.files.file) {
      return res.status(400).json({ error: 'No file uploaded. Use form field `file`.' });
    }

    const uploaded = req.files.file; // express-fileupload puts file here
    const csvText = uploaded.data.toString('utf8');

    const records = parse(csvText, {
      columns: true,
      skip_empty_lines: true
    });

    if (!Array.isArray(records) || records.length === 0) {
      return res.status(400).json({ error: 'CSV parsed to no rows' });
    }

    // Insert in chunks to avoid very large payloads
    const chunkSize = 500; // adjust as needed
    let inserted = 0;
    for (let i = 0; i < records.length; i += chunkSize) {
      const chunk = records.slice(i, i + chunkSize);
      const { data, error } = await supabase.from('Fact_BookingSales').insert(chunk);
      if (error) {
        console.error('Supabase insert error:', error);
        return res.status(500).json({ error: 'Error inserting rows', details: error });
      }
      inserted += Array.isArray(data) ? data.length : 0;
    }

    return res.json({ ok: true, inserted, total: records.length });
  } catch (err) {
    console.error('uploadCSV error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
};
