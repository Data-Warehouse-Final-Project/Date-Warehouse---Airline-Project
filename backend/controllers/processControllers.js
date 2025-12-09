const { supabase } = require('../config/supabaseClient');
const { parse } = require('csv-parse/sync');
const fs = require('fs');
const path = require('path');
const { spawn, spawnSync } = require('child_process');
const { v4: uuidv4 } = require('uuid');
const runLogger = require('../logging/runLogger');
const { runFullETL } = require('./transformControllers');
const tableConfigs = require('../etl_scripts/table_configs.json');

// Heuristic: decide whether an uploaded file is already cleaned.
function looksLikeCleaned({ filename, sampleHeaders = [], forceFlag = false }, table) {
  if (forceFlag) return true;
  if (filename && filename.toLowerCase().includes('cleaned')) return true;
  // header-based cues
  const lower = sampleHeaders.map(h => (h || '').toLowerCase());
  const cues = ['cleaned', 'cleaned_at', 'is_clean', 'quarantined', 'clean_status'];
  for (const c of cues) {
    if (lower.includes(c)) return true;
  }
  // if table config lists expected_columns and they are subset of headers, consider cleaned
  try {
    const cfg = tableConfigs[table];
    if (cfg && Array.isArray(cfg.expected_columns) && cfg.expected_columns.length > 0) {
      const need = cfg.expected_columns.map(x => x.toLowerCase());
      const present = need.every(n => lower.includes(n));
      if (present) return true;
    }
  } catch (e) {}
  return false;
}

// Helper: discover available cleaner table names from both cleaners folders
function discoverCleanerTables() {
  const results = new Set();
  try {
    const mainDir = path.join(__dirname, '..', 'etl_scripts', 'cleaners');
    if (fs.existsSync(mainDir)) {
      const files = fs.readdirSync(mainDir);
      for (const f of files) {
        if (f.endsWith('.py')) {
          let name = f.replace(/\.py$/, '');
          if (name.startsWith('staging_')) name = name.replace(/^staging_/, '');
          if (name.endsWith('_cleaner')) name = name.replace(/_cleaner$/, '');
          if (name === 'example_cleaner') continue;
          results.add(name);
        }
      }
    }
  } catch (e) {
    console.warn('discoverCleanerTables mainDir error', e.message);
  }

  try {
    const stagingDir = path.join(__dirname, '..', 'etl_scripts', 'staging_script', 'cleaners');
    if (fs.existsSync(stagingDir)) {
      const files = fs.readdirSync(stagingDir);
      for (const f of files) {
        if (f.endsWith('.py')) {
          let name = f.replace(/\.py$/, '');
          if (name === '__init__') {
            // treat package-level __init__.py as a generic cleaner available for all tables
            results.add('generic');
            continue;
          }
          if (name.startsWith('staging_')) name = name.replace(/^staging_/, '');
          results.add(name);
        }
      }
    }
  } catch (e) {
    console.warn('discoverCleanerTables stagingDir error', e.message);
  }

  return Array.from(results).sort();
}

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

// List available cleaner table names
exports.listCleaners = async (req, res) => {
  try {
    const tables = discoverCleanerTables();
    return res.json({ ok: true, tables });
  } catch (err) {
    console.error('listCleaners error:', err);
    return res.status(500).json({ error: 'Failed to list cleaners' });
  }
};

// SSE stream endpoint handler: clients connect to receive live logs for a run
exports.streamLogs = (req, res) => {
  const runId = req.params.runId;
  if (!runId) return res.status(400).send('runId required');
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders && res.flushHeaders();
  // subscribe
  const unsubscribe = runLogger.subscribe(runId, res);
  req.on('close', () => {
    try { unsubscribe(); } catch (e) {}
  });
};

// helper: run a command and stream output into runLogger
function execCommandStream(runId, command, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, Object.assign({ stdio: ['ignore', 'pipe', 'pipe'] }, opts));
    proc.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      runLogger.append(runId, text.trim());
    });
    proc.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      runLogger.append(runId, text.trim());
    });
    proc.on('error', (err) => {
      runLogger.append(runId, `process error: ${err.message}`);
      reject(err);
    });
    proc.on('close', (code) => {
      runLogger.append(runId, `exit code ${code}`);
      resolve(code);
    });
  });
}

/**
 * Upload a CSV that has already been cleaned. This will save the file into
 * `datapipeline/uploads/cleaned` and then run the transformation/loading (ETL).
 * Expects multipart form field `file` and `table` specifying the target logical table.
 */
exports.uploadCleaned = async (req, res) => {
  try {
    if (!req.files || !req.files.file) {
      return res.status(400).json({ error: 'No file uploaded. Use form field `file`.' });
    }

    const uploaded = req.files.file;
    const destDir = path.join(__dirname, '..', '..', 'datapipeline', 'uploads', 'cleaned');
    await fs.promises.mkdir(destDir, { recursive: true });
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const safeName = uploaded.name ? uploaded.name.replace(/[^a-zA-Z0-9._-]/g, '_') : 'cleaned-file';
    const destFilename = `${timestamp}_${safeName}`;
    const destPath = path.join(destDir, destFilename);
    await fs.promises.writeFile(destPath, uploaded.data);

    const table = (req.body && req.body.table) || req.query.table;
    if (!table) {
      return res.status(400).json({ error: 'Missing required form field `table` specifying target table/cleaner.' });
    }

    // decide whether to run transformation
    const sampleText = (await fs.promises.readFile(destPath, 'utf8')) || '';
    const sampleRecords = parse(sampleText, { columns: true, skip_empty_lines: true });
    const sampleHeaders = sampleRecords && sampleRecords.length > 0 ? Object.keys(sampleRecords[0]) : [];

    const forceFlag = ((req.body && req.body.is_cleaned) || req.query.is_cleaned) === 'true';
    const cleanedHeuristic = looksLikeCleaned({ filename: destFilename, sampleHeaders, forceFlag }, table);

    // Start a run and stream logs
    const runId = `${new Date().toISOString().replace(/[:.]/g, '-')}_${uuidv4()}`;
    runLogger.createRun(runId);
    runLogger.append(runId, `uploadCleaned start for table=${table}, file=${destFilename}`);

    // Run transform asynchronously and stream logs
    const transformRunner = path.join(__dirname, '..', 'etl_scripts', 'run_transform.py');
    const python = process.env.PYTHON || 'python';
    const transformedDir = path.join(__dirname, '..', '..', 'datapipeline', 'uploads', 'transformed');
    await fs.promises.mkdir(transformedDir, { recursive: true });
    const transformedPath = path.join(transformedDir, destFilename);
    try {
      runLogger.append(runId, `running transform: ${python} ${transformRunner} ${table} ${destPath} -> ${transformedPath}`);
      const code = await execCommandStream(runId, python, [transformRunner, table, destPath, transformedPath]);
      if (code !== 0) {
        return res.status(500).json({ error: 'Transform failed', code, runId });
      }
      runLogger.append(runId, 'transform completed');
    } catch (err) {
      runLogger.append(runId, `transform error: ${err.message}`);
      return res.status(500).json({ error: 'Error invoking transform', details: err.message, runId });
    }

    // parse transformed CSV and run ETL
    try {
      const transformedText = await fs.promises.readFile(transformedPath, 'utf8');
      const records = parse(transformedText, { columns: true, skip_empty_lines: true });
      const config = tableConfigs[table] || {
        staging_table: `staging_${table}`,
        pre_fact_table: `prefact_${table}`,
        dimension_table: `dim_${table}`,
        fact_table: `fact_${table}`,
        natural_key: 'id',
        scdType: 1
      };
      runLogger.append(runId, `running ETL for ${table} (${records.length} rows)`);
      const etlLogs = await runFullETL(config, records);
      runLogger.append(runId, `ETL completed for ${table}`);
      return res.json({ ok: true, cleaned: path.relative(path.join(__dirname, '..', '..'), destPath), transformed: path.relative(path.join(__dirname, '..', '..'), transformedPath), etlLogs, runId });
    } catch (err) {
      runLogger.append(runId, `uploadCleaned ETL error: ${err.message}`);
      return res.status(500).json({ error: 'ETL failed', details: err.message, runId });
    }
  } catch (err) {
    console.error('uploadCleaned error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Save uploaded file into `datapipeline/uploads/incoming` for extraction.
 * Expects multipart form file named `file`.
 */
exports.extractFile = async (req, res) => {
  try {
    // Debug logs: incoming request, body and file metadata
    console.log('extractFile invoked. body keys:', Object.keys(req.body || {}));
    try {
      const shortHeaders = Object.assign({}, req.headers || {});
      if (shortHeaders.cookie) shortHeaders.cookie = '[HIDDEN]';
      if (shortHeaders.authorization) shortHeaders.authorization = '[HIDDEN]';
      console.log('Request headers (partial):', shortHeaders);
    } catch (e) {}

    if (!req.files || !req.files.file) {
      console.warn('extractFile: no files in request');
      return res.status(400).json({ error: 'No file uploaded. Use form field `file`.' });
    }

    const uploaded = req.files.file;
    console.log('Uploaded file metadata:', { name: uploaded.name, mimetype: uploaded.mimetype, size: uploaded.size });

    // Build destination directory: project-root/datapipeline/uploads/incoming
    const destDir = path.join(__dirname, '..', '..', 'datapipeline', 'uploads', 'incoming');
    // Ensure directory exists
    await fs.promises.mkdir(destDir, { recursive: true });

    // Use original filename with timestamp to avoid collisions
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const safeName = uploaded.name ? uploaded.name.replace(/[^a-zA-Z0-9._-]/g, '_') : 'uploaded-file';
    const destFilename = `${timestamp}_${safeName}`;
    const destPath = path.join(destDir, destFilename);

    // Save file buffer to destination
    await fs.promises.writeFile(destPath, uploaded.data);

    // After saving to incoming, run the Python cleaner. `table` must be provided by frontend.
    const table = (req.body && req.body.table) || req.query.table;
    if (!table) {
      return res.status(400).json({ error: 'Missing required form field `table` specifying target table/cleaner.' });
    }

    // Read a small sample of the file to inspect headers for header-based matching
      const sampleText = (await fs.promises.readFile(destPath, 'utf8')) || '';
      const sampleRecords = parse(sampleText, { columns: true, skip_empty_lines: true });
      const sampleHeaders = sampleRecords && sampleRecords.length > 0 ? Object.keys(sampleRecords[0]) : [];

      // Validate table exists among available cleaners OR matches expected columns in table config
      const available = discoverCleanerTables();
      const hasGeneric = available.includes('generic');
      let acceptTable = available.includes(table) || hasGeneric;

      // If not present as a named cleaner, try header-based match using table_configs expected_columns
      if (!acceptTable) {
        try {
          const cfg = tableConfigs[table];
          if (cfg && Array.isArray(cfg.expected_columns) && cfg.expected_columns.length > 0 && sampleHeaders.length > 0) {
            const need = cfg.expected_columns.map(x => x.toLowerCase());
            const present = sampleHeaders.map(h => (h || '').toLowerCase());
            const isSubset = need.every(n => present.includes(n));
            if (isSubset) {
              acceptTable = true;
              runLogger && runLogger.append && runLogger.append('system', `header-match accepted table=${table}`);
            }
          }
        } catch (e) {
          // ignore and fall through to rejection
        }
      }

      if (!acceptTable) {
        return res.status(400).json({ error: 'Unknown table/cleaner', details: { table, available, sampleHeaders } });
      }
    const forceFlag = ((req.body && req.body.is_cleaned) || req.query.is_cleaned) === 'true';
    const cleanedHeuristic = looksLikeCleaned({ filename: destFilename, sampleHeaders, forceFlag }, table);

    // paths
    const cleanedDir = path.join(__dirname, '..', '..', 'datapipeline', 'uploads', 'cleaned');
    await fs.promises.mkdir(cleanedDir, { recursive: true });
    let cleanedPath = path.join(cleanedDir, destFilename);

    let transformedAlready = false;
    const runId = `${new Date().toISOString().replace(/[:.]/g, '-')}_${uuidv4()}`;
    runLogger.createRun(runId);
    runLogger.append(runId, `extractFile start for table=${table} file=${destFilename}`);
    if (cleanedHeuristic) {
      // If already cleaned, run transform runner (or identity) then use transformed output
      const transformRunner = path.join(__dirname, '..', 'etl_scripts', 'run_transform.py');
      const python = process.env.PYTHON || 'python';
      const transformedDir = path.join(__dirname, '..', '..', 'datapipeline', 'uploads', 'transformed');
      await fs.promises.mkdir(transformedDir, { recursive: true });
      const transformedPath = path.join(transformedDir, destFilename);
      try {
        runLogger.append(runId, `running transform (clean file already): ${python} ${transformRunner} ${table} ${destPath} -> ${transformedPath}`);
        const code = await execCommandStream(runId, python, [transformRunner, table, destPath, transformedPath]);
        if (code !== 0) {
          return res.status(500).json({ error: 'Transform failed', code, runId });
        }
        runLogger.append(runId, 'transform completed');
      } catch (err) {
        runLogger.append(runId, `Error invoking transform: ${err.message}`);
        return res.status(500).json({ error: 'Error invoking transform', details: err.message, runId });
      }
      // Use transformed output as cleanedPath
      cleanedPath = transformedPath;
      transformedAlready = true;
    } else {
      // Call Python cleaner runner: `python run_cleaner.py <table> <input> <output>`
      try {
        // Run the staging script file directly to perform cleaning and write cleaned CSV
        const python = process.env.PYTHON || 'python';
        const cleanerScript = path.join(__dirname, '..', 'etl_scripts', 'staging_script', 'cleaners', '__init__.py');
        runLogger.append(runId, `running cleaner: ${python} ${cleanerScript} ${destPath} -> ${cleanedPath}`);
        // pass the requested table name as an extra arg so the generic cleaner can dispatch
        const code = await execCommandStream(runId, python, [cleanerScript, destPath, cleanedPath, table]);
        if (code !== 0) {
          return res.status(500).json({ error: 'Cleaner failed', code, runId });
        }
        runLogger.append(runId, 'cleaner completed');
      } catch (err) {
        runLogger.append(runId, `Error invoking cleaner: ${err.message}`);
        return res.status(500).json({ error: 'Error invoking cleaner', details: err.message, runId });
      }
    }

    // If the cleaner script produced a file in datapipeline/uploads/cleaned, but not at cleanedPath,
    // try to locate the most recent cleaned file and use that.
    async function findMostRecentCsv(dir) {
      try {
        const files = await fs.promises.readdir(dir);
        const csvs = files.filter(f => f.toLowerCase().endsWith('.csv'));
        if (csvs.length === 0) return null;
        let newest = null;
        let newestTime = 0;
        for (const f of csvs) {
          const st = await fs.promises.stat(path.join(dir, f));
          const mtime = st.mtimeMs || st.mtime.getTime();
          if (mtime > newestTime) {
            newestTime = mtime;
            newest = path.join(dir, f);
          }
        }
        return newest;
      } catch (e) {
        return null;
      }
    }

    // Parse cleaned CSV and load into Supabase staging then run transformations
    try {
      let cleanedText;
      try {
        cleanedText = await fs.promises.readFile(cleanedPath, 'utf8');
      } catch (e) {
        // if not found, locate most recent cleaned file
        const mostRecent = await findMostRecentCsv(cleanedDir);
        if (!mostRecent) {
          throw new Error('Cleaned file not found after cleaner run');
        }
        cleanedPath = mostRecent;
        cleanedText = await fs.promises.readFile(cleanedPath, 'utf8');
      }

      // Always run transformation step on the cleaned CSV before ETL (skip if already transformed)
      if (!transformedAlready) {
        const transformRunner = path.join(__dirname, '..', 'etl_scripts', 'run_transform.py');
        const python = process.env.PYTHON || 'python';
        const transformedDir = path.join(__dirname, '..', '..', 'datapipeline', 'uploads', 'transformed');
        await fs.promises.mkdir(transformedDir, { recursive: true });
        const transformedPath = path.join(transformedDir, path.basename(cleanedPath));
        try {
          const args = [transformRunner, table, cleanedPath, transformedPath];
          const proc = spawnSync(python, args, { encoding: 'utf8' });
          if (proc.error) {
            console.error('Transform spawn error:', proc.error);
            return res.status(500).json({ error: 'Error running transform', details: proc.error.message });
          }
          if (proc.status !== 0) {
            console.error('Transform failed:', proc.stdout, proc.stderr);
            return res.status(500).json({ error: 'Transform failed', stdout: proc.stdout, stderr: proc.stderr });
          }
          // Use transformed file for ETL
          cleanedPath = transformedPath;
          cleanedText = await fs.promises.readFile(cleanedPath, 'utf8');
        } catch (err) {
          console.error('Error invoking transform:', err);
          return res.status(500).json({ error: 'Error invoking transform', details: err.message });
        }
      }

      const records = parse(cleanedText, { columns: true, skip_empty_lines: true });
      // Determine ETL config for table
      const config = tableConfigs[table];
      if (!config) {
        console.warn('No ETL config for table', table);
      }
      const etlLogs = await runFullETL(config || {
        staging_table: `staging_${table}`,
        pre_fact_table: `prefact_${table}`,
        dimension_table: `dim_${table}`,
        fact_table: `fact_${table}`,
        natural_key: 'id',
        scdType: 1
      }, records);

      const relativePath = path.relative(path.join(__dirname, '..', '..'), destPath);
      return res.json({ ok: true, path: relativePath, cleaned: path.relative(path.join(__dirname, '..', '..'), cleanedPath), etlLogs });
    } catch (err) {
      console.error('Error during ETL:', err);
      return res.status(500).json({ error: 'ETL failed', details: err.message });
    }
  } catch (err) {
    console.error('extractFile error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
};
