import express from 'express';
import multer from 'multer';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { Client } from 'pg';
import { fileURLToPath } from 'url';

dotenv.config();

// Get __dirname equivalent in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// Middleware
app.use(cors());
app.use(express.json());
const upload = multer({ dest: 'uploads/' });

// Supabase configuration
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const PORT = process.env.PORT || 3001;

// Helper: map file types to staging table names
const STAGING_TABLE_MAP = {
  transactions: 'staging_facttravelagencysales_source2_agency',
  passengers: 'staging_passengers',
  flights: 'staging_flights',
  airports: 'staging_airports',
  airlines: 'staging_airlines'
};

/**
 * Ensure a PostgreSQL table exists for the uploaded CSV.
 * This function reads the CSV header line and creates a table with TEXT columns
 * named after sanitized header names. It requires `DATABASE_URL` environment variable
 * to be set (use the Supabase Postgres connection string / service role DB URL).
 */
async function ensureTableForCsv(filePath, fileType) {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    throw new Error('DATABASE_URL not configured. Add your Supabase Postgres connection string as DATABASE_URL in the backend .env');
  }

  const tableName = STAGING_TABLE_MAP[fileType] || `staging_${fileType}`;

  try {
    // Read first line of CSV to get headers
    const content = fs.readFileSync(filePath, { encoding: 'utf8' });
    const firstLine = content.split(/\r?\n/)[0] || '';
    const headers = firstLine.split(',').map(h => h.trim()).filter(Boolean);

    console.log(`📄 CSV headers found: ${headers.join(', ')}`);

    // sanitize headers to valid SQL identifiers (simple approach)
    const sanitize = (h) => h.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/^_+|_+$/g, '') || 'col';
    const cols = headers.length ? headers.map(h => `${sanitize(h)} TEXT`) : ['raw_record JSONB'];

    const createSql = `CREATE TABLE IF NOT EXISTS ${tableName} (id SERIAL PRIMARY KEY, ${cols.join(', ')}, inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now())`;

    console.log(`🛠️  Creating table with SQL: ${createSql}`);

    const client = new Client({ connectionString: dbUrl });
    await client.connect();
    try {
      // Check if table exists and inspect column types
      const checkRes = await client.query(
        `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`,
        [tableName]
      );

      let useTable = tableName;
      if (checkRes.rows && checkRes.rows.length > 0) {
        const nonText = checkRes.rows.filter(r => r.data_type !== 'text' && r.data_type !== 'character varying');
        if (nonText.length > 0) {
          // Existing table has typed columns - create a safe raw table instead
          const altTable = `${tableName}_raw`;
          const altCreateSql = `CREATE TABLE IF NOT EXISTS ${altTable} (id SERIAL PRIMARY KEY, ${cols.join(', ')}, inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now())`;
          console.log(`⚠️  Existing table ${tableName} has non-text columns. Creating alternate table: ${altTable}`);
          await client.query(altCreateSql);
          useTable = altTable;
        } else {
          // Table exists and columns are text-like; nothing to do
          console.log(`✅ Table ${tableName} exists and is compatible`);
        }
      } else {
        // Table does not exist - create it
        console.log(`🛠️  Creating table with SQL: ${createSql}`);
        await client.query(createSql);
        console.log(`✅ Table ${tableName} created successfully`);
        useTable = tableName;
      }

      return useTable;
    } catch (queryErr) {
      console.error(`❌ Error executing SQL query: ${queryErr.message}`);
      throw new Error(`Failed to create/verify table: ${queryErr.message}`);
    } finally {
      await client.end();
    }
  } catch (err) {
    console.error(`❌ ensureTableForCsv error: ${err.message}`);
    throw err;
  }
}

// ============================================
// FILE CLEANING ENDPOINT
// ============================================
app.post('/api/clean-file', upload.single('file'), async (req, res) => {
  try {
    const file = req.file;

    if (!file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    console.log('📁 Uploaded file:', file);

    const inputFilePath = path.join(__dirname, file.path);
    const outputFilePath = path.join(__dirname, 'cleaned_' + file.filename + '.csv');
    const fileType = req.body.file_type || 'transactions';

    // Path to the cleaning Python script
    const pythonScriptPath = path.join(__dirname, 'etl_scripts', 'cleaning.py');
    
    // Try multiple possible Python paths
    const pythonPaths = [
      'C:\\Users\\Jacque\\anaconda3\\python.exe',
      'C:\\Users\\Jacque\\AppData\\Local\\Programs\\Python\\Python311\\python.exe',
      'C:\\Users\\Jacque\\AppData\\Local\\Programs\\Python\\Python310\\python.exe',
      process.env.PYTHON_EXE || 'python'
    ];

    let pythonExe = null;
    for (const p of pythonPaths) {
      if (fs.existsSync(p)) {
        pythonExe = p;
        break;
      }
    }
    
    if (!pythonExe) {
      pythonExe = pythonPaths[pythonPaths.length - 1]; // fallback to 'python'
    }

    console.log(`🔄 Running cleaning script with Python: ${pythonExe}`);
    console.log(`🔄 Script path: ${pythonScriptPath}`);

    // Ensure staging table exists for this CSV (reads header row and creates TEXT columns)
    let stagingTableName;
    try {
      stagingTableName = await ensureTableForCsv(inputFilePath, fileType);
      console.log(`🛠️  Ensured staging table exists: ${stagingTableName}`);
      // Give PostgREST / Supabase a short moment to pick up newly-created table in schema cache
      await new Promise((resolve) => setTimeout(resolve, 2000));
      console.log('⏳ Waited 2s for Supabase schema cache to refresh (if table was newly created)');
    } catch (tableErr) {
      console.error('❌ Failed to ensure staging table:', tableErr.message);
      console.error('Stack trace:', tableErr.stack);
      return res.status(500).json({ 
        error: `Failed to ensure staging table: ${tableErr.message}`,
        details: tableErr.stack
      });
    }

    // Call the Python script with full path to Python executable
    // Pass the resolved staging table name as an extra argument so the Python script writes to a compatible table
    const pythonArgs = `"${pythonScriptPath}" "${inputFilePath}" "${fileType}" "${SUPABASE_URL}" "${SUPABASE_KEY}" "${stagingTableName}"`;
    exec(`"${pythonExe}" ${pythonArgs}`, (error, stdout, stderr) => {
      if (error) {
        console.error(`❌ Error running Python script: ${error.message}`);
        return res.status(500).json({ error: `Error cleaning file: ${error.message}` });
      }
      if (stderr) {
        console.error(`❌ Python stderr: ${stderr}`);
        return res.status(500).json({ error: `Error cleaning file: ${stderr}` });
      }

      console.log(`✅ Python script output: ${stdout}`);

      // Return success response
      res.status(200).json({
        message: 'File cleaned successfully',
        cleanedFilePath: outputFilePath
      });

      // Clean up the uploaded file after processing
      try {
        fs.unlinkSync(inputFilePath);
      } catch (e) {
        console.warn('Warning: could not delete uploaded file');
      }
    });
  } catch (error) {
    console.error('❌ Error cleaning file:', error);
    res.status(500).json({ error: `Error cleaning file: ${error.message}` });
  }
});

// ============================================
// ELIGIBILITY CHECK ENDPOINT
// ============================================
app.post('/api/check-eligibility', async (req, res) => {
  const { firstName: reqFirstName, lastName: reqLastName, fullName, flightNumber, passengerId } = req.body || {};

  // Log the incoming body to help debugging when requests fail
  console.log('📥 /api/check-eligibility received body:', JSON.stringify(req.body));

  // Derive firstName/lastName from fullName when needed
  let firstName = reqFirstName || '';
  let lastName = reqLastName || '';
  if ((!firstName || !lastName) && fullName) {
    const parts = String(fullName).trim().split(/\s+/);
    if (!firstName) firstName = parts.length ? parts[0] : '';
    if (!lastName) lastName = parts.length > 1 ? parts.slice(1).join(' ') : '';
  }

  // Require at least firstName, flightNumber and passengerId
  if (!firstName || !flightNumber || !passengerId) {
    console.warn('⚠️ check-eligibility missing fields', { firstName, flightNumber, passengerId });
    return res.status(400).json({ error: 'Missing required fields: firstName (or fullName), flightNumber, passengerId' });
  }

  try {
    console.log(`📋 Checking eligibility for passenger ${passengerId} on flight ${flightNumber}`);

    // Query the flights table
    const { data: flights, error: flightError } = await supabase
      .from('flights')
      .select('flight_number, scheduled_departure, actual_departure')
      .eq('flight_number', flightNumber)
      .order('scheduled_departure', { ascending: false })
      .limit(1);

    if (flightError) {
      console.error('❌ Error querying flights table:', flightError);
      return res.status(500).json({
        error: 'Database error',
        details: flightError.message,
        eligible: false,
        reason: 'database_error'
      });
    }

    if (!flights || flights.length === 0) {
      console.log(`⚠️  Flight ${flightNumber} not found in database`);
      return res.json({
        eligible: false,
        reason: 'flight_not_found',
        message: `No flight record found for flight number ${flightNumber}`
      });
    }

    const flight = flights[0];
    const scheduledDeparture = new Date(flight.scheduled_departure);
    const actualDeparture = new Date(flight.actual_departure);

    console.log(`✈️  Flight ${flightNumber}:`);
    console.log(`   Scheduled: ${scheduledDeparture.toISOString()}`);
    console.log(`   Actual: ${actualDeparture.toISOString()}`);

    if (isNaN(scheduledDeparture.getTime()) || isNaN(actualDeparture.getTime())) {
      console.error('❌ Invalid date format in flight data');
      return res.json({
        eligible: false,
        reason: 'invalid_time_format',
        message: 'Flight time data has invalid format'
      });
    }

    // Calculate delay in minutes
    const delayMs = actualDeparture.getTime() - scheduledDeparture.getTime();
    const delayMinutes = Math.round(delayMs / (1000 * 60));

    console.log(`⏱️  Delay: ${delayMinutes} minutes`);

    // Determine eligibility (>= 120 minutes = 2 hours)
    const eligible = delayMinutes >= 120;

    const response = {
      eligible,
      delayMinutes,
      flightNumber,
      passengerId,
      firstName,
      lastName,
      reason: eligible ? 'eligible_for_insurance' : 'delay_below_threshold',
      message: eligible
        ? `✅ Passenger IS ELIGIBLE for insurance claim (${delayMinutes} min delay)`
        : `❌ Passenger is NOT eligible. Delay is only ${delayMinutes} minutes (minimum 120 required)`
    };

    console.log(`📊 Result:`, response.message);
    return res.json(response);

  } catch (err) {
    console.error('❌ Eligibility check error:', err);
    return res.status(500).json({
      error: 'Internal server error',
      details: err.message,
      eligible: false,
      reason: 'server_error'
    });
  }
});

// ============================================
// TRANSFORM TABLES ENDPOINT
// ============================================
app.post('/api/transform-tables', async (req, res) => {
  try {
    const { file_type } = req.body || {};

    if (!file_type) {
      return res.status(400).json({ error: 'Missing required field: file_type' });
    }

    console.log(`🔄 Transform request received for file_type: ${file_type}`);

    // Map file types to their staging table names
    const stagingTableMap = {
      transactions: 'staging_facttravelagencysales_source2_agency',
      passengers: 'staging_passengers',
      flights: 'staging_flights',
      airports: 'staging_airports',
      airlines: 'staging_airlines',
      airlinesales: 'staging_airlinesales'
    };

    const stagingTable = stagingTableMap[file_type];
    if (!stagingTable) {
      return res.status(400).json({ error: `Unknown file_type: ${file_type}. Valid types: ${Object.keys(stagingTableMap).join(', ')}` });
    }

    console.log(`📊 Attempting to transform and upsert from ${stagingTable}`);

    // Query the staging table to get cleaned data
    const { data: stagedData, error: queryError } = await supabase
      .from(stagingTable)
      .select('*')
      .limit(10000); // Limit to prevent memory issues

    if (queryError) {
      console.error(`❌ Error querying staging table ${stagingTable}:`, queryError);
      return res.status(500).json({ 
        error: `Failed to query staging table: ${queryError.message}`,
        details: queryError
      });
    }

    if (!stagedData || stagedData.length === 0) {
      console.log(`⚠️  No data found in staging table ${stagingTable}`);
      return res.json({ 
        message: `Transform completed. No rows found in ${stagingTable}`,
        rows_processed: 0
      });
    }

    console.log(`✅ Retrieved ${stagedData.length} rows from ${stagingTable}`);

    // For now, just confirm the data was retrieved
    // In a production system, you would:
    // 1. Apply transformations (cleansing, enrichment, dimensional lookups)
    // 2. Load into fact/dimension tables
    // 3. Update SCD Type 2 tables for slowly changing dimensions
    // 4. Archive processed records

    return res.json({
      message: `Transform completed successfully for ${file_type}`,
      file_type: file_type,
      staging_table: stagingTable,
      rows_processed: stagedData.length,
      sample_record: stagedData.length > 0 ? stagedData[0] : null
    });

  } catch (error) {
    console.error('❌ Transform error:', error);
    res.status(500).json({ 
      error: `Error during transform: ${error.message}`,
      details: error
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', port: PORT });
});

app.listen(PORT, () => {
  console.log(`🚀 Backend server listening on port ${PORT}`);
  console.log(`📊 Supabase URL: ${SUPABASE_URL ? '✓ configured' : '✗ not configured'}`);
});

export {};