import express from 'express';
import multer from 'multer';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
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

    // Call the Python script with full path to Python executable
    exec(`"${pythonExe}" "${pythonScriptPath}" "${inputFilePath}" "${fileType}" "${SUPABASE_URL}" "${SUPABASE_KEY}"`, (error, stdout, stderr) => {
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
  const { firstName, lastName, flightNumber, passengerId } = req.body || {};

  if (!firstName || !lastName || !flightNumber || !passengerId) {
    return res.status(400).json({ error: 'Missing required fields: firstName, lastName, flightNumber, passengerId' });
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

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', port: PORT });
});

app.listen(PORT, () => {
  console.log(`🚀 Backend server listening on port ${PORT}`);
  console.log(`📊 Supabase URL: ${SUPABASE_URL ? '✓ configured' : '✗ not configured'}`);
});

export {};