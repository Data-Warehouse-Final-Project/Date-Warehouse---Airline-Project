const express = require('express');
const router = express.Router();
const { uploadCSV, extractFile, listCleaners, uploadCleaned } = require('../controllers/processControllers');
const { streamLogs } = require('../controllers/processControllers');

// POST /process/upload
router.post('/upload', uploadCSV);
// POST /process/extract - save uploaded file into datapipeline/uploads/incoming
router.post('/extract', extractFile);
// POST /process/upload_cleaned - upload a cleaned CSV and run transformations
router.post('/upload_cleaned', uploadCleaned);
// SSE stream for run logs: client connects to receive live logs for a run
router.get('/stream/:runId', streamLogs);
// GET /process/tables - list available cleaner tables
router.get('/tables', listCleaners);

module.exports = router;
