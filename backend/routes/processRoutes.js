const express = require('express');
const router = express.Router();
const { uploadCSV } = require('../controllers/processControllers');

// POST /process/upload
router.post('/upload', uploadCSV);

module.exports = router;
