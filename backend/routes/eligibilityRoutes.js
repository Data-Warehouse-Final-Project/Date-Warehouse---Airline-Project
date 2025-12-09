

const express = require('express');
const router = express.Router();
const { checkEligibility } = require('../controllers/eligibilityControllers');

// POST /eligibility/check-eligibility
router.post('/check-eligibility', checkEligibility);

module.exports = router;