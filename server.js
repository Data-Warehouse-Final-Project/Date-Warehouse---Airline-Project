const express = require('express');
const fileUpload = require('express-fileupload'); // Middleware for file uploads (for CSV if needed)
const app = express();

// Middleware
app.use(express.json());  // To parse incoming JSON request bodies
app.use(fileUpload());    // To handle file uploads

// Import and use the eligibility router
const eligibilityRoutes = require('./routes/eligibilityRoutes');
app.use('/eligibility', eligibilityRoutes);

// Import and use process (CSV upload) routes
const processRoutes = require('./backend/routes/processRoutes');
app.use('/process', processRoutes);

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});