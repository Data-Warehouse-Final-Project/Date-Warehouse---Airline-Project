const express = require('express');
const fileUpload = require('express-fileupload'); // Middleware for file uploads (for CSV if needed)
const cors = require('cors');
const path = require('path');
const app = express();

// Middleware
app.use(express.json());  // To parse incoming JSON request bodies
// Enable CORS so frontend (Vite) can call the backend during development
app.use(cors());
app.use(fileUpload());    // To handle file uploads

// Simple request logger to help debug frontend uploads
app.use((req, res, next) => {
  try {
    const time = new Date().toISOString();
    const shortHeaders = Object.assign({}, req.headers);
    // avoid printing cookies or large auth tokens
    if (shortHeaders.cookie) shortHeaders.cookie = '[HIDDEN]';
    if (shortHeaders.authorization) shortHeaders.authorization = '[HIDDEN]';
    console.log(`[${time}] ${req.method} ${req.originalUrl} headers=${JSON.stringify(shortHeaders)}`);
  } catch (e) {
    // ignore logging errors
  }
  next();
});

// Import and use the eligibility router (use __dirname to build absolute paths)
const eligibilityRoutes = require(path.join(__dirname, 'backend', 'routes', 'eligibilityRoutes'));
app.use('/eligibility', eligibilityRoutes);

// Import and use process (CSV upload) routes
const processRoutes = require(path.join(__dirname, 'backend', 'routes', 'processRoutes'));
app.use('/process', processRoutes);

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});