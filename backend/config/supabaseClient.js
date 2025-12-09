// Load environment variables (if a .env file exists)
try {
	require('dotenv').config();
} catch (e) {
	// ignore if dotenv is not installed/available in this environment
}

// Import the necessary libraries
const { createClient } = require('@supabase/supabase-js');

// Setup the Supabase client with your project URL and API key
const supabaseUrl = process.env.SUPABASE_URL; // Your Supabase URL
const supabaseKey = process.env.SUPABASE_KEY; // Your Supabase service role key (or anon key)

let supabase;
if (!supabaseUrl || !supabaseKey) {
	console.warn('SUPABASE_URL or SUPABASE_KEY not set. Supabase client not configured.');
	// Provide a minimal stub that will throw helpful errors if used before configuration
	supabase = new Proxy({}, {
		get() {
			return () => {
				throw new Error('Supabase client not configured. Set SUPABASE_URL and SUPABASE_KEY environment variables.');
			};
		}
	});
} else {
	supabase = createClient(supabaseUrl, supabaseKey);
}

module.exports = { supabase }; // Export the client for use in other files