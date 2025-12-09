// Import the necessary libraries
const { createClient } = require('@supabase/supabase-js');

// Setup the Supabase client with your project URL and API key
const supabaseUrl = process.env.SUPABASE_URL; // Your Supabase URL
const supabaseKey = process.env.SUPABASE_KEY; // Your Supabase service role key (or anon key)

const supabase = createClient(supabaseUrl, supabaseKey);

module.exports = { supabase }; // Export the client for use in other files