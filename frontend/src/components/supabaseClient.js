import { createClient } from '@supabase/supabase-js';

// Get the environment variables (configured in frontend/.env)
const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL;
const SUPABASE_KEY = import.meta.env.VITE_SUPABASE_KEY;

// Default supabase client (set to null initially)
let supabase = null;

// Function to create an error proxy when Supabase is not configured
const createErrorProxy = () => {
  return new Proxy({}, {
    get(target, prop) {
      return () => {
        throw new Error(`Supabase is not configured. Please set the required environment variables.`);
      };
    },
  });
};

// Check if the necessary Supabase environment variables are set
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.warn('⚠️ Supabase client: Missing VITE_SUPABASE_URL or VITE_SUPABASE_KEY in frontend/.env. Eligibility checks will not work.');
  
  // Show an alert or log in the UI
  alert('⚠️ Supabase client is missing environment variables. Check your .env file.');

  // Create a proxy for error handling
  supabase = createErrorProxy();
} else {
  // Check if Supabase URL seems valid (basic validation for HTTPS)
  if (!SUPABASE_URL.startsWith('https://')) {
    console.error('⚠️ Invalid Supabase URL. Ensure the URL starts with "https://".');
  }

  // Initialize the Supabase client with the URL and Key
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
}

// Export the supabase client instance
export { supabase };
export default supabase;