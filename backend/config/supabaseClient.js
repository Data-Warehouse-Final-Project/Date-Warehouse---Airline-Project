import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

// Load environment variables from .env file
dotenv.config();

// Setup the Supabase client with your project URL and API key
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

let supabase;

if (!supabaseUrl || !supabaseKey) {
	console.warn('⚠️  SUPABASE_URL or SUPABASE_KEY not set. Supabase client not configured.');
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

export { supabase };
export default supabase;