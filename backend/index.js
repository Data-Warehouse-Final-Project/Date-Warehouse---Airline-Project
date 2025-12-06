import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.warn('Missing SUPABASE_URL or SUPABASE_KEY in environment. See .env.example');
} else {
  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log('Supabase client initialized (environment variables detected).');

  // Optional: sample call to verify connection (commented out because it requires a real table)
  // const { data, error } = await supabase.from('your_table').select('*').limit(1);
  // console.log({ data, error });
}

export {};
