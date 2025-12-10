import { createClient } from '@supabase/supabase-js';

// Configure these in your frontend environment (.env)
// Vite exposes client env vars via import.meta.env and they must be prefixed with VITE_
// Create a file `frontend/.env` with `VITE_SUPABASE_URL` and `VITE_SUPABASE_KEY`.
const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL || '';
const SUPABASE_KEY = import.meta.env.VITE_SUPABASE_KEY || '';

if (!SUPABASE_URL || !SUPABASE_KEY) {
	console.warn('Supabase client: missing REACT_APP_SUPABASE_URL or REACT_APP_SUPABASE_KEY');
}

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Helper: subscribe to eligibility_results inserts for a passenger
// Returns an object with `unsubscribe()` to stop listening.
export function subscribeToEligibility(passengerId, onInsert) {
	if (!passengerId) throw new Error('passengerId is required for subscribeToEligibility');

	// Use a unique channel name so multiple subscriptions are possible
	const chanName = `eligibility_sub_${passengerId}_${Date.now()}`;

	const channel = supabase
		.channel(chanName)
		.on(
			'postgres_changes',
			{
				event: 'INSERT',
				schema: 'public',
				table: 'eligibility_results',
				filter: `passenger_id=eq.${passengerId}`,
			},
			(payload) => {
				if (onInsert) onInsert(payload.new);
			}
		)
		.subscribe();

	return {
		unsubscribe: () => {
			try {
				channel.unsubscribe();
			} catch (e) {
				// ignore
			}
		},
		channel,
	};
}

export default supabase;

