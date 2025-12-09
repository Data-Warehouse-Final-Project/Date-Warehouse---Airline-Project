import { useEffect, useState } from 'react';
import { createClient } from '@supabase/supabase-js';

// Initialize Supabase
const supabase = createClient(
  import.meta.env.VITE_SUPABASE_URL, 
  import.meta.env.VITE_SUPABASE_KEY
);

export default function LiveStream() {
  const [streamData, setStreamData] = useState([]);

  useEffect(() => {
    // 1. Load the last 5 events so the list isn't empty
    fetchHistory();

    // 2. Listen for NEW events from Kafka (via Supabase)
    const channel = supabase
      .channel('ui-consumption')
      .on('postgres_changes', 
        { event: 'INSERT', schema: 'public', table: 'live_feed' }, 
        (payload) => {
          console.log('🔴 Live Stream Update:', payload.new);
          // Add new item to the TOP of the list
          setStreamData((prev) => [payload.new, ...prev]);
        }
      )
      .subscribe();

    return () => supabase.removeChannel(channel);
  }, []);

  async function fetchHistory() {
    const { data } = await supabase
      .from('live_feed')
      .select('*')
      .order('processed_at', { ascending: false })
      .limit(5);
      
    if (data) setStreamData(data);
  }

  return (
    <div style={{ marginTop: '20px', padding: '15px', backgroundColor: '#1e1e1e', color: '#00ff41', borderRadius: '8px', fontFamily: 'monospace' }}>
      <h3>📡 Kafka Consumer Stream [Topic: check-ins]</h3>
      <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
        {streamData.length === 0 ? <p style={{color: '#555'}}>Waiting for stream...</p> : null}
        
        {streamData.map((item) => (
          <div key={item.id} style={{ padding: '8px', borderBottom: '1px solid #333' }}>
            <span style={{ color: '#888', fontSize: '0.8em', marginRight: '10px' }}>
              {new Date(item.processed_at).toLocaleTimeString()}
            </span>
            <strong>{item.event}</strong>: 
            Passenger {item.details?.name || 'Unknown'} 
            (Flight {item.details?.flight || 'N/A'})
          </div>
        ))}
      </div>
    </div>
  );
}