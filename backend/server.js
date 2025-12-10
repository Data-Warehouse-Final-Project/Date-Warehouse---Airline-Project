import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import cors from 'cors';
import { createClient } from '@supabase/supabase-js';
import { initProducer, produce } from './kafkaProducer.js';

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;

const supabase = createClient(process.env.SUPABASE_URL || '', process.env.SUPABASE_KEY || '');

// POST /api/check-eligibility
// Body: { firstName, lastName, flightNumber, passengerId }
app.post('/api/check-eligibility', async (req, res) => {
  const { firstName, lastName, flightNumber, passengerId } = req.body || {};
  if (!firstName || !lastName || !flightNumber || !passengerId) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const topic = process.env.KAFKA_TOPIC || 'eligibility-requests';

  // Produce a message to Kafka for audit / downstream processing
  try {
    await initProducer();
    await produce(topic, {
      type: 'eligibility_check',
      requested_at: new Date().toISOString(),
      payload: { firstName, lastName, flightNumber, passengerId }
    });
  } catch (err) {
    console.warn('Warning: failed to produce Kafka message', err.message || err);
  }

  // Query Supabase for flight times. This assumes a `flights` table with
  // `flight_number`, `scheduled_departure`, and `actual_departure` columns.
  // Adapt table/column names if your schema differs.
  try {
    const { data, error } = await supabase
      .from('flights')
      .select('scheduled_departure, actual_departure')
      .eq('flight_number', flightNumber)
      .order('scheduled_departure', { ascending: false })
      .limit(1);

    if (error) {
      console.error('Supabase query error:', error.message || error);
      return res.status(500).json({ error: 'Database query failed' });
    }

    if (!data || data.length === 0) {
      return res.json({ eligible: false, reason: 'flight_not_found' });
    }

    const record = data[0];
    const scheduled = record.scheduled_departure;
    const actual = record.actual_departure;

    if (!scheduled || !actual) {
      return res.json({ eligible: false, reason: 'missing_time_data' });
    }

    const scheduledDate = new Date(scheduled);
    const actualDate = new Date(actual);
    if (Number.isNaN(scheduledDate.getTime()) || Number.isNaN(actualDate.getTime())) {
      return res.json({ eligible: false, reason: 'invalid_time_format' });
    }

    const diffMs = actualDate - scheduledDate;
    const diffMinutes = Math.round(diffMs / 60000);
    const eligible = diffMinutes >= 120; // 2 hours

    return res.json({ eligible, delayMinutes: diffMinutes, reason: eligible ? 'delay_threshold_met' : 'delay_below_threshold' });
  } catch (err) {
    console.error('Eligibility check error:', err.message || err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Backend server listening on port ${PORT}`);
});

export {};
