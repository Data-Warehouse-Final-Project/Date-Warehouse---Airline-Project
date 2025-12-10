import dotenv from 'dotenv';
dotenv.config();

import { Kafka } from 'kafkajs';
import { createClient } from '@supabase/supabase-js';

async function startWorker() {
  console.log("🚀 Starting Kafka Worker...");

  // 1. Setup Supabase
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY
  );

  // 2. Setup Kafka
  const kafka = new Kafka({
    clientId: 'airline-worker',
    brokers: [process.env.KAFKA_BROKER],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
    },
  });

  const consumer = kafka.consumer({ groupId: 'airline-group' });

  console.log("🔄 Connecting to Confluent...");
  await consumer.connect();
  console.log("✅ Connected to Confluent Cloud!");

  // 3. Subscribe to Topic
  await consumer.subscribe({ 
    topic: process.env.KAFKA_TOPIC, 
    fromBeginning: false 
  });

  // 4. Listen for Messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payloadString = message.value.toString();
      console.log(`📥 Received: ${payloadString}`);

      try {
        let content;
        try {
            content = JSON.parse(payloadString);
        } catch (e) {
            content = { raw: payloadString };
        }

        // Save raw message for audit
        const { error: saveErr } = await supabase
          .from('kafka_messages') 
          .insert({ 
            topic: topic,
            message_content: content
          });

        if (saveErr) console.error('❌ Database Error saving kafka_messages:', saveErr.message || saveErr);
        else console.log('✅ Saved raw message to Supabase (kafka_messages)');

        // If this is an eligibility check request, compute eligibility and write result
        // Expecting message shape: { type: 'eligibility_check', requested_at, payload: { firstName, lastName, flightNumber, passengerId } }
        if (content && content.type === 'eligibility_check' && content.payload) {
          try {
            const { firstName, lastName, flightNumber, passengerId } = content.payload;

            // Query flights table for the most recent matching flight
            const { data: flights, error: flightErr } = await supabase
              .from('flights')
              .select('scheduled_departure, actual_departure')
              .eq('flight_number', flightNumber)
              .order('scheduled_departure', { ascending: false })
              .limit(1);

            if (flightErr) {
              console.error('❌ Supabase flight query error:', flightErr.message || flightErr);
              await supabase.from('eligibility_results').insert({
                passenger_id: passengerId,
                flight_number: flightNumber,
                eligible: false,
                delay_minutes: null,
                reason: 'db_query_error',
                requested_at: content.requested_at || null,
                processed_at: new Date().toISOString(),
                details: { error: flightErr }
              });
            } else if (!flights || flights.length === 0) {
              await supabase.from('eligibility_results').insert({
                passenger_id: passengerId,
                flight_number: flightNumber,
                eligible: false,
                delay_minutes: null,
                reason: 'flight_not_found',
                requested_at: content.requested_at || null,
                processed_at: new Date().toISOString(),
                details: null
              });
            } else {
              const rec = flights[0];
              const scheduled = rec.scheduled_departure;
              const actual = rec.actual_departure;
              let reason = 'delay_below_threshold';
              let eligible = false;
              let delayMinutes = null;

              if (scheduled && actual) {
                const scheduledDate = new Date(scheduled);
                const actualDate = new Date(actual);
                if (!Number.isNaN(scheduledDate.getTime()) && !Number.isNaN(actualDate.getTime())) {
                  const diffMs = actualDate - scheduledDate;
                  delayMinutes = Math.round(diffMs / 60000);
                  if (delayMinutes >= 120) {
                    eligible = true;
                    reason = 'delay_threshold_met';
                  }
                } else {
                  reason = 'invalid_time_format';
                }
              } else {
                reason = 'missing_time_data';
              }

              await supabase.from('eligibility_results').insert({
                passenger_id: passengerId,
                first_name: firstName || null,
                last_name: lastName || null,
                flight_number: flightNumber,
                eligible,
                delay_minutes: delayMinutes,
                reason,
                requested_at: content.requested_at || null,
                processed_at: new Date().toISOString(),
                details: null
              });
              console.log('✅ Eligibility result saved for passenger', passengerId);
            }
          } catch (procErr) {
            console.error('❌ Error processing eligibility request:', procErr.message || procErr);
          }
        }

      } catch (err) {
        console.error('❌ Processing Error:', err.message);
      }
    },
  });
}

startWorker().catch(console.error);