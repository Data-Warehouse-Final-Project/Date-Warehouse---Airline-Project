// backend/supabaseWatcher.js
import dotenv from 'dotenv';
dotenv.config();

import { Kafka } from 'kafkajs';
import { createClient } from '@supabase/supabase-js';

// 1. Setup Clients
const supabase = createClient(
  process.env.SUPABASE_URL, 
  process.env.SUPABASE_KEY
);

const kafka = new Kafka({
  clientId: 'supabase-watcher',
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const producer = kafka.producer();
const KAFKA_TOPIC = process.env.KAFKA_TOPIC; 

async function startWatcher() {
  console.log("🚀 Starting Supabase Watcher...");

  // 2. Connect to Kafka as a PRODUCER
  await producer.connect();
  console.log("✅ Kafka Producer Connected");

  // 3. Listen to Supabase Realtime
  const channel = supabase
    .channel('schema-db-changes')
    .on(
      'postgres_changes',
      {
        event: 'INSERT', 
        schema: 'public',
        table: 'outbox', // <--- MAKE SURE THIS MATCHES YOUR TABLE NAME
      },
      async (payload) => {
        console.log('⚡ New Event detected in Supabase:', payload.new);

        try {
          await producer.send({
            topic: KAFKA_TOPIC,
            messages: [
              {
                value: JSON.stringify(payload.new),
              },
            ],
          });
          console.log(`📤 Sent to Kafka Topic: ${KAFKA_TOPIC}`);
        } catch (err) {
          console.error("❌ Failed to send to Kafka:", err);
        }
      }
    )
    .subscribe();

  console.log("👀 Watching for new rows in 'outbox' table...");
}

startWatcher().catch(console.error);