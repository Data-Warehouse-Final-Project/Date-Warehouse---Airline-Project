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

        const { error } = await supabase
          .from('kafka_messages') 
          .insert({ 
            topic: topic,
            message_content: content
          });

        if (error) console.error('❌ Database Error:', error.message);
        else console.log('✅ Saved to Supabase');

      } catch (err) {
        console.error('❌ Processing Error:', err.message);
      }
    },
  });
}

startWorker().catch(console.error);