import { Kafka } from 'kafkajs';

const brokers = (process.env.KAFKA_BROKERS || '').split(',').map(s => s.trim()).filter(Boolean);
const clientId = process.env.KAFKA_CLIENT_ID || 'date-warehouse-backend';
const username = process.env.KAFKA_API_KEY || process.env.KAFKA_USERNAME;
const password = process.env.KAFKA_API_SECRET || process.env.KAFKA_PASSWORD;

let kafka;
let producer;

if (brokers.length) {
  const conf = {
    clientId,
    brokers,
    ssl: true
  };
  if (username && password) {
    conf.sasl = { mechanism: 'plain', username, password };
  }
  kafka = new Kafka(conf);
} else {
  console.warn('kafkaProducer: no brokers configured (KAFKA_BROKERS)');
}

export async function initProducer() {
  if (!kafka) {
    throw new Error('Kafka not configured (KAFKA_BROKERS missing)');
  }
  if (producer) return producer;
  producer = kafka.producer();
  await producer.connect();
  console.log('kafkaProducer: connected');
  return producer;
}

export async function produce(topic, value) {
  if (!kafka) {
    throw new Error('Kafka not configured (KAFKA_BROKERS missing)');
  }
  if (!producer) await initProducer();
  const message = { value: typeof value === 'string' ? value : JSON.stringify(value) };
  const result = await producer.send({ topic, messages: [message] });
  return result;
}

export async function disconnectProducer() {
  if (producer) {
    await producer.disconnect();
    producer = null;
    console.log('kafkaProducer: disconnected');
  }
}
