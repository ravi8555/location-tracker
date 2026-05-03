import dotenv from 'dotenv';
dotenv.config();

console.log('Environment variables loaded:');
console.log('KAFKA_BROKERS:', process.env.KAFKA_BROKERS);
console.log('KAFKA_CLIENT_ID:', process.env.KAFKA_CLIENT_ID);

import { Kafka, Partitioners } from "kafkajs";

const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9093';
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'location-tracker';

console.log(`\nUsing brokers: ${KAFKA_BROKERS}`);

const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: [KAFKA_BROKERS],
    createPartitioner: Partitioners.LegacyPartitioner
});

async function test() {
    try {
        const admin = kafka.admin();
        await admin.connect();
        console.log('✅ Connected to Kafka!');
        
        const topics = await admin.listTopics();
        console.log('Topics:', topics);
        
        await admin.disconnect();
    } catch (error) {
        console.error('❌ Failed:', error.message);
        console.log('\nTroubleshooting tips:');
        console.log('1. Check if Kafka container is running: docker ps | grep kafka');
        console.log('2. Check port: netstat -an | findstr 9093');
        console.log('3. Restart Kafka: docker restart kafka_tracker');
    }
}

test();