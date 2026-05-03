// import { Kafka, Partitioners } from "kafkajs";
// import dotenv from 'dotenv';

// dotenv.config();

// const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9093';
// const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'location-tracker';

// console.log('=== Kafka Configuration ===');
// console.log('KAFKA_BROKERS:', KAFKA_BROKERS);
// console.log('KAFKA_CLIENT_ID:', KAFKA_CLIENT_ID);
// console.log('==========================');

// export const kafkaClient = new Kafka({
//     clientId: KAFKA_CLIENT_ID,
//     brokers: [KAFKA_BROKERS],
//     retry: {
//         initialRetryTime: 300,
//         retries: 10,
//         maxRetryTime: 30000
//     },
//     connectionTimeout: 10000,
//     requestTimeout: 25000,
//     createPartitioner: Partitioners.LegacyPartitioner,
//     // Add this to handle version compatibility
//     ssl: false,
//     // Specify the Kafka version explicitly
//     brokerVersion: '3.0.0'
// });

// export async function checkKafkaConnection() {
//     try {
//         console.log(`Attempting to connect to Kafka at ${KAFKA_BROKERS}...`);
//         const admin = kafkaClient.admin();
//         await admin.connect();
//         const topics = await admin.listTopics();
//         await admin.disconnect();
//         console.log('✅ Kafka connection successful');
//         console.log('Available topics:', topics);
//         return true;
//     } catch (error) {
//         console.error('❌ Kafka connection failed:', error.message);
//         return false;
//     }
// }

import { Kafka } from "kafkajs";
import dotenv from 'dotenv';

dotenv.config();

const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'location-tracker';

console.log('Connecting to Kafka at:', KAFKA_BROKERS);

export const kafkaClient = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: [KAFKA_BROKERS],
    retry: {
        initialRetryTime: 300,
        retries: 10,
        maxRetryTime: 30000
    }
});

export async function checkKafkaConnection() {
    try {
        const admin = kafkaClient.admin();
        await admin.connect();
        await admin.disconnect();
        console.log('✅ Kafka connected successfully');
        return true;
    } catch (error) {
        console.error('❌ Kafka connection failed:', error.message);
        return false;
    }
}