import { kafkaClient } from '../kafka-client.js';
import { databaseService } from '../services/database.service.js';
import { dbPool } from '../db/pool.js';

class DatabaseProcessor {
    constructor() {
        this.consumer = null;
        this.isRunning = false;
    }

    async initialize() {
        // Ensure database is ready
        await dbPool.healthCheck();
        
        // Initialize Kafka consumer
        this.consumer = kafkaClient.consumer({
            groupId: 'database-processor-group',
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
            maxBytesPerPartition: 1048576 // 1MB
        });

        await this.consumer.connect();
        await this.consumer.subscribe({
            topic: 'location-update',
            fromBeginning: false
        });

        console.log('Database processor initialized');
    }

    async start() {
        if (!this.consumer) {
            await this.initialize();
        }

        this.isRunning = true;

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat }) => {
                try {
                    const data = JSON.parse(message.value.toString());
                    
                    // Validate data
                    if (!data.userId || !data.latitude || !data.longitude) {
                        console.error('Invalid location data received:', data);
                        return;
                    }

                    // Store in database
                    await databaseService.saveLocation(
                        data.userId,
                        data.email,
                        data.name,
                        {
                            latitude: data.latitude,
                            longitude: data.longitude,
                            accuracy: data.accuracy,
                            altitude: data.altitude,
                            speed: data.speed,
                            heading: data.heading,
                            timestamp: data.timestamp ? new Date(data.timestamp) : new Date()
                        }
                    );

                    console.log(`Location stored for user ${data.email} at [${data.latitude}, ${data.longitude}]`);
                    
                    await heartbeat();
                } catch (error) {
                    console.error('Error processing location message:', error);
                }
            }
        });
    }

    async stop() {
        this.isRunning = false;
        if (this.consumer) {
            await this.consumer.disconnect();
        }
        await dbPool.close();
        console.log('Database processor stopped');
    }

    async getHealth() {
        const dbHealthy = await dbPool.healthCheck();
        return {
            processor: this.isRunning,
            database: dbHealthy,
            kafka: this.consumer ? this.consumer.connected : false
        };
    }
}

// Singleton instance
export const databaseProcessor = new DatabaseProcessor();

// Start processor if running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    databaseProcessor.start().catch(console.error);
    
    // Graceful shutdown
    process.on('SIGTERM', async () => {
        console.log('SIGTERM received, shutting down gracefully');
        await databaseProcessor.stop();
        process.exit(0);
    });
    
    process.on('SIGINT', async () => {
        console.log('SIGINT received, shutting down gracefully');
        await databaseProcessor.stop();
        process.exit(0);
    });
}