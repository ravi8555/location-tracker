import { kafkaClient } from "./kafka-client.js";

async function setup() {
    const admin = kafkaClient.admin()

    console.log('Kafka Admin connecting...');
    await admin.connect()
    console.log('Kafka Admin connecting Sucess...');

    await admin.createTopics({
        topics : [
            {topic : 'location-update', numPartitions : 2}
        ]
        
    })
    await admin.disconnect()
    
}
setup()