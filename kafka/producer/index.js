const { Kafka } = require('kafkajs');

const TOPIC_NAME = 'my-topic';
const BROKERS = [process.env.KAFKA_BROKERS || 'localhost:9092'];

async function produceMessage() {
  const kafka = new Kafka({
    clientId: 'my-producer-app',
    brokers: BROKERS,
  });

  const producer = kafka.producer();

  try {
    // Connect producer
    await producer.connect();
    console.log('✓ Producer connected to Kafka');

    // Create topic (if it doesn't exist)
    const admin = kafka.admin();
    await admin.connect();
    console.log('✓ Admin connected');

    await admin.createTopics({
      topics: [
        {
          topic: TOPIC_NAME,
          numPartitions: 3,
          replicationFactor: 1,
        },
      ],
      validateOnly: false,
      waitForLeaders: true,
    }).catch((error) => {
      if (error.message.includes('already exists')) {
        console.log(`✓ Topic "${TOPIC_NAME}" already exists`);
      } else {
        throw error;
      }
    });

    await admin.disconnect();
    console.log('✓ Admin disconnected');

    // Produce messages
    const messages = [];
    for (let i = 1; i <= 5; i++) {
      messages.push({
        key: `key-${i}`,
        value: JSON.stringify({
          id: i,
          message: `Message ${i} from Kafka Producer`,
          timestamp: new Date().toISOString(),
        }),
      });
    }

    await producer.send({
      topic: TOPIC_NAME,
      messages,
    });

    console.log('✓ Messages sent to Kafka:');
    messages.forEach(msg => {
      console.log(`  - Key: ${msg.key}, Value: ${msg.value}`);
    });

    // Disconnect producer
    await producer.disconnect();
    console.log('✓ Producer disconnected');
  } catch (error) {
    console.error('✗ Error:', error.message);
    process.exit(1);
  }
}

produceMessage();