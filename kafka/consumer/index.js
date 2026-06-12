const { Kafka } = require('kafkajs');

const TOPIC_NAME = 'my-topic';
const GROUP_ID = 'my-consumer-group';
const BROKERS = [process.env.KAFKA_BROKERS || 'localhost:9092'];

async function consumeMessages() {
  const kafka = new Kafka({
    clientId: 'my-consumer-app',
    brokers: BROKERS,
  });

  const consumer = kafka.consumer({ groupId: GROUP_ID });

  try {
    // Connect consumer
    await consumer.connect();
    console.log('✓ Consumer connected to Kafka');

    // Subscribe to topic
    await consumer.subscribe({
      topic: TOPIC_NAME,
      fromBeginning: true, // Read from the beginning of the topic
    });
    console.log(`✓ Subscribed to topic: ${TOPIC_NAME}`);
    console.log(`✓ Consumer group: ${GROUP_ID}`);

    console.log('\n⏳ Waiting for messages...\n');

    // Run consumer
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = JSON.parse(message.value.toString());
        console.log('📨 Message received:');
        console.log(`   Topic: ${topic}`);
        console.log(`   Partition: ${partition}`);
        console.log(`   Offset: ${message.offset}`);
        console.log(`   Key: ${message.key ? message.key.toString() : 'null'}`);
        console.log(`   Value: ${JSON.stringify(value, null, 2)}`);
        console.log('');
      },
    });

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      console.log('\n✓ Shutting down...');
      await consumer.disconnect();
      console.log('✓ Consumer disconnected');
      process.exit(0);
    });
  } catch (error) {
    console.error('✗ Error:', error.message);
    await consumer.disconnect();
    process.exit(1);
  }
}

consumeMessages();