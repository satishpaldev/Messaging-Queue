const amqp = require('amqplib');

const QUEUE_NAME = 'hello_queue';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function sendMessage(message) {
  let connection;
  try {
    // Connect to RabbitMQ
    connection = await amqp.connect(RABBITMQ_URL);
    console.log('✓ Connected to RabbitMQ');

    // Create a channel
    const channel = await connection.createChannel();
    console.log('✓ Channel created');

    // Declare the queue
    await channel.assertQueue(QUEUE_NAME, {
      durable: true, // Queue persists even after RabbitMQ restart
    });
    console.log(`✓ Queue "${QUEUE_NAME}" declared`);

    // Send message
    const msg = JSON.stringify(message);
    const sent = channel.sendToQueue(QUEUE_NAME, Buffer.from(msg), {
      persistent: true, // Message persists on disk
    });

    if (sent) {
      console.log(`✓ Message sent to queue: ${msg}`);
    } else {
      console.log('✗ Failed to send message');
    }

    // Close channel
    await channel.close();
    await connection.close();
    console.log('✓ Connection closed');
  } catch (error) {
    console.error('✗ Error:', error.message);
    process.exit(1);
  }
}

// Send a test message
const testMessage = {
  id: Date.now(),
  content: 'Hello from RabbitMQ Producer!',
  timestamp: new Date().toISOString(),
};

sendMessage(testMessage);