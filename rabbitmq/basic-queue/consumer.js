const amqp = require('amqplib');

const QUEUE_NAME = 'hello_queue';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function consumeMessages() {
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
      durable: true,
    });
    console.log(`✓ Queue "${QUEUE_NAME}" declared`);

    // Set prefetch to 1 - process one message at a time
    await channel.prefetch(1);
    console.log('✓ Prefetch set to 1');

    // Consume messages
    console.log('⏳ Waiting for messages...');
    await channel.consume(QUEUE_NAME, (msg) => {
      if (msg) {
        try {
          const content = JSON.parse(msg.content.toString());
          console.log('📨 Received message:', content);

          // Acknowledge the message (remove from queue)
          channel.ack(msg);
          console.log('✓ Message acknowledged');
        } catch (error) {
          console.error('✗ Error parsing message:', error.message);
          // Negative acknowledge and requeue
          channel.nack(msg, false, true);
        }
      }
    });

    // Keep the connection alive
    process.on('SIGINT', async () => {
      console.log('\n✓ Shutting down...');
      await channel.close();
      await connection.close();
      console.log('✓ Connection closed');
      process.exit(0);
    });
  } catch (error) {
    console.error('✗ Error:', error.message);
    if (connection) await connection.close();
    process.exit(1);
  }
}

consumeMessages();