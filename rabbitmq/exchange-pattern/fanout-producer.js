const amqp = require('amqplib');

const EXCHANGE_NAME = 'logs_fanout';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function publishMessage(message) {
  let connection;
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    console.log('✓ Connected to RabbitMQ');

    const channel = await connection.createChannel();
    console.log('✓ Channel created');

    // Declare fanout exchange
    await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
    console.log(`✓ Fanout exchange "${EXCHANGE_NAME}" declared`);

    // Publish message
    const msg = JSON.stringify(message);
    channel.publish(EXCHANGE_NAME, '', Buffer.from(msg), {
      persistent: true,
    });

    console.log(`✓ Message published to exchange: ${msg}`);

    await channel.close();
    await connection.close();
  } catch (error) {
    console.error('✗ Error:', error.message);
    process.exit(1);
  }
}

const testMessage = {
  level: 'INFO',
  message: 'System log message',
  timestamp: new Date().toISOString(),
};

publishMessage(testMessage);