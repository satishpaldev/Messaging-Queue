const amqp = require('amqplib');

const EXCHANGE_NAME = 'logs_fanout';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';

async function consumeMessages(consumerName) {
  let connection;
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    console.log(`✓ [${consumerName}] Connected to RabbitMQ`);

    const channel = await connection.createChannel();
    console.log(`✓ [${consumerName}] Channel created`);

    // Declare fanout exchange
    await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
    console.log(`✓ [${consumerName}] Exchange declared`);

    // Create exclusive queue for this consumer
    const queue = await channel.assertQueue('', { exclusive: true });
    console.log(`✓ [${consumerName}] Queue created: ${queue.queue}`);

    // Bind queue to exchange
    await channel.bindQueue(queue.queue, EXCHANGE_NAME, '');
    console.log(`✓ [${consumerName}] Queue bound to exchange`);

    console.log(`⏳ [${consumerName}] Waiting for messages...\n`);

    await channel.consume(queue.queue, (msg) => {
      if (msg) {
        const content = JSON.parse(msg.content.toString());
        console.log(`📨 [${consumerName}] Received:`, content);
        channel.ack(msg);
      }
    });

    process.on('SIGINT', async () => {
      console.log(`\n✓ [${consumerName}] Shutting down...`);
      await channel.close();
      await connection.close();
      process.exit(0);
    });
  } catch (error) {
    console.error(`✗ [${consumerName}] Error:`, error.message);
    if (connection) await connection.close();
    process.exit(1);
  }
}

const consumerName = process.argv[2] || 'Consumer1';
consumeMessages(consumerName);