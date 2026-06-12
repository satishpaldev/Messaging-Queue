# RabbitMQ Guide

## What is RabbitMQ?

RabbitMQ is a robust, open-source message broker that implements the Advanced Message Queuing Protocol (AMQP). It enables reliable, asynchronous communication between distributed systems.

## Architecture

```
Producer → Exchange → Queue → Consumer
```

### Components

1. **Producer**: Sends messages to an exchange
2. **Exchange**: Routes messages to queues based on binding rules
3. **Queue**: Stores messages until consumers process them
4. **Consumer**: Receives and processes messages
5. **Binding**: Rules that connect exchanges to queues

## Exchange Types

### 1. Direct Exchange
Routes messages based on exact routing key matching.

```
Producer → Exchange → Queue (routing key matches)
                   ↓
                  Queue (routing key doesn't match)
```

**Use Case**: Task queues, logging with severity levels

### 2. Fanout Exchange
Broadcasts messages to all bound queues, ignoring routing keys.

```
Producer → Exchange → Queue 1
                    → Queue 2
                    → Queue 3
```

**Use Case**: Event notifications, real-time updates

### 3. Topic Exchange
Routes messages based on pattern matching.

**Routing Key Format**: `<word>.<word>.<word>`
- `*` matches exactly one word
- `#` matches zero or more words

**Example**:
```
logs.error.* → Error logs
logs.# → All logs
```

**Use Case**: Event streaming, filtered message delivery

### 4. Headers Exchange
Routes messages based on message headers, not routing keys.

**Use Case**: Complex routing logic

## Quick Start

### 1. Start RabbitMQ

```bash
cd rabbitmq
docker-compose up -d
```

### 2. Access Management UI

Open http://localhost:15672
- Username: guest
- Password: guest

### 3. Run Producer

```bash
npm install
npm run producer
```

### 4. Run Consumer

```bash
npm run consumer
```

## Key Concepts

### Message Durability

- **Durable Queue**: Survives broker restart
- **Persistent Message**: Saved to disk

```javascript
await channel.assertQueue(QUEUE_NAME, { durable: true });
channel.sendToQueue(QUEUE_NAME, message, { persistent: true });
```

### Message Acknowledgment

- **Auto-ack**: Acknowledged immediately upon delivery
- **Manual-ack**: Acknowledged after successful processing

```javascript
await channel.consume(QUEUE_NAME, (msg) => {
  // Process message
  channel.ack(msg); // Acknowledge
  // channel.nack(msg, false, true); // Negative ack with requeue
});
```

### Prefetch/QoS

Limits the number of unacknowledged messages a consumer can receive.

```javascript
await channel.prefetch(1); // Process one message at a time
```

## Common Patterns

### Work Queue (Task Distribution)
Multiple consumers process tasks in parallel.

### Publish/Subscribe (Event Broadcasting)
One producer sends events to multiple consumers.

### RPC (Remote Procedure Call)
Client sends request, server sends response.

### Dead Letter Queue (DLQ)
Handles messages that couldn't be processed.

## Best Practices

1. **Use Durable Queues**: Prevents message loss
2. **Enable Message Persistence**: Save to disk
3. **Set Prefetch Limit**: Avoid overloading consumers
4. **Use Manual Acknowledgment**: Ensure processing before removing
5. **Implement Retry Logic**: Handle transient failures
6. **Monitor Queue Depth**: Prevent queue buildup
7. **Use Connection Pooling**: Reuse connections

## Troubleshooting

### Connection Refused
- Check if RabbitMQ is running
- Verify host and port (default: localhost:5672)

### Queue Not Found
- Ensure producer declares queue before consumer connects
- Use `durable: true` to persist queue

### Messages Not Being Consumed
- Check consumer connection
- Verify queue binding
- Check prefetch settings