# Best Practices for Message Queuing

## 1. Error Handling

### RabbitMQ - Dead Letter Queue (DLQ)
```javascript
// Original queue
await channel.assertQueue('tasks', {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': 'dlx',
    'x-message-ttl': 60000, // 1 minute
  }
});

// DLX
await channel.assertExchange('dlx', 'direct', { durable: true });
await channel.assertQueue('tasks.dlq', { durable: true });
await channel.bindQueue('tasks.dlq', 'dlx', '');
```

### Kafka - Error Topic
```javascript
try {
  // Process message
} catch (error) {
  // Send to error topic
  await producer.send({
    topic: 'errors',
    messages: [{ value: JSON.stringify(error) }]
  });
}
```

## 2. Monitoring and Observability

### Key Metrics
- **Queue Depth**: Number of messages waiting
- **Consumer Lag**: Kafka-specific metric
- **Processing Rate**: Messages processed per second
- **Error Rate**: Failed message percentage
- **Latency**: End-to-end message processing time

### Implementation
```javascript
// Simple metrics tracking
const metrics = {
  messagesProcessed: 0,
  messagesFailed: 0,
  processingTime: [],
};

console.log(`Queue Depth: ${queue.messageCount}`);
console.log(`Consumer Lag: ${offset - committed}`);
```

## 3. Message Schema Management

### Use Schema Validation
```javascript
const Joi = require('joi');

const messageSchema = Joi.object({
  id: Joi.string().required(),
  type: Joi.string().required(),
  payload: Joi.object().required(),
  timestamp: Joi.date().required(),
});

const validationResult = messageSchema.validate(message);
```

### Versioning
```javascript
{
  id: 'evt-123',
  version: '1.0',
  type: 'UserCreated',
  data: { /* ... */ }
}
```

## 4. Idempotency

### Problem
Duplicate message processing causes issues.

### Solution
```javascript
// Track processed message IDs
const processedIds = new Set();

async function processMessage(msg) {
  const id = msg.id;
  
  if (processedIds.has(id)) {
    console.log(`Duplicate: ${id}`);
    return;
  }
  
  // Process message
  processedIds.add(id);
}
```

## 5. Connection Management

### Connection Pooling
```javascript
// Reuse connections
class MessageQueuePool {
  constructor(size = 5) {
    this.connections = [];
  }
  
  async getConnection() {
    if (this.connections.length > 0) {
      return this.connections.pop();
    }
    return await amqp.connect(URL);
  }
  
  releaseConnection(conn) {
    this.connections.push(conn);
  }
}
```

## 6. Backpressure Handling

### RabbitMQ Prefetch
```javascript
// Don't overwhelm consumer
await channel.prefetch(10); // Max 10 unacked messages
```

### Kafka Consumer Pause/Resume
```javascript
if (isOverloaded) {
  consumer.pause(['topic-name']);
} else {
  consumer.resume(['topic-name']);
}
```

## 7. Circuit Breaker Pattern

```javascript
class CircuitBreaker {
  constructor(threshold = 5, timeout = 60000) {
    this.failureCount = 0;
    this.threshold = threshold;
    this.timeout = timeout;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
  }
  
  async execute(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailure > this.timeout) {
        this.state = 'HALF_OPEN';
      } else {
        throw new Error('Circuit breaker is OPEN');
      }
    }
    
    try {
      const result = await fn();
      if (this.state === 'HALF_OPEN') {
        this.state = 'CLOSED';
        this.failureCount = 0;
      }
      return result;
    } catch (error) {
      this.failureCount++;
      this.lastFailure = Date.now();
      if (this.failureCount >= this.threshold) {
        this.state = 'OPEN';
      }
      throw error;
    }
  }
}
```

## 8. Message Retry Strategy

### Exponential Backoff
```javascript
function calculateRetryDelay(attempt) {
  return Math.min(1000 * Math.pow(2, attempt), 30000);
}

// Attempt 0: 1s
// Attempt 1: 2s
// Attempt 2: 4s
// Attempt 3: 8s
```

### Implementation
```javascript
const MAX_RETRIES = 3;

async function processWithRetry(message) {
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      return await processMessage(message);
    } catch (error) {
      if (attempt < MAX_RETRIES - 1) {
        const delay = calculateRetryDelay(attempt);
        await sleep(delay);
      } else {
        // Send to DLQ
        throw error;
      }
    }
  }
}
```

## 9. Security

### Authentication
```javascript
const connection = await amqp.connect({
  protocol: 'amqp',
  hostname: 'rabbitmq.example.com',
  port: 5672,
  username: process.env.RABBITMQ_USER,
  password: process.env.RABBITMQ_PASS,
});
```

### Message Encryption
```javascript
const crypto = require('crypto');

function encryptMessage(message, key) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(key), iv);
  let encrypted = cipher.update(JSON.stringify(message));
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return iv.toString('hex') + ':' + encrypted.toString('hex');
}
```

## 10. Testing

### Mock Producer/Consumer
```javascript
const mockProducer = {
  send: jest.fn(),
  connect: jest.fn(),
};

test('should send message', async () => {
  await sendMessage(mockProducer, { id: 1 });
  expect(mockProducer.send).toHaveBeenCalledWith({
    topic: 'messages',
    messages: expect.any(Array),
  });
});
```

### Integration Testing
```bash
# Use Docker Compose for integration tests
docker-compose -f docker-compose.test.yml up -d
npm test
docker-compose -f docker-compose.test.yml down
```
