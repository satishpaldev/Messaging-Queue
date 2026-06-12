# Kafka Guide

## What is Kafka?

Apache Kafka is a distributed event streaming platform that stores a continuous feed of data records called events. It's built to handle high-throughput, low-latency streaming data.

## Architecture

```
Producer → Topic (Partitions) → Consumer Group
```

### Components

1. **Producer**: Publishes messages to topics
2. **Topic**: Named feed of records
3. **Partition**: Topic is split across multiple partitions
4. **Consumer**: Reads messages from topics
5. **Consumer Group**: Multiple consumers share topic consumption
6. **Broker**: Server that manages topics and partitions
7. **Zookeeper**: Manages cluster coordination

## Key Concepts

### Topics and Partitions

A topic is divided into partitions for parallel processing.

```
Topic: user-events
├── Partition 0 → [msg1, msg3, msg5, ...]
├── Partition 1 → [msg2, msg4, msg6, ...]
└── Partition 2 → [msg7, msg8, msg9, ...]
```

**Benefits**:
- Parallel processing
- High throughput
- Replication for fault tolerance

### Consumer Groups

Multiple consumers in a group read from different partitions.

```
Consumer Group 1
├── Consumer 1 → Partition 0
├── Consumer 2 → Partition 1
└── Consumer 3 → Partition 2
```

**Features**:
- Each partition read by one consumer
- Automatic rebalancing
- Offset management

### Offset Management

Kafka tracks the position (offset) of each message.

```
Partition 0: [0: msg1, 1: msg2, 2: msg3, 3: msg4, ...]
             ↑
             Offset
```

**Strategies**:
- `earliest`: Start from beginning
- `latest`: Start from end
- Specific offset: Resume from saved position

## Quick Start

### 1. Start Kafka Cluster

```bash
cd kafka
docker-compose up -d
```

### 2. Access Kafka UI

Open http://localhost:8080

### 3. Run Producer

```bash
npm install
npm run producer
```

### 4. Run Consumer

```bash
npm run consumer
```

## Message Delivery Guarantees

### At Most Once
- Messages delivered once or not at all
- Highest performance, lowest reliability

### At Least Once
- Messages guaranteed to be delivered
- May receive duplicates
- Most common choice

### Exactly Once
- Each message processed once
- Highest reliability, lowest performance
- Requires idempotent processing

## Partitioning Strategy

### By Key
Messages with same key go to same partition (default).

```javascript
{
  topic: 'orders',
  messages: [
    { key: 'customer-123', value: 'order-1' },
    { key: 'customer-123', value: 'order-2' }, // Same partition
    { key: 'customer-456', value: 'order-3' },
  ]
}
```

**Use Case**: Maintain order for same entity

### Round-Robin
Default when no key provided.

**Use Case**: Load distribution

## Consumer Group Management

### Rebalancing

Automatically redistributes partitions when:
- Consumer joins group
- Consumer leaves group
- New partition added to topic

```
Before: Consumer 1 → P0, P1
After:  Consumer 1 → P0
        Consumer 2 → P1
```

### Offset Commits

Save consumer position for resumption.

```javascript
// Automatic commit
autoCommit: true // Default interval: 5000ms

// Manual commit
await consumer.commitOffsets([{
  topic: TOPIC_NAME,
  partition: 0,
  offset: '10'
}]);
```

## Performance Tuning

### Producer Settings
```javascript
{
  allowAutoTopicCreation: false,
  idempotent: true,           // Prevent duplicates
  maxInFlightRequests: 5,     // Concurrency
  compression: 'snappy',      // Compress messages
}
```

### Consumer Settings
```javascript
{
  sessionTimeout: 30000,       // Time before rebalance
  rebalanceTimeout: 60000,     // Time to rejoin
  heartbeatInterval: 3000,     // Keep-alive
}
```

## Best Practices

1. **Use Durable Storage**: Kafka persists all messages
2. **Set Retention Policy**: Define message retention time
3. **Monitor Consumer Lag**: Track offset behind latest
4. **Use Consumer Groups**: Scale consumption
5. **Implement Idempotency**: Handle duplicate messages
6. **Enable Compression**: Reduce network bandwidth
7. **Batch Messages**: Improve throughput
8. **Monitor Broker Health**: Watch partition replicas

## Common Patterns

### Event Sourcing
Store all state changes as events.

### Stream Processing
Process continuous stream of data.

### Change Data Capture (CDC)
Capture database changes as events.

### Event Replay
Replay events for analysis or recovery.

## Troubleshooting

### Consumer Lag
- Consumer processing too slowly
- Increase number of consumers
- Optimize consumer code

### Partition Rebalancing
- Check broker logs
- Verify network connectivity
- Adjust rebalance timeout

### Message Loss
- Enable message replication
- Increase replication factor
- Verify producer acknowledgments