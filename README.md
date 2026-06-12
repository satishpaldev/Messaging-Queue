# Messaging Queue - RabbitMQ & Kafka

A comprehensive demonstration of message queuing systems using **RabbitMQ** and **Kafka**. This project showcases best practices for building scalable, decoupled microservices using asynchronous messaging patterns.

## 📋 Project Structure

```
├── rabbitmq/                 # RabbitMQ implementations
│   ├── basic-queue/         # Basic producer-consumer pattern
│   ├── routing/             # Routing with exchange types
│   ├── topic-exchange/      # Topic-based message distribution
│   ├── rpc/                 # Remote Procedure Call pattern
│   └── docker-compose.yml   # RabbitMQ Docker setup
├── kafka/                    # Kafka implementations
│   ├── producer/            # Message producer
│   ├── consumer/            # Message consumer
│   ├── topics/              # Topic management
│   └── docker-compose.yml   # Kafka Docker setup
├── docs/                     # Documentation
└── examples/                 # Integration examples
```

## 🚀 Features

### RabbitMQ
- **Simple Queues**: Basic producer-consumer messaging
- **Exchanges & Routing**: Direct, topic, and fanout exchanges
- **RPC Pattern**: Request-reply communication
- **Dead Letter Queues**: Error handling and retry logic
- **Message Persistence**: Durable queues and messages

### Kafka
- **Topics & Partitions**: Distributed data storage
- **Consumer Groups**: Scalable message consumption
- **Offset Management**: Message replay and state management
- **Producers & Consumers**: High-throughput messaging

## 📦 Prerequisites

- Docker & Docker Compose
- Node.js (v18+) or Python (v3.9+)
- RabbitMQ Server (or Docker container)
- Kafka Server (or Docker container)

## 🔧 Quick Start

### RabbitMQ

```bash
# Start RabbitMQ container
cd rabbitmq
docker-compose up -d

# Install dependencies
npm install

# Run producer
node basic-queue/producer.js

# Run consumer
node basic-queue/consumer.js
```

### Kafka

```bash
# Start Kafka container
cd kafka
docker-compose up -d

# Install dependencies
npm install

# Run producer
node producer/index.js

# Run consumer
node consumer/index.js
```

## 📚 Documentation

- [RabbitMQ Guide](./docs/rabbitmq-guide.md)
- [Kafka Guide](./docs/kafka-guide.md)
- [Message Patterns](./docs/patterns.md)
- [Best Practices](./docs/best-practices.md)

## 🎯 Use Cases

- **Event-Driven Architecture**: Decouple services with async events
- **Load Balancing**: Distribute work across multiple workers
- **Real-time Processing**: Handle streaming data
- **Service Communication**: Reliable inter-service messaging
- **Task Queuing**: Background job processing

## 📝 License

MIT

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.