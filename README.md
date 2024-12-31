# Message Consumer SDK

## Overview
A reactive Java SDK for consuming messages from the Message Provider Service with support for automatic reconnection, batch acknowledgment, and resilient error handling.

### Architecture Overview

Based on the diagrams, the Consumer SDK follows a layered architecture:

1. **API Layer**
   - `MessageConsumer` interface - Primary entry point
   - `ConsumerFactory` - Factory for creating consumer instances
   - `ConsumerBuilder` - Fluent builder for consumer configuration

2. **Core Layer**
   - `DefaultMessageConsumer` - Main implementation of MessageConsumer
   - `ConnectionManager` - Manages RSocket connections
   - `MessageSerializer` - Handles message serialization/deserialization

3. **Network Layer**
   - `ConsumerRSocketImpl` - RSocket protocol implementation
   - `PayloadCreator` - Creates RSocket payloads
   - `ConnectionMetadata` - Connection metadata management

## Features

- Reactive Streams based API using Project Reactor
- Automatic connection management and reconnection
- Batch message acknowledgment support
- Comprehensive error handling with retries
- Performance metrics collection
- Group-based message consumption
- Message replay capability

## Installation

```xml
<dependency>
    <groupId>com.example.messaging</groupId>
    <artifactId>message-consumer-sdk</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

```java
// Create consumer configuration
ConsumerConfig config = ConsumerConfig.builder()
    .serverHost("localhost")
    .serverPort(7000)
    .consumerId("consumer-1")
    .groupId("group-1")
    .maxRetryAttempts(3)
    .retryDelayMillis(1000)
    .maxConcurrentMessages(100)
    .build();

// Create consumer instance
MessageConsumer consumer = ConsumerFactory.createConsumer(config);

// Connect and consume messages
consumer.connect()
    .thenMany(consumer.consume(0))
    .subscribe(message -> {
        // Process message
        processMessage(message);
        
        // Acknowledge message
        consumer.acknowledge(message.getMsgOffset())
            .subscribe();
    });
```

## Connection Management

The SDK handles connections through the `ConnectionManager`:

```java
// Automatic reconnection with backoff
consumer.connect()
    .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
        .maxBackoff(Duration.ofSeconds(30)))
    .subscribe();
```

## Message Consumption

### Single Message Processing

```java
consumer.consume(lastProcessedOffset)
    .subscribe(message -> {
        try {
            // Process message
            processMessage(message);
            
            // Acknowledge
            consumer.acknowledge(message.getMsgOffset())
                .subscribe();
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    });
```

### Batch Processing

```java
consumer.consume(lastProcessedOffset)
    .buffer(100)  // Batch size
    .flatMap(messages -> {
        // Process batch
        processBatch(messages);
        
        // Batch acknowledge
        List<Long> offsets = messages.stream()
            .map(Message::getMsgOffset)
            .collect(Collectors.toList());
            
        return consumer.acknowledgeBatch(offsets);
    })
    .subscribe();
```

## Error Handling

The SDK provides comprehensive error handling:

```java
consumer.consume(lastProcessedOffset)
    .doOnError(error -> {
        if (error instanceof ConsumerException) {
            ConsumerException ce = (ConsumerException) error;
            if (ce.isRetryable()) {
                // Handle retryable error
            } else {
                // Handle non-retryable error
            }
        }
    })
    .retry(when -> when.handler(error -> {
        // Custom retry logic
        return error instanceof ConsumerException && 
               ((ConsumerException) error).isRetryable();
    }))
    .subscribe();
```

## Metrics

The SDK provides built-in metrics:

```java
ConsumerMetrics metrics = consumer.getMetrics();

// Available metrics
long messagesReceived = metrics.getMessagesReceived();
long messagesAcknowledged = metrics.getMessagesAcknowledged();
long errors = metrics.getErrors();
Timer processingTime = metrics.getProcessingTimer();
long lastOffset = metrics.getLastProcessedOffset();
```

## Best Practices

1. **Connection Management**
   - Always handle disconnects gracefully
   - Implement appropriate retry strategies
   - Monitor connection health

2. **Message Processing**
   - Use batch processing for better performance
   - Implement proper error handling
   - Keep track of processed offsets

3. **Resource Management**
   - Close consumers when done
   - Monitor memory usage
   - Use appropriate batch sizes

4. **Error Handling**
   - Distinguish between retryable and non-retryable errors
   - Implement circuit breakers for external dependencies
   - Log errors appropriately

## Advanced Configuration

```java
ConsumerConfig config = ConsumerConfig.builder()
    .serverHost("localhost")
    .serverPort(7000)
    .consumerId("consumer-1")
    .groupId("group-1")
    .maxRetryAttempts(3)
    .retryDelayMillis(1000)
    .maxConcurrentMessages(100)
    .build();

// Custom message handler
MessageHandler customHandler = message -> {
    // Custom message handling logic
    return Mono.empty();
};

// Create consumer with custom configuration
MessageConsumer consumer = new ConsumerBuilder()
    .withConfig(config)
    .withMessageHandler(customHandler)
    .withMetricsRegistry(meterRegistry)
    .build();
```

## Troubleshooting

Common issues and solutions:

1. Connection Issues
   - Check network connectivity
   - Verify server host and port
   - Check consumer ID and group ID

2. Message Processing Issues
   - Verify message format
   - Check for processing timeouts
   - Monitor error rates

3. Performance Issues
   - Adjust batch sizes
   - Monitor processing times
   - Check resource usage

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
