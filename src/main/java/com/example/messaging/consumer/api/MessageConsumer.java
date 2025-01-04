package com.example.messaging.consumer.api;

import com.example.messaging.models.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MessageConsumer {
    /**
     * Connect to message provider and start consuming messages
     * Messages will be delivered to registered MessageHandler
     */
    Mono<Object> connect();

    /**
     * Disconnect from message provider and stop consuming
     */
    Mono<Void> disconnect();

    /**
     * Get current consumer status
     */
    ConsumerStatus getStatus();

    /**
     * Get consumer ID
     */
    String getConsumerId();
}
