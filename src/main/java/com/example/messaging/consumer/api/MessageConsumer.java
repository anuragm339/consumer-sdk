package com.example.messaging.consumer.api;

import com.example.messaging.models.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MessageConsumer {
    /**
     * Start consuming messages
     */
    Flux<Message> consume();

    /**
     * Acknowledge message processing
     */
    Mono<Void> acknowledge(long messageId);

    /**
     * Connect to the message server
     */
    Mono<Void> connect();

    /**
     * Disconnect from the message server
     */
    Mono<Void> disconnect();

    public ConsumerStatus getStatus();
}
