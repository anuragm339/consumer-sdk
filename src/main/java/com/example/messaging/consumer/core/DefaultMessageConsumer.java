package com.example.messaging.consumer.core;

import com.example.messaging.consumer.api.ConsumerStatus;
import com.example.messaging.consumer.api.MessageConsumer;
import com.example.messaging.consumer.connection.ConnectionManager;
import com.example.messaging.consumer.handler.MessageHandler;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

public class DefaultMessageConsumer implements MessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageConsumer.class);

    private final ConnectionManager connectionManager;
    private final String consumerId;
    private final MessageHandler messageHandler;
    private final AtomicReference<ConsumerStatus> status;

    public DefaultMessageConsumer(
            ConnectionManager connectionManager,
            String consumerId,
            MessageHandler messageHandler) {
        this.connectionManager = connectionManager;
        this.consumerId = consumerId;
        this.messageHandler = messageHandler;
        this.status = new AtomicReference<>(ConsumerStatus.DISCONNECTED);
    }

    @Override
    public Mono<Object> connect() {
        if (status.get() != ConsumerStatus.DISCONNECTED) {
            return Mono.error(new IllegalStateException("Consumer is already " + status.get()));
        }

        status.set(ConsumerStatus.CONNECTING);
        return connectionManager.getConnection()
                .flatMap(rSocket -> {
                    status.set(ConsumerStatus.CONNECTED);
                    // Start message stream but don't return it since messages go to handler
                    rSocket.requestStream(DefaultPayload.create(""))
                            .doOnNext(payload -> {
                                logger.debug("Received message for consumer: {}", consumerId);
                                // Messages will be handled by ConsumerRSocketImpl
                            })
                            .doOnError(error -> {
                                logger.error("Error in message stream: {}", error.getMessage());
                                status.set(ConsumerStatus.ERROR);
                            })
                            .subscribe();
                    return Mono.empty();
                })
                .doOnError(error -> {
                    logger.error("Failed to connect: {}", error.getMessage());
                    status.set(ConsumerStatus.ERROR);
                });
    }

    @Override
    public Mono<Void> disconnect() {
        return connectionManager.disconnect()
                .doFinally(__ -> status.set(ConsumerStatus.DISCONNECTED));
    }

    @Override
    public ConsumerStatus getStatus() {
        return status.get();
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }
}
