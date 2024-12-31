package com.example.messaging.consumer.core;

import com.example.messaging.consumer.api.ConsumerStatus;
import com.example.messaging.consumer.api.MessageConsumer;
import com.example.messaging.consumer.connection.ConnectionManager;
import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.models.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultMessageConsumer implements MessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageConsumer.class);

    private final ConnectionManager connectionManager;
    private final AtomicReference<ConsumerStatus> status;
    private final String consumerId;

    public DefaultMessageConsumer(ConnectionManager connectionManager, String consumerId) {
        this.connectionManager = connectionManager;
        this.consumerId = consumerId;
        this.status = new AtomicReference<>(ConsumerStatus.DISCONNECTED);
    }

    @Override
    public Flux<Message> consume() {
        if (status.get() != ConsumerStatus.CONNECTED) {
            return Flux.error(new IllegalStateException("Consumer is not connected"));
        }

        status.set(ConsumerStatus.CONSUMING);
        return connectionManager.getConnection()
                .flatMapMany(rSocket -> rSocket.requestStream(DefaultPayload.create("")))
                .map(payload -> {
                    try{
                        String json = payload.getDataUtf8();
                        JsonNode root = new ObjectMapper().readTree(json);
                        JsonNode messageNode = root.get("message");
                        return Message.builder()
                                .msgOffset(messageNode.get("offset").asLong())
                                .type(messageNode.get("type").asText())
                                .data(Base64.getDecoder().decode(messageNode.get("data").asText()))
                                .createdUtc(Instant.ofEpochMilli(messageNode.get("createdUtc").asLong()))
                                .build();
                    } catch (JsonMappingException e) {
                        throw new RuntimeException(e);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .doOnNext(message ->
                        logger.info("Received message for consumer: {} with offset: {}",
                                consumerId, message.getMsgOffset()))
                .doOnError(error -> {
                    logger.error("Error consuming messages: {}", error.getMessage());
                    status.set(ConsumerStatus.ERROR);
                });
    }

    @Override
    public Mono<Void> acknowledge(long messageId) {
        return connectionManager.getConnection()
                .flatMap(rSocket -> rSocket.fireAndForget(DefaultPayload.create(String.valueOf(messageId))))
                .doOnSuccess(v -> logger.info("Acknowledged message {}", messageId))
                .doOnError(e -> logger.error("Failed to acknowledge message {}", messageId));
    }

    @Override
    public Mono<Void> connect() {
        status.set(ConsumerStatus.CONNECTING);
        return connectionManager.getConnection()
                .then()
                .doOnSuccess(v -> {
                    status.set(ConsumerStatus.CONNECTED);
                    logger.info("Consumer {} connected successfully", consumerId);
                })
                .doOnError(error -> {
                    status.set(ConsumerStatus.ERROR);
                    logger.error("Failed to connect consumer {}: {}", consumerId, error.getMessage());
                });
    }

    @Override
    public Mono<Void> disconnect() {
        return connectionManager.disconnect()
                .doFinally(signalType -> {
                    status.set(ConsumerStatus.DISCONNECTED);
                    logger.info("Consumer {} disconnected", consumerId);
                });
    }

    @Override
    public ConsumerStatus getStatus() {
        return status.get();
    }
}
