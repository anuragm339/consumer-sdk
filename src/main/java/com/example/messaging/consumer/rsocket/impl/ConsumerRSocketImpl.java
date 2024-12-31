package com.example.messaging.consumer.rsocket.impl;

import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.models.Message;
import com.example.messaging.models.MessageState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRSocketImpl implements RSocket {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRSocketImpl.class);
    private final MessageHandler messageHandler;
    private final ObjectMapper objectMapper;
    private final String consumerId;

    public ConsumerRSocketImpl(MessageHandler messageHandler, String consumerId) {
        this.messageHandler = messageHandler;
        this.consumerId = consumerId;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received stream request: {}", data);

            // For consuming messages, we just need to keep the stream open
            return Flux.<Payload>create(sink -> {
                try {
                    Message message = deserializeMessage(payload);
                    messageHandler.handleMessage(message)
                            .doOnSuccess(v -> {
                                // Send acknowledgment after successful processing
                                sendAcknowledgment(message.getMsgOffset())
                                        .subscribe();
                            })
                            .subscribe();
                } catch (Exception e) {
                    sink.error(e);
                }
            });

        } catch (Exception e) {
            logger.error("Error in requestStream: {}", e.getMessage());
            return Flux.error(e);
        } finally {
            payload.release();
        }
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received fire and forget message: {}", data);

            Message message = deserializeMessage(payload);
            return messageHandler.handleMessage(message);
        } catch (Exception e) {
            logger.error("Error in fireAndForget: {}", e.getMessage());
            return Mono.error(e);
        } finally {
            payload.release();
        }
    }

    private Message deserializeMessage(Payload payload) {
        try {
            String json = payload.getDataUtf8();
            JsonNode root = objectMapper.readTree(json);
            JsonNode messageNode = root.get("message");

            return Message.builder()
                    .msgOffset(messageNode.get("msg_offset").asLong())
                    .type(messageNode.get("type").asText())
                    .data(Base64.getDecoder().decode(messageNode.get("data").asText()))
                    .createdUtc(Instant.ofEpochMilli(messageNode.get("created_utc").asLong()))
                    .state(MessageState.valueOf(messageNode.get("state").asText()))
                    .build();

        } catch (Exception e) {
            logger.error("Failed to deserialize message: {}", e.getMessage());
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    public Mono<Void> sendAcknowledgment(long messageId) {
        try {
            Map<String, Object> ack = new HashMap<>();
            ack.put("type", "ACK");
            ack.put("messageId", messageId);
            ack.put("consumerId", consumerId);
            ack.put("timestamp", Instant.now().toEpochMilli());

            String ackJson = objectMapper.writeValueAsString(ack);
            logger.info("Sending acknowledgment for message {}: {}", messageId, ackJson);

            return fireAndForget(DefaultPayload.create(ackJson));
        } catch (Exception e) {
            logger.error("Failed to send acknowledgment for message {}: {}",
                    messageId, e.getMessage());
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received request and response message: {}", data);

            Message message = deserializeMessage(payload);
            return messageHandler.handleFluxMessage(message);
        } catch (Exception e) {
            logger.error("Error in requestResponse: {}", e.getMessage());
            return Mono.error(e);
        } finally {
            payload.release();
        }
    }


    @Override
    public void dispose() {
        logger.info("Disposing RSocket connection for consumer: {}", consumerId);
    }
}
