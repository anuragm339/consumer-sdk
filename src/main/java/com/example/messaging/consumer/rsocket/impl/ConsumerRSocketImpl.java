package com.example.messaging.consumer.rsocket.impl;

import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.models.BatchMessage;
import com.example.messaging.models.Message;
import com.example.messaging.models.MessageState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class ConsumerRSocketImpl implements RSocket {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRSocketImpl.class);
    private final MessageHandler messageHandler;
    private final ObjectMapper objectMapper;
    private final String consumerId;

    public ConsumerRSocketImpl(MessageHandler messageHandler, String consumerId) {
        this.messageHandler = messageHandler;
        this.consumerId = consumerId;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received stream request: {}", data);
            JsonNode root = objectMapper.readTree(data);

            if (root.has("batchMessage")) {
                BatchMessage batchMessage = deserializeBatchMessage(payload);
                Payload acknowledgment = sendBatchAcknowledgment(batchMessage);
                return messageHandler.handleBatchMessage(batchMessage)
                        .thenReturn(acknowledgment)
                        .flux();
            } else {
                Message message = deserializeMessage(payload);
                return messageHandler.handleMessage(message)
                        .then(sendAcknowledgment(message))
                        .thenReturn(DefaultPayload.create("ACK"))
                        .flux();
            }
        } catch (Exception e) {
            logger.error("Error in requestStream: {}", e.getMessage());
            return Flux.error(e);
        } finally {
            payload.release();
        }
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(payloads)
                .flatMap(payload -> {
                    try {
                        String data = payload.getDataUtf8();
                        logger.debug("Received requestChannel message: {}", data);
                        JsonNode root = objectMapper.readTree(data);

                        if (root.has("batchMessage")) {
                            BatchMessage batchMessage = deserializeBatchMessage(payload);
                            Payload acknowledgment = sendBatchAcknowledgment(batchMessage);
                            return messageHandler.handleBatchMessage(batchMessage)
                                    .thenReturn(acknowledgment);
                        } else {
                            Message message = deserializeMessage(payload);
                            return messageHandler.handleMessage(message)
                                    .then(sendAcknowledgment(message))
                                    .thenReturn(DefaultPayload.create("ACK"));
                        }
                    } catch (JsonMappingException e) {
                        logger.error("Error deserializing requestChannel message: {}", e.getMessage());
                        throw new RuntimeException(e);
                    } catch (JsonProcessingException e) {
                        logger.error("Error processing requestChannel message: {}", e.getMessage());
                        throw new RuntimeException(e);
                    } finally {
                        payload.release();
                    }
                });
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received request-response message: {}", data);
            JsonNode root = objectMapper.readTree(data);
            String type = root.get("type").asText();

            switch(type) {
                case "BATCH_ACK":
                case "ACK":
                    return Mono.just(DefaultPayload.create(data));
                default:
                    if (root.has("batchMessage")) {
                        BatchMessage batchMessage = deserializeBatchMessage(payload);
                        Payload acknowledgment = sendBatchAcknowledgment(batchMessage);
                        return messageHandler.handleBatchMessage(batchMessage)
                                .thenReturn(acknowledgment);
                    } else {
                        Message message = deserializeMessage(payload);
                        return messageHandler.handleMessage(message)
                                .then(sendAcknowledgment(message))
                                .thenReturn(DefaultPayload.create("ACK"));
                    }
            }
        } catch (Exception e) {
            logger.error("Error in requestResponse: {}", e.getMessage());
            return Mono.error(e);
        } finally {
            payload.release();
        }
    }

    private BatchMessage deserializeBatchMessage(Payload payload) {
        try {
            String json = payload.getDataUtf8();
            JsonNode root = objectMapper.readTree(json);
            if (root.get("batchMessage") != null) {
                JsonNode messageNode = root.get("batchMessage");
                String batchId = messageNode.get("batchId").asText();
                String type = messageNode.get("type").asText();
                Long count = messageNode.get("count").asLong();
                List<Message> messages = new ArrayList<>();

                ((ArrayNode) messageNode.get("data")).forEach(node -> {
                    Message message = Message.builder()
                            .msgOffset(node.get("msg_offset").asLong())
                            .type(node.get("type").asText())
                            .data(Base64.getDecoder().decode(node.get("data").asText()))
                            .createdUtc(Instant.ofEpochMilli(node.get("created_utc").asLong()))
                            .state(MessageState.valueOf(node.get("state").asText()))
                            .build();
                    messages.add(message);
                });

                logger.info("Received batch message with id: {} and count: {}", batchId, count);
                return new BatchMessage(batchId, count, messages, type);
            }
            return null;
        } catch (Exception e) {
            logger.error("Failed to deserialize batch message: {}", e.getMessage());
            throw new RuntimeException("Failed to deserialize batch message", e);
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

    private Payload sendBatchAcknowledgment(BatchMessage batchMessage) {
        try {
            Map<String, Object> ack = new HashMap<>();
            ack.put("type", "BATCH_ACK");
            ack.put("batchId", batchMessage.getBatchId());
            ack.put("consumerId", consumerId);
            ack.put("count", batchMessage.getCount());
            ack.put("messageOffsets", batchMessage.getData().stream()
                    .map(Message::getMsgOffset)
                    .collect(Collectors.toList()));
            ack.put("timestamp", Instant.now().toEpochMilli());
            String ackJson = objectMapper.writeValueAsString(ack);
            return DefaultPayload.create(ackJson);
        } catch (Exception e) {
            logger.error("Failed to send batch acknowledgment: {}", e.getMessage());
            throw new RuntimeException("Failed to send batch acknowledgment", e);
        }
    }

    private Mono<Void> sendAcknowledgment(Message message) {
        try {
            Map<String, Object> ack = new HashMap<>();
            ack.put("type", "ACK");
            ack.put("messageOffset", message.getMsgOffset());
            ack.put("consumerId", consumerId);
            ack.put("timestamp", Instant.now().toEpochMilli());

            String ackJson = objectMapper.writeValueAsString(ack);
            logger.info("Sending acknowledgment for message {}: {}",
                    message.getMsgOffset(), ackJson);

            return requestResponse(DefaultPayload.create(ackJson)).then();
        } catch (Exception e) {
            logger.error("Failed to send acknowledgment: {}", e.getMessage());
            return Mono.error(e);
        }
    }

    @Override
    public void dispose() {
        logger.info("Disposing RSocket connection for consumer: {}", consumerId);
    }
}
