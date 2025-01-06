package com.example.messaging.consumer.rsocket.impl;

import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.models.Message;
import com.example.messaging.models.MessageState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        this.objectMapper.registerModule(new JavaTimeModule());
    }


    @Override
    public Flux<Payload> requestStream(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received stream request: {}", data);

            Message message = deserializeMessage(payload);
            return messageHandler.handleMessage(message)
                    .then(sendAcknowledgment(message))  // Send ack after successful handling
                    .thenReturn(DefaultPayload.create("ACK"))  // Return ack payload
                    .flux();  // Convert to Flux

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

                        Message message = deserializeMessage(payload);
                        return messageHandler.handleMessage(message)
                                .then(createAckPayload(message));  // Send ack after successful handling
                        // Process acknowledgment
                    } finally {
                        payload.release();
                    }
                });
    }

    private Mono<Payload> createAckPayload(Message message) {
        try {
            Map<String, Object> ack = new HashMap<>();
            ack.put("type", "ACK");
            ack.put("message", message);
            ack.put("consumerId", consumerId);
            ack.put("timestamp", Instant.now().toEpochMilli());

            String ackJson = objectMapper.writeValueAsString(ack);
            logger.debug("Creating acknowledgment for message {}: {}", message.getMsgOffset(), ackJson);

            return Mono.just(DefaultPayload.create(ackJson));
        } catch (Exception e) {
            logger.error("Failed to create acknowledgment for message {}: {}",
                    message.getMsgOffset(), e.getMessage());
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            String data = payload.getDataUtf8();
            logger.info("Received request and response message: {}", data);

            Message message = deserializeMessage(payload);
            return messageHandler.handleMessage(message)
                    .then(sendAcknowledgment(message))  // Send ack after successful handling
                    .thenReturn(DefaultPayload.create("ACK"));  // Return ack payload

        } catch (Exception e) {
            logger.error("Error in requestResponse: {}", e.getMessage());
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

    public Mono<Void> sendAcknowledgment(Message message) {
        try {
            Map<String, Object> ack = new HashMap<>();
            ack.put("type", "ACK");
            ack.put("message", message);
            ack.put("consumerId", consumerId);
            ack.put("timestamp", Instant.now().toEpochMilli());

            String ackJson = objectMapper.writeValueAsString(ack);
            logger.info("Sending acknowledgment for message {}: {}", message.getMsgOffset(), ackJson);

            return requestResponse(DefaultPayload.create(ackJson)).then();
        } catch (Exception e) {
            logger.error("Failed to send acknowledgment for message {}: {}",
                    message.getMsgOffset(), e.getMessage());
            return Mono.error(e);
        }
    }





    @Override
    public void dispose() {
        logger.debug("Disposing RSocket connection for consumer: {}", consumerId);
    }
}
