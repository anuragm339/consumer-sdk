package com.example.messaging.consumer.handler;

import com.example.messaging.models.BatchMessage;
import com.example.messaging.models.Message;
import io.rsocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class DefaultMessageHandler implements MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageHandler.class);

    private final ConcurrentHashMap<String, Consumer<Message>> messageTypeHandlers;
    private final ConcurrentHashMap<String, Consumer<BatchMessage>> batchMessageTypeHandlers;
    private final String consumerId;

    public DefaultMessageHandler(String consumerId) {
        this.consumerId = consumerId;
        this.messageTypeHandlers = new ConcurrentHashMap<>();
        this.batchMessageTypeHandlers = new ConcurrentHashMap<>();
        registerDefaultHandlers();
    }

    @Override
    public Mono<Void> handleMessage(Message message) {
        return Mono.defer(() -> {
            try {
                logger.debug("Processing message: type={}, offset={} for consumer={}", message.getType(), message.getMsgOffset(), consumerId);

                Consumer<Message> handler = messageTypeHandlers.getOrDefault(
                        message.getType(),
                        this::handleUnknownMessageType
                );

                handler.accept(message);
                logger.debug("Successfully processed message: offset={}", message.getMsgOffset());

                return Mono.empty();
            } catch (Exception e) {
                logger.error("Error processing message: offset={}, error={}",
                        message.getMsgOffset(), e.getMessage());
                return Mono.error(e);
            }
        });
    }

    /**
     * Register a handler for a specific message type
     */
    public void registerHandler(String messageType, Consumer<Message> handler) {
        messageTypeHandlers.put(messageType, handler);
        logger.info("Registered handler for message type: {}", messageType);
    }

    public void registerBatchHandler(String messageType, Consumer<BatchMessage> handler) {
        batchMessageTypeHandlers.put(messageType, handler);
        logger.info("Registered handler for message type: {}", messageType);
    }

    /**
     * Remove a handler for a specific message type
     */
    public void removeHandler(String messageType) {
        messageTypeHandlers.remove(messageType);
        logger.info("Removed handler for message type: {}", messageType);
    }

    private void registerDefaultHandlers() {
        // Register default handler for standard message types
        registerHandler("DEFAULT", message ->
                logger.info("Handling default message: {}", message.getMsgOffset()));

        registerHandler("CONTROL", message ->
                logger.info("Handling control message: {}", message.getMsgOffset()));

        // Add more default handlers as needed
    }

    private void handleUnknownMessageType(Message message) {
        logger.warn("Received unknown message type: {} for offset={}",
                message.getType(), message.getMsgOffset());
    }

    /**
     * Clear all registered handlers
     */
    public void clearHandlers() {
        messageTypeHandlers.clear();
        logger.info("Cleared all message handlers");
    }

    /**
     * Check if a handler exists for a message type
     */
    public boolean hasHandler(String messageType) {
        return messageTypeHandlers.containsKey(messageType);
    }


    @Override
    public Mono<Payload> handleFluxMessage(Message message) {
        return Mono.defer(() -> {
            try {
                logger.debug("Processing message: type={}, offset={} for consumer={}",
                        message.getType(), message.getMsgOffset(), consumerId);

                Consumer<Message> handler = messageTypeHandlers.getOrDefault(
                        message.getType(),
                        this::handleUnknownMessageType
                );

                handler.accept(message);
                logger.info("Successfully processed message: offset={}", message.getMsgOffset());

                return Mono.empty();
            } catch (Exception e) {
                logger.error("Error processing message: offset={}, error={}",
                        message.getMsgOffset(), e.getMessage());
                return Mono.error(e);
            }
        });
    }

    @Override
    public Mono<Void> handleBatchMessage(BatchMessage batchMessage) {
        try {
            Consumer<BatchMessage> handler = batchMessageTypeHandlers.getOrDefault(batchMessage.getType(), this::handleUnknownBatchMessageType);
            handler.accept(batchMessage);
            return Mono.empty();
        }catch (Exception e) {
            logger.error("Error processing message: offset={}, error={}", batchMessage.getBatchId(), e.getMessage(),e);
            return Mono.error(e);
        }


    }

    private void handleUnknownBatchMessageType(BatchMessage message) {
        logger.debug("Received unknown message type: {} for offset={}", message.getType(), message.getBatchId());
    }
}
