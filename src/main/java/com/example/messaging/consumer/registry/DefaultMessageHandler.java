package com.example.messaging.consumer.registry;

import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.models.Message;
import com.example.messaging.models.BatchMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Map;

public class DefaultMessageHandler implements MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageHandler.class);

    private final Object bean;
    private final Map<String, Method> handlers;

    public DefaultMessageHandler(Object bean, Map<String, Method> handlers) {
        this.bean = bean;
        this.handlers = handlers;
    }

    @Override
    public Mono<Void> handleMessage(Message message) {
        return Mono.defer(() -> {
            try {
                Method handler = handlers.get(message.getType());
                if (handler != null) {
                    handler.setAccessible(true);
                    handler.invoke(bean, message);
                    logger.debug("Successfully handled message of type: {}", message.getType());
                    return Mono.empty();
                } else {
                    logger.warn("No handler found for message type: {}", message.getType());
                    return Mono.empty();
                }
            } catch (Exception e) {
                logger.error("Failed to handle message: {}", e.getMessage());
                return Mono.error(e);
            }
        });
    }

    @Override
    public Mono<Void> handleBatchMessage(BatchMessage batchMessage) {
        return Mono.defer(() -> {
            try {
                Method handler = handlers.get(batchMessage.getType());
                if (handler != null) {
                    handler.setAccessible(true);
                    handler.invoke(bean, batchMessage);
                    logger.debug("Successfully handled batch message of type: {}", batchMessage.getType());
                    return Mono.empty();
                } else {
                    logger.warn("No handler found for batch message type: {}", batchMessage.getType());
                    return Mono.empty();
                }
            } catch (Exception e) {
                logger.error("Failed to handle batch message: {}", e.getMessage());
                return Mono.error(e);
            }
        });
    }
}
