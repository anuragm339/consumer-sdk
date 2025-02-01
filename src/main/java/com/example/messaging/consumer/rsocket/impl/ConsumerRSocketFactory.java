package com.example.messaging.consumer.rsocket.impl;

import com.example.messaging.consumer.config.MessageConsumerProperties;
import com.example.messaging.consumer.handler.MessageHandler;
import io.micronaut.context.annotation.Context;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.util.DefaultPayload;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
@Context // Add this annotation to ensure it's loaded in the context
public class ConsumerRSocketFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRSocketFactory.class);

    private  MessageConsumerProperties properties;
    private final ObjectMapper objectMapper;

    @Inject
    public ConsumerRSocketFactory(MessageConsumerProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
    }


    public Mono<RSocket> createRSocket(String consumerId, String groupId, MessageHandler messageHandler) {
        return RSocketConnector.create()
                .setupPayload(createSetupPayload(consumerId, groupId))
                .acceptor(SocketAcceptor.with(new ConsumerRSocketImpl(messageHandler)))
                .connect(TcpClientTransport.create(
                        properties.getHost(),
                        properties.getPort()
                ));
    }

    private Payload createSetupPayload(String consumerId, String groupId) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("consumerId", consumerId);
            metadata.put("groupId", groupId);
            metadata.put("version", "1.0");

            String metadataStr = objectMapper.writeValueAsString(metadata);
            return DefaultPayload.create("", metadataStr);
        } catch (Exception e) {
            logger.error("Failed to create setup payload", e);
            throw new RuntimeException("Failed to create setup payload", e);
        }
    }
}
