package com.example.messaging.consumer.rsocket.impl;

import com.example.messaging.consumer.api.MessageConsumer;
import com.example.messaging.consumer.connection.ConnectionManager;
import com.example.messaging.consumer.core.ConsumerConfig;
import com.example.messaging.consumer.core.DefaultMessageConsumer;
import com.example.messaging.consumer.handler.DefaultMessageHandler;
import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.consumer.model.ConsumerSetupPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class ConsumerRSocketFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRSocketFactory.class);

    private final MessageHandler messageHandler;
    private final ConsumerConfig config;
    private final ObjectMapper objectMapper;

    public ConsumerRSocketFactory(MessageHandler messageHandler, ConsumerConfig config, ObjectMapper objectMapper) {
        this.messageHandler = messageHandler;
        this.config = config;
        this.objectMapper=objectMapper;
    }
    public MessageConsumer createConsumer(ConsumerConfig config) {
        MessageHandler messageHandler = new DefaultMessageHandler(config.getConsumerId());
        ConnectionManager connectionManager = new ConnectionManager(config, messageHandler);
        // Create and return consumer instance
        return new DefaultMessageConsumer(connectionManager, config.getConsumerId());
    }

    public Mono<RSocket> createRSocket() throws JsonProcessingException {
        return RSocketConnector.create()
                .setupPayload(createSetupPayload())
                .acceptor(SocketAcceptor.with(createResponderRSocket()))
                .connect(TcpClientTransport.create(
                        config.getServerHost(),
                        config.getServerPort()
                ));
    }

    private Payload createSetupPayload() throws JsonProcessingException {
        ConsumerSetupPayload setupPayload = new ConsumerSetupPayload(
                config.getConsumerId(),
                config.getGroupId()
        );

        String setupData = objectMapper.writeValueAsString(setupPayload);
        logger.info("Creating setup payload: {}", setupData);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("consumerId", config.getConsumerId());
        metadata.put("groupId", config.getGroupId());
        metadata.put("version", "1.0");

        String metadataStr = objectMapper.writeValueAsString(metadata);

        return DefaultPayload.create(setupData, metadataStr);
    }

    private RSocket createResponderRSocket() {
        return new ConsumerRSocketImpl(messageHandler, config.getConsumerId());
    }
}
