package com.example.messaging.consumer.connection;

import com.example.messaging.consumer.core.ConsumerConfig;
import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.consumer.rsocket.impl.ConsumerRSocketFactory;
import com.example.messaging.consumer.rsocket.impl.ConsumerRSocketImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private final ConsumerConfig config;
    private final MessageHandler messageHandler;
    private volatile RSocket rSocket;
    private final Duration RETRY_DELAY = Duration.ofSeconds(1);
    private final Long MAX_RETRIES = Long.MAX_VALUE;

    public ConnectionManager(ConsumerConfig config, MessageHandler messageHandler) {
        this.config = config;
        this.messageHandler = messageHandler;
    }

    public Mono<RSocket> getConnection() {
        if (rSocket != null && !rSocket.isDisposed()) {
            return Mono.just(rSocket);
        }

        return createConnection()
                .retryWhen(Retry.backoff(MAX_RETRIES, RETRY_DELAY)
                        .doBeforeRetry(retrySignal ->
                                logger.info("Attempting to reconnect. Attempt {}/{}",
                                        retrySignal.totalRetries() + 1, MAX_RETRIES))
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            logger.error("Failed to establish connection after {} attempts", MAX_RETRIES);
                            return new RuntimeException("Failed to establish connection after max retries");
                        }));
    }

    private Mono<RSocket> createConnection() {
        return Mono.defer(() -> {
            ConsumerRSocketFactory rSocketFactory = new ConsumerRSocketFactory(messageHandler, config, new ObjectMapper());

            try {
                return rSocketFactory.createRSocket()
                        .doOnNext(socket -> {
                            this.rSocket = socket;
                            logger.info("Created new RSocket connection for consumer: {}", config.getConsumerId());

                            // Setup connection lost handler
                            socket.onClose()
                                    .doFinally(signalType -> {
                                        logger.debug("Connection closed. Signal: {}", signalType);
                                        this.rSocket = null;
                                    })
                                    .subscribe();
                        })
                        .doOnError(error ->
                                logger.error("Failed to create RSocket: {}", error.getMessage())
                        );
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
    public Mono<Void> disconnect() {
        return Mono.defer(() -> {
            if (rSocket != null) {
                rSocket.dispose();
                rSocket = null;
                logger.info("Disconnected consumer: {}", config.getConsumerId());
            }
            return Mono.empty();
        });
    }

    public boolean isConnected() {
        return rSocket != null && !rSocket.isDisposed();
    }
}
