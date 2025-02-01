package com.example.messaging.consumer.connection;

import com.example.messaging.consumer.config.MessageConsumerProperties;
import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.consumer.rsocket.impl.ConsumerRSocketFactory;
import io.rsocket.RSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    private final String consumerId;
    private final String groupId;
    private final MessageHandler messageHandler;
    private final ConsumerRSocketFactory rSocketFactory;
    private final MessageConsumerProperties properties;
    private volatile RSocket rSocket;

    public ConnectionManager(
            String consumerId,
            String groupId,
            MessageHandler messageHandler,
            ConsumerRSocketFactory rSocketFactory,
            MessageConsumerProperties properties) {
        this.consumerId = consumerId;
        this.groupId = groupId;
        this.messageHandler = messageHandler;
        this.rSocketFactory = rSocketFactory;
        this.properties = properties;
    }

    public Mono<RSocket> getConnection() {
        if (rSocket != null && !rSocket.isDisposed()) {
            return Mono.just(rSocket);
        }

        return createConnection()
                .retryWhen(Retry.backoff(
                        Long.MAX_VALUE,
                        Duration.ofMillis(5)
                ).doBeforeRetry(signal ->
                        logger.info("Retrying connection, attempt: {}", signal.totalRetries() + 1)
                ));
    }

    private Mono<RSocket> createConnection() {
        return rSocketFactory.createRSocket(consumerId, groupId, messageHandler)
                .doOnNext(socket -> {
                    this.rSocket = socket;
                    logger.info("Created new RSocket connection for consumer: {}", consumerId);

                    socket.onClose()
                            .doFinally(signalType -> {
                                logger.info("Connection closed for consumer: {}", consumerId);
                                this.rSocket = null;
                            })
                            .subscribe();
                });
    }

    public boolean isConnected() {
        return rSocket != null && !rSocket.isDisposed();
    }

    public void disconnect() {
        if (rSocket != null) {
            rSocket.dispose();
            rSocket = null;
            logger.info("Disconnected consumer: {}", consumerId);
        }
    }
}
