package com.example.messaging.consumer.handler;

import com.example.messaging.models.Message;
import io.rsocket.Payload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MessageHandler {
    Mono<Void> handleMessage(Message message);

    Mono<Payload> handleFluxMessage(Message message);
}
