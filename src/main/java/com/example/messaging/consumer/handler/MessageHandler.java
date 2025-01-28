package com.example.messaging.consumer.handler;

import com.example.messaging.models.BatchMessage;
import com.example.messaging.models.Message;
import io.rsocket.Payload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface MessageHandler {
    Mono<Void> handleMessage(Message message);

    Mono<Void> handleBatchMessage(BatchMessage batchMessage);

    Mono<Payload> handleFluxMessage(Message message);
}
