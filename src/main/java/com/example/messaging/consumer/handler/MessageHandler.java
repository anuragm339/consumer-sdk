package com.example.messaging.consumer.handler;

import com.example.messaging.models.Message;
import com.example.messaging.models.BatchMessage;
import reactor.core.publisher.Mono;

public interface MessageHandler {

    /**
     * Handle a single message
     * @param message The message to be handled
     * @return Mono<Void> completion signal
     */
    Mono<Void> handleMessage(Message message);

    /**
     * Handle batch of messages
     * @param batchMessage The batch message to be handled
     * @return Mono<Void> completion signal
     */
    Mono<Void> handleBatchMessage(BatchMessage batchMessage);
}
