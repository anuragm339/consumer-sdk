package com.example.messaging.consumer.api;

/**
 * Represents the current status of a consumer
 */
public enum ConsumerStatus {
    DISCONNECTED("Not connected to server"),
    CONNECTING("Establishing connection"),
    CONNECTED("Connected to server"),
    CONSUMING("Actively consuming messages"),
    PAUSED("Message consumption paused"),
    ERROR("Error state");

    private final String description;

    ConsumerStatus(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
