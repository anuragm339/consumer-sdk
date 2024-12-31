package com.example.messaging.consumer.core;

/**
 * Configuration for message consumer
 */
public class ConsumerConfig {
    private final String serverHost;
    private final int serverPort;
    private final String consumerId;
    private final String groupId;
    private final int maxRetryAttempts;
    private final long retryDelayMillis;
    private final int maxConcurrentMessages;

    private ConsumerConfig(Builder builder) {
        this.serverHost = builder.serverHost;
        this.serverPort = builder.serverPort;
        this.consumerId = builder.consumerId;
        this.groupId = builder.groupId;
        this.maxRetryAttempts = builder.maxRetryAttempts;
        this.retryDelayMillis = builder.retryDelayMillis;
        this.maxConcurrentMessages = builder.maxConcurrentMessages;
    }

    // Getters
    public String getServerHost() { return serverHost; }
    public int getServerPort() { return serverPort; }
    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }
    public int getMaxRetryAttempts() { return maxRetryAttempts; }
    public long getRetryDelayMillis() { return retryDelayMillis; }
    public int getMaxConcurrentMessages() { return maxConcurrentMessages; }

    /**
     * Builder for ConsumerConfig
     */
    public static class Builder {
        private String serverHost = "localhost";
        private int serverPort = 7000;
        private String consumerId;
        private String groupId;
        private int maxRetryAttempts = 3;
        private long retryDelayMillis = 1000;
        private int maxConcurrentMessages = 100;

        public Builder serverHost(String serverHost) {
            this.serverHost = serverHost;
            return this;
        }

        public Builder serverPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public Builder consumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder maxRetryAttempts(int maxRetryAttempts) {
            this.maxRetryAttempts = maxRetryAttempts;
            return this;
        }

        public Builder retryDelayMillis(long retryDelayMillis) {
            this.retryDelayMillis = retryDelayMillis;
            return this;
        }

        public Builder maxConcurrentMessages(int maxConcurrentMessages) {
            this.maxConcurrentMessages = maxConcurrentMessages;
            return this;
        }

        public ConsumerConfig build() {
            validate();
            return new ConsumerConfig(this);
        }

        private void validate() {
            if (consumerId == null || consumerId.trim().isEmpty()) {
                throw new IllegalStateException("ConsumerId must be set");
            }
            if (groupId == null || groupId.trim().isEmpty()) {
                throw new IllegalStateException("GroupId must be set");
            }
            if (serverPort <= 0) {
                throw new IllegalStateException("Invalid server port");
            }
            if (maxRetryAttempts < 0) {
                throw new IllegalStateException("Max retry attempts cannot be negative");
            }
            if (retryDelayMillis < 0) {
                throw new IllegalStateException("Retry delay cannot be negative");
            }
            if (maxConcurrentMessages <= 0) {
                throw new IllegalStateException("Max concurrent messages must be greater than 0");
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
