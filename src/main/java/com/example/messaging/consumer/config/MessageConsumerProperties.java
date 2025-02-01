package com.example.messaging.consumer.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Context;
import jakarta.inject.Singleton;

@Singleton
@ConfigurationProperties("messaging.consumer")
@Context
public class MessageConsumerProperties {

    private String host = "localhost";
    private int port = 7000;
    private String defaultGroupId;
    private RetryConfig retry = new RetryConfig();

    @ConfigurationProperties("retry")
    public static class RetryConfig {
        private int maxAttempts = 3;
        private long delayMs = 1000;

        // Getters and Setters
        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getDelayMs() {
            return delayMs;
        }

        public void setDelayMs(long delayMs) {
            this.delayMs = delayMs;
        }
    }

    // Getters and Setters
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDefaultGroupId() {
        return defaultGroupId;
    }

    public void setDefaultGroupId(String defaultGroupId) {
        this.defaultGroupId = defaultGroupId;
    }

    public RetryConfig getRetry() {
        return retry;
    }

    public void setRetry(RetryConfig retry) {
        this.retry = retry;
    }
}
