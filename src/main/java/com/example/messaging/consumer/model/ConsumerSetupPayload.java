package com.example.messaging.consumer.model;

public class ConsumerSetupPayload {
    private final String consumerId;
    private final String groupId;
    private final String protocolVersion;

    public ConsumerSetupPayload(String consumerId, String groupId) {
        this.consumerId = consumerId;
        this.groupId = groupId;
        this.protocolVersion = "1.0"; // protocol version
    }

    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }
    public String getProtocolVersion() { return protocolVersion; }
}
