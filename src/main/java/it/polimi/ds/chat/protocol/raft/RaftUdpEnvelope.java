package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;
import java.util.Objects;

/**
 * Transport envelope for Raft RPCs carried over UDP broadcast/unicast.
 *
 * <p>The payload is intentionally kept as {@link Object} to reuse the existing
 * serializable Raft message classes during the first migration step.
 */
public final class RaftUdpEnvelope implements Serializable {

    public static final int BROADCAST_TARGET = -1;

    private static final long serialVersionUID = 1L;

    private final String clusterId;
    private final String messageId;
    private final int senderId;
    private final int targetId;
    private final RaftUdpMessageType type;
    private final long term;
    private final Object payload;
    private final long createdAtMillis;

    public RaftUdpEnvelope(String clusterId,
                           String messageId,
                           int senderId,
                           int targetId,
                           RaftUdpMessageType type,
                           long term,
                           Object payload,
                           long createdAtMillis) {
        this.clusterId = requireNonBlank(clusterId, "clusterId");
        this.messageId = requireNonBlank(messageId, "messageId");
        this.senderId = senderId;
        this.targetId = targetId;
        this.type = Objects.requireNonNull(type, "type");
        this.term = term;
        this.payload = Objects.requireNonNull(payload, "payload");
        this.createdAtMillis = createdAtMillis;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getMessageId() {
        return messageId;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getTargetId() {
        return targetId;
    }

    public RaftUdpMessageType getType() {
        return type;
    }

    public long getTerm() {
        return term;
    }

    public Object getPayload() {
        return payload;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public boolean isBroadcast() {
        return targetId == BROADCAST_TARGET;
    }

    public boolean isAddressedTo(int nodeId) {
        return isBroadcast() || targetId == nodeId;
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    @Override
    public String toString() {
        return "RaftUdpEnvelope{" +
                "clusterId='" + clusterId + '\'' +
                ", messageId='" + messageId + '\'' +
                ", senderId=" + senderId +
                ", targetId=" + targetId +
                ", type=" + type +
                ", term=" + term +
                ", payloadType=" + payload.getClass().getSimpleName() +
                ", createdAtMillis=" + createdAtMillis +
                '}';
    }
}
