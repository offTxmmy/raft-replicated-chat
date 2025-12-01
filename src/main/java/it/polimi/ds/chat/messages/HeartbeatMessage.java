package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class HeartbeatMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int brokerId;
    private final long timestamp;

    public HeartbeatMessage(int brokerId, long timestamp) {
        this.brokerId = brokerId;
        this.timestamp = timestamp;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "HeartbeatMessage{" +
                "brokerId=" + brokerId +
                ", timestamp=" + timestamp +
                '}';
    }
}
