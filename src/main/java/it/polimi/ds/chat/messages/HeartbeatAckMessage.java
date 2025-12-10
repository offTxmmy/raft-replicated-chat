package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class HeartbeatAckMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long timestamp;
    private final int brokerId;

    public HeartbeatAckMessage(long timestamp, int brokerId) {
        this.timestamp = timestamp;
        this.brokerId = brokerId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getBrokerId() {
        return brokerId;
    }
}

