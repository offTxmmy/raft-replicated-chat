package it.polimi.ds.chat.client;


public class ClientPendingMessage {

    private final long timestamp;
    private final String wireLine;   // stringa così come viene mandata su socket
    private volatile long lastSendTime;

    public ClientPendingMessage(long timestamp, String wireLine) {
        this.timestamp = timestamp;
        this.wireLine = wireLine;
        this.lastSendTime = System.currentTimeMillis();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getWireLine() {
        return wireLine;
    }

    public long getLastSendTime() {
        return lastSendTime;
    }

    public void updateLastSendTime() {
        this.lastSendTime = System.currentTimeMillis();
    }
}
