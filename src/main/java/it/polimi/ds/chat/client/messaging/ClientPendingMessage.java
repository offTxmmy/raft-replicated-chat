package it.polimi.ds.chat.client.messaging;

/**
 * Represents a message sent by the client that is pending acknowledgment (ACK) from the server.
 * Stores the message timestamp, the wire format line, and the last time it was sent.
 */
public class ClientPendingMessage {

    private final long timestamp;
    private final String wireLine;   // stringa così come viene mandata su socket
    private volatile long lastSendTime;

    /**
     * Constructs a ClientPendingMessage with the given timestamp and wire line.
     *
     * @param timestamp the timestamp of the message
     * @param wireLine the message in wire format as sent on the socket
     */
    public ClientPendingMessage(long timestamp, String wireLine) {
        this.timestamp = timestamp;
        this.wireLine = wireLine;
        this.lastSendTime = System.currentTimeMillis();
    }

    /**
     * Returns the timestamp of the message.
     *
     * @return the message timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the wire format line of the message.
     *
     * @return the wire line as sent on the socket
     */
    public String getWireLine() {
        return wireLine;
    }

    /**
     * Returns the last time this message was sent.
     *
     * @return the last send time in milliseconds
     */
    public long getLastSendTime() {
        return lastSendTime;
    }

    /**
     * Updates the last send time to the current system time.
     */
    public void updateLastSendTime() {
        this.lastSendTime = System.currentTimeMillis();
    }
}
