package it.polimi.ds.chat.client.messaging;

/**
 * Represents a message sent by the client that is pending acknowledgment (ACK) from the server.
 * Stores the client sequence, the wire format line, and the last time it was sent.
 */
public class ClientPendingMessage {

    private final long clientSeq;
    private final String wireLine;   // stringa così come viene mandata su socket
    private volatile long lastSendTime;

    /**
     * Constructs a ClientPendingMessage with the given client sequence and wire line.
     *
     * @param clientSeq the sequence of the message for this client process
     * @param wireLine the message in wire format as sent on the socket
     */
    public ClientPendingMessage(long clientSeq, String wireLine) {
        this.clientSeq = clientSeq;
        this.wireLine = wireLine;
        this.lastSendTime = System.currentTimeMillis();
    }

    /**
     * Returns the client sequence of the message.
     *
     * @return the message client sequence
     */
    public long getClientSeq() {
        return clientSeq;
    }

    /**
     * Legacy name kept for older call sites.
     */
    public long getTimestamp() {
        return clientSeq;
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
