package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.broker.BrokerMessage;

public class ChatReqMessage extends BrokerMessage {
    private final String localMsgId;
    private final int brokerId;
    private final String username;
    private final String text;
    private final VectorClock vectorClock;

    // Stable client retry key: retries resend the same MSG timestamp even
    // though this broker may generate a fresh localMsgId for each attempt.
    private final long clientTimestamp;
    private final boolean hasClientTimestamp;

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text, VectorClock vectorClock) {
        this(localMsgId, brokerId, username, text, vectorClock, 0L, false);
    }

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text,
                          VectorClock vectorClock, long clientTimestamp) {
        this(localMsgId, brokerId, username, text, vectorClock, clientTimestamp, true);
    }

    private ChatReqMessage(String localMsgId, int brokerId, String username, String text,
                           VectorClock vectorClock, long clientTimestamp, boolean hasClientTimestamp) {
        this.localMsgId = localMsgId;
        this.brokerId = brokerId;
        this.username = username;
        this.text = text;
        this.vectorClock = vectorClock;
        this.clientTimestamp = clientTimestamp;
        this.hasClientTimestamp = hasClientTimestamp;
    }

    public String getLocalMsgId() {
        return localMsgId;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getUsername() {
        return username;
    }

    public String getText() {
        return text;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public long getClientTimestamp() {
        return clientTimestamp;
    }

    public boolean hasClientTimestamp() {
        return hasClientTimestamp;
    }
}
