package it.polimi.ds.chat.messages.raft;

import it.polimi.ds.chat.utilities.VectorClock;

import java.io.Serializable;

/**
 * Application command stored in the Raft log for chat delivery.
 *
 * This is the payload that the Raft layer replicates. Person C can map a
 * committed {@link ChatCommand} into a {@link it.polimi.ds.chat.messages.ChatDeliverMessage}
 * by using:
 * - log index as the global sequence number
 * - brokerId/username/text/vectorClock as the message content
 *
 * localMsgId is preserved for tracing. Client-originated commands also carry
 * the original MSG timestamp so the leader can de-duplicate retries by
 * (username, timestamp).
 */
public class ChatCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String localMsgId;
    private final int brokerId;
    private final String username;
    private final String text;
    private final VectorClock vectorClock;
    private final long clientTimestamp;
    private final boolean hasClientTimestamp;

    public ChatCommand(String localMsgId, int brokerId, String username, String text, VectorClock vectorClock) {
        this(localMsgId, brokerId, username, text, vectorClock, 0L, false);
    }

    public ChatCommand(String localMsgId, int brokerId, String username, String text,
                       VectorClock vectorClock, long clientTimestamp) {
        this(localMsgId, brokerId, username, text, vectorClock, clientTimestamp, true);
    }

    private ChatCommand(String localMsgId, int brokerId, String username, String text,
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
