package it.polimi.ds.chat.protocol.raft;

import it.polimi.ds.chat.common.clock.VectorClock;

import java.io.Serializable;

/**
 * Application command stored in the Raft log for chat delivery.
 *
 * This is the payload that the Raft layer replicates. A committed
 * {@link ChatCommand} maps into a {@link it.polimi.ds.chat.protocol.chat.ChatDeliverMessage}
 * using:
 * - log index as the global sequence number
 * - brokerId/username/text/vectorClock as the message content
 *
 * localMsgId is preserved for tracing. Client-originated commands also carry
 * the stable client identity so the leader can de-duplicate retries by
 * (clientId, clientSeq).
 */
public class ChatCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String localMsgId;
    private final int brokerId;
    private final String username;
    private final String clientId;
    private final long clientSeq;
    private final String text;
    private final VectorClock vectorClock;
    private final boolean hasClientIdentity;

    public ChatCommand(String localMsgId, int brokerId, String username, String text, VectorClock vectorClock) {
        this(localMsgId, brokerId, username, null, 0L, text, vectorClock, false);
    }

    public ChatCommand(String localMsgId, int brokerId, String username, String text,
                       VectorClock vectorClock, long clientSeq) {
        this(localMsgId, brokerId, username, username, clientSeq, text, vectorClock, true);
    }

    public ChatCommand(String localMsgId, int brokerId, String username, String text,
                       VectorClock vectorClock, String clientId, long clientSeq) {
        this(localMsgId, brokerId, username, clientId, clientSeq, text, vectorClock, true);
    }

    private ChatCommand(String localMsgId, int brokerId, String username, String clientId,
                        long clientSeq, String text, VectorClock vectorClock, boolean hasClientIdentity) {
        this.localMsgId = localMsgId;
        this.brokerId = brokerId;
        this.username = username;
        this.clientId = clientId;
        this.clientSeq = clientSeq;
        this.text = text;
        this.vectorClock = vectorClock;
        this.hasClientIdentity = hasClientIdentity;
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

    public String getClientId() {
        return clientId;
    }

    public long getClientSeq() {
        return clientSeq;
    }

    public String getText() {
        return text;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public long getClientTimestamp() {
        return clientSeq;
    }

    public boolean hasClientTimestamp() {
        return hasClientIdentity;
    }

    public boolean hasClientIdentity() {
        return hasClientIdentity;
    }
}
