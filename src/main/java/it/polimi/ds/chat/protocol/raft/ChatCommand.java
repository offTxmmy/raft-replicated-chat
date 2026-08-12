package it.polimi.ds.chat.protocol.raft;

import it.polimi.ds.chat.common.clock.VectorClock;

import java.io.Serializable;

/**
 * Application command stored in the Raft log for chat delivery.
 *
 * <p>
 * This is the payload replicated by Raft. Once committed and applied,
 * a {@link ChatCommand} is mapped into a
 * {@link it.polimi.ds.chat.protocol.chat.ChatDeliverMessage}.
 *
 * <p>
 * The Raft log index is intentionally kept separate from the
 * client-visible chat sequence number. Internal Raft entries may occupy
 * log indexes without representing chat messages.
 *
 * <p>
 * {@code localMsgId} is preserved for tracing. Client-originated
 * commands also carry the stable client identity so the leader can
 * de-duplicate retries by {@code (clientId, clientSeq)}.
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
    private final boolean deliveryBarrier;

    public ChatCommand(String localMsgId, int brokerId, String username, String text, VectorClock vectorClock) {
        this(localMsgId, brokerId, username, null, 0L, text, vectorClock, false, false);
    }

    public ChatCommand(String localMsgId, int brokerId, String username, String text,
            VectorClock vectorClock, long clientSeq) {
        this(localMsgId, brokerId, username, username, clientSeq, text, vectorClock, true, false);
    }

    public ChatCommand(String localMsgId, int brokerId, String username, String text,
            VectorClock vectorClock, String clientId, long clientSeq) {
        this(localMsgId, brokerId, username, clientId, clientSeq, text, vectorClock, true, false);
    }

    private ChatCommand(String localMsgId, int brokerId, String username, String clientId,
            long clientSeq, String text, VectorClock vectorClock,
            boolean hasClientIdentity, boolean deliveryBarrier) {
        this.localMsgId = localMsgId;
        this.brokerId = brokerId;
        this.username = username;
        this.clientId = clientId;
        this.clientSeq = clientSeq;
        this.text = text;
        this.vectorClock = vectorClock;
        this.hasClientIdentity = hasClientIdentity;
        this.deliveryBarrier = deliveryBarrier;
    }

    /**
     * Creates an internal committed-order fence used to activate a newly joined
     * client only after all preceding commands have been applied locally.
     */
    public static ChatCommand deliveryBarrier(String barrierId, int brokerId) {
        return new ChatCommand(
                barrierId,
                brokerId,
                "",
                null,
                0L,
                "",
                new VectorClock(),
                false,
                true
        );
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

    public boolean isDeliveryBarrier() {
        return deliveryBarrier;
    }
}
