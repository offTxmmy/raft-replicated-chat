package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.broker.BrokerMessage;

public class ChatReqMessage extends BrokerMessage {
    private final String localMsgId;
    private final int brokerId;
    private final String username;
    private final String clientId;
    private final long clientSeq;
    private final String text;
    private final VectorClock vectorClock;
    private final boolean hasClientIdentity;
    private final boolean deliveryBarrier;

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text, VectorClock vectorClock) {
        this(localMsgId, brokerId, username, null, 0L, text, vectorClock, false, false);
    }

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text,
                          VectorClock vectorClock, long clientSeq) {
        this(localMsgId, brokerId, username, username, clientSeq, text, vectorClock, true, false);
    }

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text,
                          VectorClock vectorClock, String clientId, long clientSeq) {
        this(localMsgId, brokerId, username, clientId, clientSeq, text, vectorClock, true, false);
    }

    private ChatReqMessage(String localMsgId, int brokerId, String username, String clientId,
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
     * Creates an internal command that linearizes a client JOIN in the committed
     * Raft order without producing a client-visible chat message.
     */
    public static ChatReqMessage deliveryBarrier(String barrierId, int brokerId) {
        return new ChatReqMessage(
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

    public boolean hasClientIdentity() {
        return hasClientIdentity;
    }

    public boolean isDeliveryBarrier() {
        return deliveryBarrier;
    }
}
