package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.broker.BrokerMessage;

/**
 * Application-level chat message emitted after the corresponding Raft
 * command has been committed and applied.
 *
 * <p>
 * The sequence number represents the dense client-visible order of chat
 * messages. It is derived deterministically from Raft application order and
 * is intentionally independent from the Raft log index.
 */
public class ChatDeliverMessage extends BrokerMessage {
    private final long seq;
    private final int brokerId;
    private final String username;
    private final String clientId;
    private final String text;
    private final VectorClock vectorClock;

    public ChatDeliverMessage(long seq, int brokerId, String username, String text, VectorClock vectorClock) {
        this(seq, brokerId, username, null, text, vectorClock);
    }

    public ChatDeliverMessage(long seq, int brokerId, String username, String clientId, String text,
            VectorClock vectorClock) {
        this.seq = seq;
        this.brokerId = brokerId;
        this.username = username;
        this.clientId = clientId;
        this.text = text;
        this.vectorClock = vectorClock;
    }

    public long getSeq() {
        return seq;
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

    public String getText() {
        return text;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }
}
