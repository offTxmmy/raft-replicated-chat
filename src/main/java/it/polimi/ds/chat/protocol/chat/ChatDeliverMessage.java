package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.broker.BrokerMessage;

/**
 * Message sent by the sequencer to all brokers to deliver an ordered chat
 * message.
 */
public class ChatDeliverMessage extends BrokerMessage {
    private final long seq;
    private final int brokerId;
    private final String username;
    private final String text;
    private final VectorClock vectorClock;

    public ChatDeliverMessage(long seq, int brokerId, String username, String text, VectorClock vectorClock) {
        this.seq = seq;
        this.brokerId = brokerId;
        this.username = username;
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

    public String getText() {
        return text;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }
}
