package it.polimi.ds.chat.messages;

/**
 * Message sent by the sequencer to all brokers to deliver an ordered chat
 * message.
 */
public class ChatDeliverMessage extends BrokerMessage {
    private final long seq;
    private final int brokerId;
    private final String username;
    private final String text;

    public ChatDeliverMessage(long seq, int brokerId, String username, String text) {
        this.seq = seq;
        this.brokerId = brokerId;
        this.username = username;
        this.text = text;
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
}