package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.protocol.broker.BrokerMessage;

/** A committed chat message, ordered by its Raft log index. */
public class ChatDeliverMessage extends BrokerMessage {
    private final long seq;
    private final String username;
    private final String clientId;
    private final long clientSeq;
    private final String text;

    public ChatDeliverMessage(long seq, String username, String clientId, long clientSeq, String text) {
        this.seq = seq;
        this.username = username;
        this.clientId = clientId;
        this.clientSeq = clientSeq;
        this.text = text;
    }

    public long getSeq() {
        return seq;
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
}
