package it.polimi.ds.chat.protocol.chat;

import it.polimi.ds.chat.protocol.broker.BrokerMessage;

/** A client chat proposal identified by its stable FIFO identity. */
public class ChatReqMessage extends BrokerMessage {
    private final String username;
    private final String clientId;
    private final long clientSeq;
    private final String text;

    public ChatReqMessage(String username, String clientId, long clientSeq, String text) {
        this.username = username;
        this.clientId = clientId;
        this.clientSeq = clientSeq;
        this.text = text;
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
