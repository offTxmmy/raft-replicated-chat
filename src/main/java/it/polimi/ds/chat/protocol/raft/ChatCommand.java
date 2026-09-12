package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;

/** Application command replicated by Raft for one stable client operation. */
public class ChatCommand implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String username;
    private final String clientId;
    private final long clientSeq;
    private final String text;

    public ChatCommand(String username, String clientId, long clientSeq, String text) {
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
