package it.polimi.ds.chat.messages;

public class ChatReqMessage extends BrokerMessage {
    private final String localMsgId;
    private final int brokerId;
    private final String username;
    private final String text;

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text) {
        this.localMsgId = localMsgId;
        this.brokerId = brokerId;
        this.username = username;
        this.text = text;
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
}