package it.polimi.ds.chat.messages;

import it.polimi.ds.chat.utilities.VectorClock;

public class ChatReqMessage extends BrokerMessage {
    private final String localMsgId;
    private final int brokerId;
    private final String username;
    private final String text;
    private final VectorClock vectorClock;

    public ChatReqMessage(String localMsgId, int brokerId, String username, String text, VectorClock vectorClock) {
        this.localMsgId = localMsgId;
        this.brokerId = brokerId;
        this.username = username;
        this.text = text;
        this.vectorClock = vectorClock;
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

    public VectorClock getVectorClock() {
        return vectorClock;
    }
}