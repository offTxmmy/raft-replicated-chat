package it.polimi.ds.chat.utilities;

public class ChatReqFields {
    public final String localMsgId;
    public final String brokerId;
    public final String username;
    public final String text;

    public ChatReqFields(String localMsgId, String brokerId,
                         String username, String text) {
        this.localMsgId = localMsgId;
        this.brokerId = brokerId;
        this.username = username;
        this.text = text;
    }
}
