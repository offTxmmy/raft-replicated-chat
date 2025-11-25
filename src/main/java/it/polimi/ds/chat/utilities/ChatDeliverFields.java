package it.polimi.ds.chat.utilities;

public class ChatDeliverFields {
    public final long seq;
    public final String brokerId;
    public final String username;
    public final String text;

    public ChatDeliverFields(long seq, String brokerId,
                             String username, String text) {
        this.seq = seq;
        this.brokerId = brokerId;
        this.username = username;
        this.text = text;
    }
}
