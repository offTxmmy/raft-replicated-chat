package it.polimi.ds.chat.broker;

public class SequencerState {
    private long globalSeq = 0;
    private final Broker broker;

    public SequencerState(Broker broker) {
        this.broker = broker;
    }

    public synchronized void handleChatFromBroker(String senderBrokerId, String username, String text) {
        long seq = ++globalSeq;

        // for now directly deliver to this broker's clients
        // later will broadcast CHAT_DELIVER over UDP to all brokers
        broker.onChatDeliver(seq, username, text);
    }
}
