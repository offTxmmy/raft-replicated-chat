package it.polimi.ds.chat.messages;

public class RetransmissionRequestMessage extends BrokerMessage{
    private final long missingSeq;

    public RetransmissionRequestMessage(long missingSeq) {
        this.missingSeq = missingSeq;
    }

    public long getMissingSeq() {
        return missingSeq;
    }
}
