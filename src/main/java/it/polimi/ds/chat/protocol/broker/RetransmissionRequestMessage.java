package it.polimi.ds.chat.protocol.broker;

public class RetransmissionRequestMessage extends BrokerMessage{
    private final long missingSeq;

    public RetransmissionRequestMessage(long missingSeq) {
        this.missingSeq = missingSeq;
    }

    public long getMissingSeq() {
        return missingSeq;
    }
}
