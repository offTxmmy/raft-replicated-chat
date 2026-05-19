package it.polimi.ds.chat.protocol.broker;

public class BrokerJoinAck extends BrokerMessage{
    private final int brokerId;
    private final long currentSequenceNumber;

    public BrokerJoinAck(int brokerId, long currentSequenceNumber) {
        this.brokerId = brokerId;
        this.currentSequenceNumber = currentSequenceNumber;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public long getCurrentSequenceNumber() {
        return currentSequenceNumber;
    }
}
