package it.polimi.ds.chat.messages;

public class BrokerJoinAck extends BrokerMessage{
    private final int brokerId;

    public BrokerJoinAck(int brokerId) {
        this.brokerId = brokerId;
    }

    public int getBrokerId() {
        return brokerId;
    }
}
