package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class ClientCountUpdateMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int brokerId;
    private final int clientCount;

    public ClientCountUpdateMessage(int brokerId, int clientCount) {
        this.brokerId = brokerId;
        this.clientCount = clientCount;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getClientCount() {
        return clientCount;
    }

    @Override
    public String toString() {
        return "ClientCountUpdateMessage{" +
                "brokerId=" + brokerId +
                ", clientCount=" + clientCount +
                '}';
    }
}
