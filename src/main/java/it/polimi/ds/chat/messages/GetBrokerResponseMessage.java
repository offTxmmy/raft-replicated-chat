package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class GetBrokerResponseMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean available;
    private final String brokerHost;
    private final int brokerPort;
    private final int brokerId;

    public GetBrokerResponseMessage(boolean available, String brokerHost, int brokerPort, int brokerId) {
        this.available = available;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.brokerId = brokerId;
    }

    public boolean isAvailable() {
        return available;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public int getBrokerId() {
        return brokerId;
    }

    @Override
    public String toString() {
        return "GetBrokerResponseMessage{" +
                "available=" + available +
                ", brokerHost='" + brokerHost + '\'' +
                ", brokerPort=" + brokerPort +
                ", brokerId=" + brokerId +
                '}';
    }
}
