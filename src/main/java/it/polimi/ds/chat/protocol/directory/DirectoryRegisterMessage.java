package it.polimi.ds.chat.protocol.directory;

import java.io.Serializable;

public class DirectoryRegisterMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;

    public DirectoryRegisterMessage(int brokekId, String brokerHost, int brokerPort) {
        this.brokerId = brokekId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    @Override
    public String toString() {
        return "DirectoryRegisterMessage{" +
                "brokerHost='" + brokerHost + '\'' +
                ", brokerPort=" + brokerPort +
                '}';
    }
}
