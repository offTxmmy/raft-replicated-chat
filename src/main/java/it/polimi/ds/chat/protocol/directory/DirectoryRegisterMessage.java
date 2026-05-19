package it.polimi.ds.chat.protocol.directory;

import java.io.Serializable;

public class DirectoryRegisterMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final boolean sequencer;

    public DirectoryRegisterMessage(int brokekId, String brokerHost, int brokerPort, boolean sequencer) {
        this.brokerId = brokekId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.sequencer = sequencer;
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

    public boolean isSequencer() {
        return sequencer;
    }

    @Override
    public String toString() {
        return "DirectoryRegisterMessage{" +
                "brokerHost='" + brokerHost + '\'' +
                ", brokerPort=" + brokerPort +
                ", sequencer=" + sequencer +
                '}';
    }
}
