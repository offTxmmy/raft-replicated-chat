package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class DirectoryRegisterMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String brokerHost;
    private final int brokerPort;
    private final boolean sequencer;

    public DirectoryRegisterMessage(String brokerHost, int brokerPort, boolean sequencer) {
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.sequencer = sequencer;
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
