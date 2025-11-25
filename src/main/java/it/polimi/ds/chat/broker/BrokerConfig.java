package it.polimi.ds.chat.broker;

public class BrokerConfig {
    private final String brokerId;
    private boolean sequencer;
    private final int clientPort;
    private final String sequencerHost;
    private final int sequencerPort;
    private final int udpBroadcastPort;

    public BrokerConfig(String brokerId, boolean sequencer, int clientPort, String sequencerHost, int sequencerPort, int udpBroadcastPort) {
        this.brokerId = brokerId;
        this.sequencer = sequencer;
        this.clientPort = clientPort;
        this.sequencerHost = sequencerHost;
        this.sequencerPort = sequencerPort;
        this.udpBroadcastPort = udpBroadcastPort;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public boolean isSequencer() {
        return sequencer;
    }

    public int getClientPort() {
        return clientPort;
    }

    public String getSequencerHost() {
        return sequencerHost;
    }

    public int getSequencerPort() {
        return sequencerPort;
    }

    public int getUdpBroadcastPort() {
        return udpBroadcastPort;
    }



}