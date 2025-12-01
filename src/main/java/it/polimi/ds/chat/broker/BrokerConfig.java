package it.polimi.ds.chat.broker;

public class BrokerConfig {

    private final int brokerId;          // 0 for leader, -1 for followers at startup
    private final boolean isSequencer;

    private final String brokerHost;
    private final int brokerPort;

    private final int clientPort;
    private final String sequencerHost;
    private final int sequencerPort;
    private final int udpPort;

    // Only non-null on the sequencer (leader); null for followers
    private final HandlerState handlerState;

    public BrokerConfig(int brokerId,
                        boolean isSequencer,
                        String brokerHost,
                        int brokerPort,
                        int clientPort,
                        String sequencerHost,
                        int sequencerPort,
                        int udpPort,
                        HandlerState handlerState) {
        this.brokerId = brokerId;
        this.isSequencer = isSequencer;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.clientPort = clientPort;
        this.sequencerHost = sequencerHost;
        this.sequencerPort = sequencerPort;
        this.udpPort = udpPort;
        this.handlerState = handlerState;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public boolean isSequencer() {
        return isSequencer;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
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

    public int getUdpPort() {
        return udpPort;
    }

    public HandlerState getHandlerState() {
        return handlerState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerConfig that)) return false;

        // Identify a broker only by host+port+role in the cluster context
        if (brokerPort != that.brokerPort) return false;
        if (isSequencer != that.isSequencer) return false;
        return brokerHost.equals(that.brokerHost);
    }

    @Override
    public int hashCode() {
        int result = brokerHost.hashCode();
        result = 31 * result + brokerPort;
        result = 31 * result + (isSequencer ? 1 : 0);
        return result;
    }
}