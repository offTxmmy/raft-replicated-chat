package it.polimi.ds.chat.broker.config;

import it.polimi.ds.chat.broker.session.HandlerState;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;

/**
 * Configuration class for a broker instance.
 * Holds network parameters, role information, and handler state.
 */
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

    //aggiungo qui tomma
    private final OrderingMode orderingMode;
    private final RaftConfig raftConfig;

    /**
     * Constructs a BrokerConfig with all required parameters.
     *
     * @param brokerId      the broker's identifier (0 for leader, -1 for followers at startup)
     * @param isSequencer   true if this broker is the sequencer (leader)
     * @param brokerHost    hostname or IP address of the broker
     * @param brokerPort    TCP port for broker communication
     * @param clientPort    TCP port for client connections
     * @param sequencerHost hostname or IP address of the sequencer
     * @param sequencerPort TCP port of the sequencer
     * @param udpPort       UDP port for broker communication
     * @param handlerState  handler state (non-null only for sequencer)
     */

    public BrokerConfig(int brokerId,
                        boolean isSequencer,
                        String brokerHost,
                        int brokerPort,
                        int clientPort,
                        String sequencerHost,
                        int sequencerPort,
                        int udpPort,
                        HandlerState handlerState) {
        this(brokerId, isSequencer, brokerHost, brokerPort, clientPort,
                sequencerHost, sequencerPort, udpPort, handlerState,
                OrderingMode.SEQUENCER, null);
    }
    //aggiungo pure nel costruttore
    public BrokerConfig(int brokerId,
                        boolean isSequencer,
                        String brokerHost,
                        int brokerPort,
                        int clientPort,
                        String sequencerHost,
                        int sequencerPort,
                        int udpPort,
                        HandlerState handlerState,
                        OrderingMode orderingMode,
                        RaftConfig raftConfig) {

        if (orderingMode == null) {
            throw new IllegalArgumentException("orderingMode must not be null");
        }
        if (orderingMode == OrderingMode.RAFT && raftConfig == null) {
            throw new IllegalArgumentException("raftConfig is required when orderingMode == RAFT");
        }
        if (orderingMode == OrderingMode.SEQUENCER && raftConfig != null) {
            throw new IllegalArgumentException("raftConfig must be null when orderingMode == SEQUENCER");
        }
        this.brokerId = brokerId;
        this.isSequencer = isSequencer;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.clientPort = clientPort;
        this.sequencerHost = sequencerHost;
        this.sequencerPort = sequencerPort;
        this.udpPort = udpPort;
        this.handlerState = handlerState;
        this.orderingMode = orderingMode;
        this.raftConfig = raftConfig;
    }

    /**
     * Gets the broker's identifier.
     *
     * @return broker id
     */
    public int getBrokerId() {
        return brokerId;
    }

    /**
     * Checks if this broker is the sequencer (leader).
     *
     * @return true if sequencer, false otherwise
     */
    public boolean isSequencer() {
        return isSequencer;
    }

    /**
     * Gets the broker's hostname or IP address.
     *
     * @return broker host
     */
    public String getBrokerHost() {
        return brokerHost;
    }

    /**
     * Gets the broker's TCP port.
     *
     * @return broker port
     */
    public int getBrokerPort() {
        return brokerPort;
    }

    /**
     * Gets the TCP port for client connections.
     *
     * @return client port
     */
    public int getClientPort() {
        return clientPort;
    }

    /**
     * Gets the sequencer's hostname or IP address.
     *
     * @return sequencer host
     */
    public String getSequencerHost() {
        return sequencerHost;
    }

    /**
     * Gets the sequencer's TCP port.
     *
     * @return sequencer port
     */
    public int getSequencerPort() {
        return sequencerPort;
    }

    /**
     * Gets the UDP port for broker communication.
     *
     * @return UDP port
     */
    public int getUdpPort() {
        return udpPort;
    }

    /**
     * Gets the handler state (non-null only for sequencer).
     *
     * @return handler state
     */
    public HandlerState getHandlerState() {
        return handlerState;
    }

    /**
     * Ordering mode selected at startup. Never null.
     */
    public OrderingMode getOrderingMode() {
        return orderingMode;
    }

    /**
     * Raft configuration block. Non-null iff {@link #getOrderingMode()} == {@link OrderingMode#RAFT}.
     */
    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    /**
     * Checks equality based on broker host, port, and sequencer role.
     *
     * @param o the object to compare
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerConfig that)) return false;

        // Identify a broker only by host+port+role in the cluster context
        if (brokerPort != that.brokerPort) return false;
        if (isSequencer != that.isSequencer) return false;
        return brokerHost.equals(that.brokerHost);
    }

    /**
     * Computes hash code based on broker host, port, and sequencer role.
     *
     * @return hash code
     */
    @Override
    public int hashCode() {
        int result = brokerHost.hashCode();
        result = 31 * result + brokerPort;
        result = 31 * result + (isSequencer ? 1 : 0);
        return result;
    }
}
