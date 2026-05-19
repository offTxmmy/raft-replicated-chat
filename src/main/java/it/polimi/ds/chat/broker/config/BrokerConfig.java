package it.polimi.ds.chat.broker.config;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;

import java.util.Objects;

/**
 * Configuration for a Raft broker instance.
 */
public class BrokerConfig {

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final int clientPort;
    private final int udpPort;
    private final RaftConfig raftConfig;

    public BrokerConfig(int brokerId,
                        String brokerHost,
                        int brokerPort,
                        int clientPort,
                        int udpPort,
                        RaftConfig raftConfig) {
        this.brokerId = brokerId;
        this.brokerHost = Objects.requireNonNull(brokerHost, "brokerHost");
        this.brokerPort = validatePort(brokerPort, "brokerPort");
        this.clientPort = validatePort(clientPort, "clientPort");
        this.udpPort = validatePort(udpPort, "udpPort");
        this.raftConfig = Objects.requireNonNull(raftConfig, "raftConfig");
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

    public int getClientPort() {
        return clientPort;
    }

    public int getUdpPort() {
        return udpPort;
    }

    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    private static int validatePort(int port, String fieldName) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(fieldName + " out of range: " + port);
        }
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerConfig that)) return false;

        return brokerId == that.brokerId
                && brokerPort == that.brokerPort
                && brokerHost.equals(that.brokerHost);
    }

    @Override
    public int hashCode() {
        int result = brokerId;
        result = 31 * result + brokerHost.hashCode();
        result = 31 * result + brokerPort;
        return result;
    }
}
