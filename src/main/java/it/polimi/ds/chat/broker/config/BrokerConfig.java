package it.polimi.ds.chat.broker.config;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;

import java.util.Objects;

/**
 * Configuration for a Raft broker instance.
 */
public class BrokerConfig {

    public static final String DEFAULT_DIRECTORY_HOST = "localhost";
    public static final int DEFAULT_DIRECTORY_PORT = 60000;

    private final int brokerId;
    private final String brokerHost;
    private final int clientPort;
    private final RaftConfig raftConfig;
    private final String directoryHost;
    private final int directoryPort;

    public BrokerConfig(int brokerId,
                        String brokerHost,
                        int clientPort,
                        RaftConfig raftConfig) {
        this(
                brokerId,
                brokerHost,
                clientPort,
                raftConfig,
                DEFAULT_DIRECTORY_HOST,
                DEFAULT_DIRECTORY_PORT);
    }

    public BrokerConfig(int brokerId,
                        String brokerHost,
                        int clientPort,
                        RaftConfig raftConfig,
                        String directoryHost,
                        int directoryPort) {
        this.brokerId = brokerId;
        this.brokerHost = Objects.requireNonNull(brokerHost, "brokerHost");
        this.clientPort = validatePort(clientPort, "clientPort");
        this.raftConfig = Objects.requireNonNull(raftConfig, "raftConfig");
        this.directoryHost = requireNonBlank(directoryHost, "directoryHost");
        this.directoryPort = validatePort(directoryPort, "directoryPort");
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getClientPort() {
        return clientPort;
    }

    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    public String getDirectoryHost() {
        return directoryHost;
    }

    public int getDirectoryPort() {
        return directoryPort;
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static int validatePort(int port, String fieldName) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(fieldName + " out of range: " + port);
        }
        return port;
    }
}
