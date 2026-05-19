package it.polimi.ds.chat.protocol.broker;

import java.io.Serializable;

/**
 * Information about a broker peer in the cluster.
 * Used for broker-to-broker discovery.
 */
public class PeerInfo implements Serializable {
    private final int brokerId;
    private final String host;
    private final int port;
    private final boolean isSequencer;

    public PeerInfo(int brokerId, String host, int port, boolean isSequencer) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.isSequencer = isSequencer;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isSequencer() {
        return isSequencer;
    }

    @Override
    public String toString() {
        return "PeerInfo{" +
                "brokerId=" + brokerId +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", isSequencer=" + isSequencer +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeerInfo peerInfo = (PeerInfo) o;
        return brokerId == peerInfo.brokerId;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(brokerId);
    }
}

