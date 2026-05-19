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

    public PeerInfo(int brokerId, String host, int port) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
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

    @Override
    public String toString() {
        return "PeerInfo{" +
                "brokerId=" + brokerId +
                ", host='" + host + '\'' +
                ", port=" + port +
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

