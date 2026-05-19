package it.polimi.ds.chat.protocol.broker;

import java.io.Serializable;
import java.util.List;

/**
 * Response message from Directory Service containing the list of all broker peers.
 * Used for broker-to-broker peer discovery.
 */
public class GetPeerListResponseMessage implements Serializable {
    private final List<PeerInfo> peers;
    private final boolean success;

    public GetPeerListResponseMessage(boolean success, List<PeerInfo> peers) {
        this.success = success;
        this.peers = peers;
    }

    public List<PeerInfo> getPeers() {
        return peers;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "GetPeerListResponseMessage{" +
                "success=" + success +
                ", peers=" + peers +
                '}';
    }
}

