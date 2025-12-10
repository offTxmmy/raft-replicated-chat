package it.polimi.ds.chat.messages;

import java.io.Serializable;

/**
 * Request message sent by a broker to the Directory Service
 * to get the list of all registered broker peers.
 */
public class GetPeerListRequestMessage implements Serializable {
    private final int requestingBrokerId;

    public GetPeerListRequestMessage(int requestingBrokerId) {
        this.requestingBrokerId = requestingBrokerId;
    }

    public int getRequestingBrokerId() {
        return requestingBrokerId;
    }

    @Override
    public String toString() {
        return "GetPeerListRequestMessage{requestingBrokerId=" + requestingBrokerId + '}';
    }
}

