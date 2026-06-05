package it.polimi.ds.chat.protocol.directory;

import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;

import java.io.Serializable;
import java.util.Map;

public class GetClusterResponseMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean ok;
    private final Map<Integer, RaftPeerEndpoint> voters;

    public GetClusterResponseMessage(boolean ok, Map<Integer, RaftPeerEndpoint> voters) {
        this.ok = ok;
        this.voters = voters;
    }

    public boolean isOk() {
        return ok;
    }

    public Map<Integer, RaftPeerEndpoint> getVoters() {
        return voters;
    }

    @Override
    public String toString() {
        return "GetClusterResponseMessage{ok=" + ok + ", voters=" + (voters == null ? "null" : voters.keySet()) + "}";
    }
}