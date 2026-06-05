package it.polimi.ds.chat.protocol.directory;

import java.io.Serializable;

public class GetClusterRequestMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int nodeId;

    public GetClusterRequestMessage(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "GetClusterRequestMessage{nodeId=" + nodeId + "}";
    }
}