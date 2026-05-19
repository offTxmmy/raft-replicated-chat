package it.polimi.ds.chat.messages.raft;

import it.polimi.ds.chat.messages.ChatReqMessage;

import java.io.Serializable;
import java.util.Objects;

/**
 * Broker-to-broker RPC used by a Raft follower to proxy a client proposal to
 * the current leader.
 */
public class ForwardClientProposalRequestMessage implements Serializable {

    private final ChatReqMessage request;

    public ForwardClientProposalRequestMessage(ChatReqMessage request) {
        this.request = Objects.requireNonNull(request, "request");
    }

    public ChatReqMessage getRequest() {
        return request;
    }
}
