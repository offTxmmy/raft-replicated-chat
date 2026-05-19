package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;

/**
 * Response to a forwarded client proposal.
 */
public class ForwardClientProposalResponseMessage implements Serializable {

    private final boolean accepted;
    private final int leaderId;
    private final String reason;

    public ForwardClientProposalResponseMessage(boolean accepted, int leaderId, String reason) {
        this.accepted = accepted;
        this.leaderId = leaderId;
        this.reason = reason;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public String getReason() {
        return reason;
    }
}
