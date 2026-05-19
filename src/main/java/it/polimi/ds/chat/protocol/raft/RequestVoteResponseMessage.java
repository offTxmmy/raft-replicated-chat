package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;
/**
 * Raft RPC response to a RequestVote request.
 */
public class RequestVoteResponseMessage implements Serializable {

    //private static final long serialVersionUID = 1L;
    private final long term;
    private final boolean voteGranted;
    private final int voterId;

    public RequestVoteResponseMessage(long term, boolean voteGranted, int voterId) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.voterId = voterId;
    }

    public long getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public int getVoterId() {
        return voterId;
    }
}
