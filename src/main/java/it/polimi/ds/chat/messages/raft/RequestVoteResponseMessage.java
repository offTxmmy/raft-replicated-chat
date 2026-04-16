package it.polimi.ds.chat.messages.raft;

/**
 * Raft RPC response to a RequestVote request.
 */
public class RequestVoteResponseMessage {

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
