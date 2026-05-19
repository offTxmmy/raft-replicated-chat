package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;
/**
 * Raft RPC sent by a candidate to request a vote.
 */
public class RequestVoteRequestMessage implements Serializable {

    //private static final long serialVersionUID = 1L;
    private final long term;
    private final int candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;

    public RequestVoteRequestMessage(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }
}
