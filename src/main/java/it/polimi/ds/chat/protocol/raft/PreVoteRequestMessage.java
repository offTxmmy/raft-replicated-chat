package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;
import java.util.Objects;

/**
 * Preliminary election request. Its term is prospective, not an instruction
 * to advance the receiver's current term or persist a vote.
 */
public final class PreVoteRequestMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long term;
    private final int candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;
    private final String roundId;

    public PreVoteRequestMessage(long term, int candidateId, long lastLogIndex,
                                 long lastLogTerm, String roundId) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.roundId = Objects.requireNonNull(roundId, "roundId");
    }

    public long getTerm() { return term; }

    public int getCandidateId() { return candidateId; }

    public long getLastLogIndex() { return lastLogIndex; }

    public long getLastLogTerm() { return lastLogTerm; }

    public String getRoundId() { return roundId; }
}
