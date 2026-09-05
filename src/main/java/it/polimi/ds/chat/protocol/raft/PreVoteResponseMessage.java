package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;
import java.util.Objects;

/**
 * Preliminary election response. The responder's actual term is separate
 * from the prospective term and unique round identifier echoed from the request.
 */
public final class PreVoteResponseMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long term;
    private final long prospectiveTerm;
    private final String roundId;
    private final boolean voteGranted;
    private final int voterId;

    public PreVoteResponseMessage(long term, long prospectiveTerm, String roundId,
                                  boolean voteGranted, int voterId) {
        this.term = term;
        this.prospectiveTerm = prospectiveTerm;
        this.roundId = Objects.requireNonNull(roundId, "roundId");
        this.voteGranted = voteGranted;
        this.voterId = voterId;
    }

    public long getTerm() { return term; }

    public long getProspectiveTerm() { return prospectiveTerm; }

    public String getRoundId() { return roundId; }

    public boolean isVoteGranted() { return voteGranted; }

    public int getVoterId() { return voterId; }
}
