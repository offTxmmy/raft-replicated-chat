package it.polimi.ds.chat.messages.raft;

import java.io.Serializable;

/**
 * Raft RPC response to an AppendEntries request.
 */
public class AppendEntriesResponseMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    // Responder's current term (for leader to detect stale term).
    private final long term;
    // True if follower accepted prevLogIndex/prevLogTerm and appended entries.
    private final boolean success;
    // Follower id sending the response.
    private final int responderId;
    // Highest log index known to be replicated on the responder (0 if none).
    private final long matchIndex;

    public AppendEntriesResponseMessage(long term, boolean success, int responderId, long matchIndex) {
        this.term = term;
        this.success = success;
        this.responderId = responderId;
        this.matchIndex = matchIndex;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getResponderId() {
        return responderId;
    }

    public long getMatchIndex() {
        return matchIndex;
    }
}
