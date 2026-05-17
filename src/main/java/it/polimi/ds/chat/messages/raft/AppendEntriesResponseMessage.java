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
    // Term of the conflicting entry at prevLogIndex, or -1 if the log is too short.
    private final long conflictTerm;
    // First index of conflictTerm, or nextIndex hint when the log is too short.
    private final long conflictIndex;

    public AppendEntriesResponseMessage(long term,
                                        boolean success,
                                        int responderId,
                                        long matchIndex,
                                        long conflictTerm,
                                        long conflictIndex) {
        this.term = term;
        this.success = success;
        this.responderId = responderId;
        this.matchIndex = matchIndex;
        this.conflictTerm = conflictTerm;
        this.conflictIndex = conflictIndex;
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

    public long getConflictTerm() {
        return conflictTerm;
    }

    public long getConflictIndex() {
        return conflictIndex;
    }
}
