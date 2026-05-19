package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Raft RPC sent by a leader to replicate log entries or to act as a heartbeat.
 *
 * If {@code entries} is empty, this message is a heartbeat that only refreshes
 * the follower's term/leader knowledge and commit index.
 */
public class AppendEntriesRequestMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    // Leader's current term.
    private final long term;
    // Leader id so followers can redirect clients.
    private final int leaderId;
    // Index of the log entry immediately preceding the new ones.
    private final long prevLogIndex;
    // Term of the log entry at prevLogIndex (0 if prevLogIndex is 0).
    private final long prevLogTerm;
    // Log entries to append; may be empty for heartbeat.
    private final List<RaftLogEntry> entries;
    // Leader's commit index for follower to advance commit state.
    private final long leaderCommit;

    public AppendEntriesRequestMessage(long term,
                                       int leaderId,
                                       long prevLogIndex,
                                       long prevLogTerm,
                                       List<RaftLogEntry> entries,
                                       long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.leaderCommit = leaderCommit;
    }

    public long getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<RaftLogEntry> getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }
}
