package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Tracks commit progress and applies committed entries in order.
 *
 * Responsibilities:
 * - Keep commitIndex monotonic.
 * - Apply log entries exactly once and in index order.
 * - Enforce Raft's leader commit rule (only commit entries from current term
 *   when advancing based on majority match indexes).
 */
public class RaftCommitManager {

    private final RaftLog log;
    private final Consumer<RaftLogEntry> applyCallback;

    private long commitIndex;
    private long lastApplied;

    public RaftCommitManager(RaftLog log, Consumer<RaftLogEntry> applyCallback) {
        this.log = Objects.requireNonNull(log, "log must not be null");
        this.applyCallback = Objects.requireNonNull(applyCallback, "applyCallback must not be null");
        this.commitIndex = 0L;
        this.lastApplied = 0L;
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized long getLastApplied() {
        return lastApplied;
    }

    /**
     * Advances commit index to the given value and applies newly committed entries.
     *
     * @param newCommitIndex target commit index
     */
    public synchronized void advanceCommitIndex(long newCommitIndex) {
        if (newCommitIndex <= commitIndex) {
            return;
        }

        commitIndex = newCommitIndex;
        applyCommittedEntries();
    }

    /**
     * Updates commit index based on a leader commit hint.
     *
     * @param leaderCommitIndex leader's commit index
     */
    public synchronized void updateCommitIndexFromLeader(long leaderCommitIndex) {
        long cappedCommitIndex = Math.min(leaderCommitIndex, log.lastLogIndex());
        if (cappedCommitIndex <= commitIndex) {
            return;
        }

        commitIndex = cappedCommitIndex;
        applyCommittedEntries();
    }

    /**
     * Leader-side commit calculation based on follower match indexes.
     *
     * @param matchIndexes match indexes from all voters (including leader)
     * @param majority required majority count
     * @param currentTerm current leader term
     * @return the updated commit index
     */
    public synchronized long tryAdvanceCommitIndex(Collection<Long> matchIndexes, int majority, long currentTerm) {
        if (matchIndexes == null || matchIndexes.isEmpty()) {
            return commitIndex;
        }
        if (majority <= 0) {
            throw new IllegalArgumentException("majority must be > 0");
        }

        List<Long> sorted = new ArrayList<>(matchIndexes);
        Collections.sort(sorted);

        if (majority > sorted.size()) {
            return commitIndex;
        }

        long candidateIndex = sorted.get(sorted.size() - majority);
        if (candidateIndex <= commitIndex || candidateIndex == 0L) {
            return commitIndex;
        }

        long candidateTerm = log.getTermAt(candidateIndex);
        if (candidateTerm != currentTerm) {
            return commitIndex;
        }

        commitIndex = candidateIndex;
        applyCommittedEntries();
        return commitIndex;
    }

    /**
     * Applies all entries in (lastApplied, commitIndex] in order.
     *
     * Stops early if a log entry is missing, preserving safety until the log
     * catches up. Each entry is delivered to the state machine via applyCallback.
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            long nextIndex = lastApplied + 1L;
            RaftLogEntry entry = log.getEntry(nextIndex);
            if (entry == null) {
                return;
            }
            applyCallback.accept(entry);
            lastApplied = nextIndex;
        }
    }
}
