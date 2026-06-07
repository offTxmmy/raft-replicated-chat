package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Manages Raft commit progression and state-machine application.
 *
 * <p>This component tracks two indexes:
 * <ul>
 *   <li>{@code commitIndex}: highest log entry known to be committed.</li>
 *   <li>{@code lastApplied}: highest log entry already applied locally.</li>
 * </ul>
 *
 * <p>Main responsibilities:
 * <ul>
 *   <li>keep {@code commitIndex} monotonic;</li>
 *   <li>apply entries in strict index order, without gaps;</li>
 *   <li>enforce Raft leader commit rule when advancing from match indexes
 *       (commit only entries from the current term).</li>
 * </ul>
 *
 * <p>On startup, callers may provide persisted commit progress. In that case
 * the manager resumes from the supplied state and applies only the missing
 * suffix in {@code (lastApplied, commitIndex]}.
 */
public class RaftCommitManager {

    /**
     * Callback used to persist the current commit progress after it changes.
     */
    @FunctionalInterface
    public interface CommitProgressListener {
        void onProgress(long commitIndex, long lastApplied);
    }

    private final RaftLog log;
    private final Consumer<RaftLogEntry> applyCallback;
    private final CommitProgressListener progressListener;

    private long commitIndex;
    private long lastApplied;

    public RaftCommitManager(RaftLog log, Consumer<RaftLogEntry> applyCallback) {
        this(log, applyCallback, 0L, 0L, (commitIndex, lastApplied) -> {});
    }

    /**
     * Builds a commit manager with explicit initial progress.
     *
     * <p>The initial state is normalized to keep invariants valid:
     * {@code commitIndex >= 0} and {@code 0 <= lastApplied <= commitIndex}.
     * Any unapplied portion up to {@code commitIndex} is applied immediately.
     */
    public RaftCommitManager(
            RaftLog log,
            Consumer<RaftLogEntry> applyCallback,
            long initialCommitIndex,
            long initialLastApplied,
            CommitProgressListener progressListener
    ) {
        this.log = Objects.requireNonNull(log, "log must not be null");
        this.applyCallback = Objects.requireNonNull(applyCallback, "applyCallback must not be null");
        this.progressListener = Objects.requireNonNull(progressListener, "progressListener must not be null");
        this.commitIndex = Math.max(0L, initialCommitIndex);
        this.lastApplied = Math.max(0L, Math.min(initialLastApplied, this.commitIndex));
        applyCommittedEntries();
        notifyProgress();
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
        notifyProgress();
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
        notifyProgress();
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
        notifyProgress();
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
            notifyProgress();
        }
    }

    private void notifyProgress() {
        progressListener.onProgress(commitIndex, lastApplied);
    }
}
