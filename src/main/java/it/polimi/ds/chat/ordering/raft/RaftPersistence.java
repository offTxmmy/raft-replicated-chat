package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.Collections;
import java.util.List;

/**
 * Durable storage hook for Raft state that must survive a crash:
 * {@code currentTerm}, {@code votedFor}, the replicated log, and commit progress.
 *
 * <p>Raft safety requires that a vote, once granted, is never forgotten in the
 * same term. Without durable {@code votedFor}, a node could vote for candidate X,
 * crash, restart, and vote for candidate Y in the same term, breaking the
 * at-most-one-leader-per-term invariant. Same reasoning applies to
 * {@code currentTerm}: a stale term after restart would enable split brain.
 *
 * <p>Implementations must persist atomically and synchronously: the call
 * must return only after the new value is durable on stable storage. The caller
 * (typically {@link RaftNode}) invokes the term/vote hook before any side effect
 * that exposes the new state to the outside world (e.g. before returning a vote
 * response or sending an RPC that reflects the new term).

 * <p>Persisting {@code commitIndex}/{@code lastApplied} is not a Raft election
 * requirement, but it provides deterministic restart behavior for the local
 * state machine: after a crash, the node can reconstruct the already-applied
 * prefix silently, then execute only the committed-but-not-applied suffix as
 * new application work.
 *
 * <p>Log and commit-progress methods are default no-op so that test doubles only
 * interested in election state do not need to implement them.
 *
 * Tests use an in-memory fake.
 */
public interface RaftPersistence extends AutoCloseable {

    /** Fences every protocol operation after a durable-state failure or close. */
    default void checkHealthy() { }

    @Override
    default void close() { }

    /**
     * Persists the current Raft term and vote atomically and synchronously.
     *
     * @param currentTerm the latest term known to the node
     * @param votedFor the candidate id this node voted for in {@code currentTerm},
     *                 or {@code null} if the node has not voted yet in this term
     */
    void persistTermAndVote(long currentTerm, Integer votedFor);

    /**
     * Loads the persisted (currentTerm, votedFor) pair, or {@link PersistedState#EMPTY}
     * if nothing has been persisted yet (first boot).
     */
    default PersistedState loadTermAndVote() {
        return PersistedState.EMPTY;
    }

    /**
     * Appends a single log entry to durable storage, synchronously fsynced.
     * Entries are appended in strictly increasing index order.
     */
    default void appendLogEntry(RaftLogEntry entry) {
        // default no-op for test doubles that don't exercise log persistence
    }

    /**
     * Removes all persisted log entries with index &gt;= {@code fromIndex}.
     * Used when an AppendEntries from a new leader conflicts with the local tail.
     */
    default void truncateLogFrom(long fromIndex) {
        // default no-op
    }

    /**
     * Loads all persisted log entries in index order. Used at startup to
     * rebuild the in-memory {@link RaftLog}.
     */
    default List<RaftLogEntry> loadLogEntries() {
        return Collections.emptyList();
    }

    /**
     * Persists the current commit progress of the state machine.
        *
        * <p>Implementations should persist atomically and synchronously, so that
        * on restart the node can reconstruct a consistent applied prefix.
     *
     * @param commitIndex highest committed log index known by the node
     * @param lastApplied highest log index already applied to the state machine
     */
    default void persistCommitProgress(long commitIndex, long lastApplied) {
        // default no-op for test doubles that do not exercise restart recovery
    }

    /**
     * Loads the persisted commit progress, or {@link CommitProgress#EMPTY} if
     * no progress has been recorded yet.
     *
     * <p>The loaded values are expected to satisfy:
     * {@code 0 <= lastApplied <= commitIndex}.
     */
    default CommitProgress loadCommitProgress() {
        return CommitProgress.EMPTY;
    }

    /**
     * Snapshot of the durable (currentTerm, votedFor) pair.
     */
    record PersistedState(long currentTerm, Integer votedFor) {
        public static final PersistedState EMPTY = new PersistedState(0L, null);
    }

    /**
     * Snapshot of the persisted commit progress.
     *
     * @param commitIndex highest known committed index
     * @param lastApplied highest index already applied to the local state machine
     */
    record CommitProgress(long commitIndex, long lastApplied) {
        public static final CommitProgress EMPTY = new CommitProgress(0L, 0L);
    }

    /**
     * No-op implementation, useful for unit tests that do not exercise the
     * persistence call site.
     */
    RaftPersistence NO_OP = new RaftPersistence() {
        @Override
        public void persistTermAndVote(long currentTerm, Integer votedFor) {
            // intentionally empty
        }
    };
}
