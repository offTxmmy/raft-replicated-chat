package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.RaftLogEntry;

import java.util.Collections;
import java.util.List;

/**
 * Durable storage hook for Raft state that must survive a crash:
 * {@code currentTerm}, {@code votedFor}, and the replicated log.
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
 *
 * <p>Log methods are default no-op so that test doubles only interested in the
 * term/vote hook do not need to implement them.
 *
 * <p>Person C owns the production implementation ({@link FileRaftPersistence}).
 * Tests use an in-memory fake.
 */
public interface RaftPersistence {

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
     * Snapshot of the durable (currentTerm, votedFor) pair.
     */
    record PersistedState(long currentTerm, Integer votedFor) {
        public static final PersistedState EMPTY = new PersistedState(0L, null);
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