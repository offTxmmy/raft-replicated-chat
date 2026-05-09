package it.polimi.ds.chat.ordering.raft;

/**
 * Durable storage hook for the two pieces of Raft state that must survive a crash:
 * {@code currentTerm} and {@code votedFor}.
 *
 * <p>Raft safety requires that a vote, once granted, is never forgotten in the
 * same term. Without durable {@code votedFor}, a node could vote for candidate X,
 * crash, restart, and vote for candidate Y in the same term, breaking the
 * at-most-one-leader-per-term invariant. Same reasoning applies to
 * {@code currentTerm}: a stale term after restart would enable split brain.
 *
 * <p>Implementations must persist the pair atomically and synchronously: the call
 * must return only after the new value is durable on stable storage. The caller
 * (typically {@link RaftNode}) invokes this hook before any side effect that
 * exposes the new state to the outside world (e.g. before returning a vote
 * response or sending an RPC that reflects the new term).
 *
 * <p>Person C owns the production implementation (file-based).
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
