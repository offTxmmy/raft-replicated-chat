package it.polimi.ds.chat.ordering.raft;

/**
 * Leader-side per-follower replication state.
 *
 * Tracks:
 * - nextIndex: the next log index the leader will send to this follower
 * - matchIndex: the highest log index the leader knows the follower has replicated
 *
 * This is the minimal bookkeeping Raft requires to drive AppendEntries retries
 * and to compute majority commit progress.
 */
public class RaftPeerReplicationState {

    private long nextIndex;
    private long matchIndex;

    public RaftPeerReplicationState(long nextIndex) {
        if (nextIndex < 1L) {
            throw new IllegalArgumentException("nextIndex must be >= 1");
        }
        this.nextIndex = nextIndex;
        this.matchIndex = 0L;
    }

    public synchronized long getNextIndex() {
        return nextIndex;
    }

    public synchronized long getMatchIndex() {
        return matchIndex;
    }

    public synchronized void setNextIndex(long nextIndex) {
        if (nextIndex < 1L) {
            throw new IllegalArgumentException("nextIndex must be >= 1");
        }
        this.nextIndex = nextIndex;
    }

    public synchronized void decrementNextIndex() {
        if (nextIndex > 1L) {
            nextIndex--;
        }
    }

    public synchronized void updateMatchIndex(long newMatchIndex) {
        if (newMatchIndex > matchIndex) {
            matchIndex = newMatchIndex;
        }
    }
}
