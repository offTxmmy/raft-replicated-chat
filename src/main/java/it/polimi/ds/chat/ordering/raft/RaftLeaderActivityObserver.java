package it.polimi.ds.chat.ordering.raft;

/**
 * Callback for valid leader activity.
 * Used by the replication layer to reset election timeouts without
 * depending directly on the election manager.
 */
public interface RaftLeaderActivityObserver {

    /**
     * Invoked when a leader heartbeat or AppendEntries is accepted.
     *
     * @param term leader term
     * @param leaderId leader id
     */
    void onValidLeaderActivityObserved(long term, int leaderId);
}
