package it.polimi.ds.chat.ordering.api;

/**
 * Callback interface for ordering service events.
 * Used to notify the Broker when the observed Raft leader changes.
 */
public interface OrderingServiceCallback {
    /**
     * Called when the ordering service observes a leader change.
     *
     * @param newLeaderId the known leader id, or -1 if no leader is known
     * @param term the Raft term in which the leader was observed
     */
    default void onLeaderChanged(int newLeaderId, long term) {
        // no-op by default
    }
}

