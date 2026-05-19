package it.polimi.ds.chat.ordering.api;

/**
 * Callback interface for ordering service events.
 * Used to notify the Broker of important ordering-service events.
 */
public interface OrderingServiceCallback {

    /**
     * Called when the ordering service assigns a new broker ID.
     * Raft brokers use static IDs, so this callback is retained only for
     * backward-compatible test hooks.
     *
     * @param brokerId The assigned broker ID
     */
    void onBrokerIdAssigned(int brokerId, long currentSeq);

    /**
     * Called when connection to the ordering service is lost.
     */
    void onConnectionLost();

    /**
     * Called when connection to the ordering service is established.
     */
    void onConnectionEstablished();

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

