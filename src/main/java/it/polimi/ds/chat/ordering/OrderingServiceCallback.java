package it.polimi.ds.chat.ordering;

/**
 * Callback interface for ordering service events.
 * Used to notify the Broker of important events like broker ID assignment.
 */
public interface OrderingServiceCallback {

    /**
     * Called when the ordering service assigns a new broker ID.
     * This happens when a follower connects to the sequencer.
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
}

