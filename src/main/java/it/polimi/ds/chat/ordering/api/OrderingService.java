package it.polimi.ds.chat.ordering.api;

import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;

import java.util.function.Consumer;

/**
 * Interface for message ordering services.
 *
 * This abstraction decouples the Raft ordering logic from the broker networking.
 *
 * The ordering service is responsible for:
 * 1. Receiving message proposals from brokers
 * 2. Assigning a global total order to messages
 * 3. Notifying all brokers when messages are ready to be delivered
 */
public interface OrderingService {
 
    /**
    * Propose a message for ordering.
    * The message will be assigned a global sequence number and delivered
    * to all registered callbacks once committed.
    *
    * @param request The chat request message to be ordered
    * @return true if the proposal was accepted by this ordering service, false otherwise
    */
    boolean propose(ChatReqMessage request);

    /**
     * Register a callback to be notified when ordered messages are ready for delivery.
     * The callback receives ChatDeliverMessage with assigned sequence numbers.
     *
     * @param callback Consumer that handles ordered messages
     */
    void onDeliver(Consumer<ChatDeliverMessage> callback);

    /**
     * Start the ordering service.
     * This may involve starting network listeners, connecting to peers, etc.
     */
    void start();

    /**
     * Stop the ordering service gracefully.
     */
    void stop();

    /**
     * Check if this node is currently the Raft leader.
     *
     * @return true if this node can accept proposals
     */
    boolean isLeader();

    /**
     * Get the current leader's broker ID.
     * Returns -1 if no leader is known.
     *
     * @return leader broker ID or -1
     */
    int getLeaderId();
}

