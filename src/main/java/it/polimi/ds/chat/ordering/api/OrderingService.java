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
    * @return true only after the proposal is definitively committed; false if it
    *         was rejected, failed, or timed out before commitment was confirmed
    */
    boolean propose(ChatReqMessage request);

    /**
     * Establishes a committed delivery boundary for a newly joined local client.
     * The call succeeds only after the boundary has also been applied by this
     * broker, so commands preceding it cannot become visible to that client.
     *
     * @param boundaryId unique id for this connection's JOIN boundary
     * @return true once the boundary is committed and locally applied
     */
    boolean establishDeliveryBoundary(String boundaryId);

    /**
     * Establishes a local delivery boundary and runs the activation callback
     * synchronously at that boundary. Implementations that apply an ordered
     * log must invoke {@code onApplied} from their serialized state-machine
     * application path, before any later entry can be delivered locally.
     *
     * <p>The default keeps simple/testing implementations source compatible;
     * replicated implementations must override it to provide the atomic
     * boundary semantics described above.</p>
     *
     * @param boundaryId unique id for this connection's JOIN boundary
     * @param onApplied local activation to run at the boundary
     * @return true once the boundary and activation have completed
     */
    default boolean establishDeliveryBoundary(String boundaryId, Runnable onApplied) {
        boolean established = establishDeliveryBoundary(boundaryId);
        if (established) {
            onApplied.run();
        }
        return established;
    }

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

