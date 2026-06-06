package it.polimi.ds.chat.common.delivery;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;

import java.util.*;

/**
 * Hold-back queue for ordered message delivery.
 *
 * Ensures both:
 * <ul>
 *   <li><b>Total Order:</b> Messages are delivered in strict sequence number order (assigned by Raft log index)</li>
 *   <li><b>Causal Order:</b> Messages respect causal dependencies tracked via vector clocks</li>
 * </ul>
 * A message can be delivered only when:
 * <ul>
 *   <li>Its sequence number equals the next expected sequence number (total order)</li>
 *   <li>All its causal dependencies have been delivered (causal order via vector clocks)</li>
 * </ul>
 */
public class HoldBackQueue {

    // Priority queue ordered by sequence number (lowest first)
    private final PriorityQueue<ChatDeliverMessage> pendingMessages;

    // Next expected sequence number for total order
    private long expectedSeq;

    // Vector clock tracking what has been delivered
    private final VectorClock deliveredClock;

    /**
     * Constructs a HoldBackQueue with the default initial expected sequence number (1).
     */
    public HoldBackQueue() {
        this(1); // Default: expect sequence to start at 1
    }

    /**
     * Constructs a HoldBackQueue with a specified initial expected sequence number.
     *
     * @param initialExpectedSeq the initial expected sequence number
     */
    public HoldBackQueue(long initialExpectedSeq) {
        this.pendingMessages = new PriorityQueue<>(Comparator.comparingLong(ChatDeliverMessage::getSeq));
        this.expectedSeq = initialExpectedSeq;
        this.deliveredClock = new VectorClock();
    }

    /**
     * Add a message to the hold-back queue and return all messages
     * that are now ready to be delivered (in order).
     *
     * @param message The incoming ChatDeliverMessage
     * @return List of messages ready for delivery (may be empty), in delivery order
     */
    public synchronized List<ChatDeliverMessage> enqueue(ChatDeliverMessage message) {
        // Ignore duplicate or already-delivered messages
        if (message.getSeq() < expectedSeq) {
            System.out.println("[HoldBackQueue] Ignoring duplicate/old message with seq=" + message.getSeq()
                    + " (expected >= " + expectedSeq + ")");
            return Collections.emptyList();
        }

        // Check if message is already in the queue
        boolean alreadyQueued = pendingMessages.stream()
                .anyMatch(m -> m.getSeq() == message.getSeq());
        if (alreadyQueued) {
            return Collections.emptyList();
        }

        pendingMessages.add(message);
        return releaseReadyMessages();
    }

    /**
     * Release all messages that satisfy both total and causal order conditions.
     *
     * @return List of messages ready for delivery
     */
    private List<ChatDeliverMessage> releaseReadyMessages() {
        List<ChatDeliverMessage> readyToDeliver = new ArrayList<>();

        while (!pendingMessages.isEmpty()) {
            ChatDeliverMessage head = pendingMessages.peek();

            // Check total order: must be the next expected sequence
            if (head.getSeq() != expectedSeq) {
                // Gap in sequence - must wait for missing message(s)
                break;
            }

            // Check causal order: all cross-broker dependencies must have been delivered
            if (!canDeliverCausally(head)) {
                // Causal dependency not yet satisfied - must wait
                // Note: This should not happen when Raft applies committed entries in order,
                // but we keep this check for robustness
                System.out.println("[HoldBackQueue] Message seq=" + head.getSeq() +
                        " waiting for causal dependencies");
                break;
            }

            // Message is ready: remove from queue and prepare for delivery
            pendingMessages.poll();
            readyToDeliver.add(head);

            // Update state: increment expected sequence and merge vector clock
            expectedSeq++;
            deliveredClock.update(head.getVectorClock());
        }

        return readyToDeliver;
    }

    /**
     * Check if a message's causal dependencies are satisfied.
     *
     * <p>A message M from sender S can be delivered when, for every broker
     * K != S referenced in M's vector clock,
     * {@code messageClock[K] <= deliveredClock[K]}.
     *
     * <p>The sender's own component is intentionally not checked: Raft total
     * order is authoritative for ordering events from the same broker.
     * Propose-time vector-clock increments can disagree with the committed
     * Raft log order (two concurrent proposals from the same broker may be
     * reordered by Raft), so enforcing strict per-sender FIFO here would
     * permanently block such cases without adding any real safety.
     *
     * @param message the message to check
     * @return true if causal dependencies are satisfied, false otherwise
     */
    private boolean canDeliverCausally(ChatDeliverMessage message) {
        VectorClock messageClock = message.getVectorClock();
        if (messageClock == null) {
            // No vector clock attached - allow delivery (for backward compatibility)
            return true;
        }

        int senderId = message.getBrokerId();

        // Check dependencies from other brokers; the sender's own component is
        // ignored on purpose (see javadoc).
        for (Map.Entry<Integer, Integer> entry : messageClock.getClock().entrySet()) {
            int brokerId = entry.getKey();
            int msgTimestamp = entry.getValue();

            if (brokerId == senderId) {
                continue;
            }

            int deliveredTimestamp = deliveredClock.getTimeStamp(brokerId);
            if (msgTimestamp > deliveredTimestamp) {
                // Message depends on an event from brokerId that hasn't been delivered yet
                return false;
            }
        }

        return true;
    }

    /**
     * Updates the expected sequence number directly.
     * Used when a broker joins an existing cluster and needs to sync state.
     *
     * @param seq the current global sequence number (next expected will be seq + 1)
     */
    public synchronized void syncToSequence(long seq) {
        this.expectedSeq = seq + 1;
        System.out.println("[HoldBackQueue] Synced state. Fast-forwarding expectedSeq to " + expectedSeq);
    }

    /**
     * Get the current expected sequence number.
     *
     * @return the next expected sequence number
     */
    public synchronized long getExpectedSeq() {
        return expectedSeq;
    }

    /**
     * Get a copy of the current delivered vector clock.
     *
     * @return a copy of the delivered vector clock
     */
    public synchronized VectorClock getDeliveredClock() {
        return new VectorClock(deliveredClock);
    }

    /**
     * Get the number of messages currently held back.
     *
     * @return the number of pending messages
     */
    public synchronized int getPendingCount() {
        return pendingMessages.size();
    }

    /**
     * Check if there are any pending messages waiting for delivery.
     *
     * @return true if there are pending messages, false otherwise
     */
    public synchronized boolean hasPendingMessages() {
        return !pendingMessages.isEmpty();
    }

    /**
     * Returns a string representation of the HoldBackQueue, including expected sequence,
     * number of pending messages, and the delivered vector clock.
     *
     * @return string representation of the HoldBackQueue
     */
    @Override
    public synchronized String toString() {
        return "HoldBackQueue{expectedSeq=" + expectedSeq +
                ", pending=" + pendingMessages.size() +
                ", deliveredClock=" + deliveredClock + "}";
    }
}