package it.polimi.ds.chat;

import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.common.delivery.HoldBackQueue;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HoldBackQueue.
 * Tests both total order (by sequence number) and causal order (by vector clocks).
 */
public class HoldBackQueueTest {

    private HoldBackQueue queue;

    @BeforeEach
    void setUp() {
        queue = new HoldBackQueue(1); // Expect sequence starting from 1
    }

    // =========================================================================
    // TOTAL ORDER TESTS
    // =========================================================================

    @Test
    @DisplayName("Messages in sequence order are delivered immediately")
    void testInOrderDelivery() {
        VectorClock vc1 = createVectorClock(0, 1);
        VectorClock vc2 = createVectorClock(0, 2);
        VectorClock vc3 = createVectorClock(0, 3);

        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "Alice", "Hello", vc1);
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 0, "Alice", "World", vc2);
        ChatDeliverMessage msg3 = new ChatDeliverMessage(3, 0, "Alice", "!", vc3);

        List<ChatDeliverMessage> result1 = queue.enqueue(msg1);
        assertEquals(1, result1.size());
        assertEquals("Hello", result1.get(0).getText());

        List<ChatDeliverMessage> result2 = queue.enqueue(msg2);
        assertEquals(1, result2.size());
        assertEquals("World", result2.get(0).getText());

        List<ChatDeliverMessage> result3 = queue.enqueue(msg3);
        assertEquals(1, result3.size());
        assertEquals("!", result3.get(0).getText());

        assertEquals(4, queue.getExpectedSeq());
        assertEquals(0, queue.getPendingCount());
    }

    @Test
    @DisplayName("Out-of-order message is held back until gap is filled")
    void testOutOfOrderHoldBack() {
        VectorClock vc1 = createVectorClock(0, 1);
        VectorClock vc2 = createVectorClock(0, 2);
        VectorClock vc3 = createVectorClock(0, 3);

        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "Alice", "First", vc1);
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 0, "Alice", "Second", vc2);
        ChatDeliverMessage msg3 = new ChatDeliverMessage(3, 0, "Alice", "Third", vc3);

        // Deliver msg1
        List<ChatDeliverMessage> result1 = queue.enqueue(msg1);
        assertEquals(1, result1.size());
        assertEquals(2, queue.getExpectedSeq());

        // Send msg3 before msg2 - should be held back
        List<ChatDeliverMessage> result3 = queue.enqueue(msg3);
        assertEquals(0, result3.size()); // Held back
        assertEquals(1, queue.getPendingCount());
        assertEquals(2, queue.getExpectedSeq()); // Still waiting for seq=2

        // Now send msg2 - both msg2 and msg3 should be released
        List<ChatDeliverMessage> result2 = queue.enqueue(msg2);
        assertEquals(2, result2.size());
        assertEquals("Second", result2.get(0).getText());
        assertEquals("Third", result2.get(1).getText());

        assertEquals(4, queue.getExpectedSeq());
        assertEquals(0, queue.getPendingCount());
    }

    @Test
    @DisplayName("Multiple gaps are handled correctly")
    void testMultipleGaps() {
        // Create messages with sequence 1, 3, 5 (missing 2, 4)
        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "A", "1", createVectorClock(0, 1));
        ChatDeliverMessage msg3 = new ChatDeliverMessage(3, 0, "A", "3", createVectorClock(0, 3));
        ChatDeliverMessage msg5 = new ChatDeliverMessage(5, 0, "A", "5", createVectorClock(0, 5));
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 0, "A", "2", createVectorClock(0, 2));
        ChatDeliverMessage msg4 = new ChatDeliverMessage(4, 0, "A", "4", createVectorClock(0, 4));

        // Deliver 1
        assertEquals(1, queue.enqueue(msg1).size());

        // 3 and 5 held back
        assertEquals(0, queue.enqueue(msg3).size());
        assertEquals(0, queue.enqueue(msg5).size());
        assertEquals(2, queue.getPendingCount());

        // Deliver 2 -> releases 2, 3
        List<ChatDeliverMessage> result = queue.enqueue(msg2);
        assertEquals(2, result.size());
        assertEquals("2", result.get(0).getText());
        assertEquals("3", result.get(1).getText());
        assertEquals(1, queue.getPendingCount()); // msg5 still pending

        // Deliver 4 -> releases 4, 5
        result = queue.enqueue(msg4);
        assertEquals(2, result.size());
        assertEquals("4", result.get(0).getText());
        assertEquals("5", result.get(1).getText());
        assertEquals(0, queue.getPendingCount());
    }

    @Test
    @DisplayName("Duplicate messages are ignored")
    void testDuplicateIgnored() {
        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "A", "Hello", createVectorClock(0, 1));
        ChatDeliverMessage msg1Dup = new ChatDeliverMessage(1, 0, "A", "Hello", createVectorClock(0, 1));

        assertEquals(1, queue.enqueue(msg1).size());
        assertEquals(0, queue.enqueue(msg1Dup).size()); // Duplicate ignored
        assertEquals(2, queue.getExpectedSeq());
    }

    @Test
    @DisplayName("Old messages (already delivered) are ignored")
    void testOldMessageIgnored() {
        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "A", "First", createVectorClock(0, 1));
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 0, "A", "Second", createVectorClock(0, 2));
        ChatDeliverMessage msgOld = new ChatDeliverMessage(1, 0, "A", "First Again", createVectorClock(0, 1));

        queue.enqueue(msg1);
        queue.enqueue(msg2);

        // Try to enqueue an old message
        List<ChatDeliverMessage> result = queue.enqueue(msgOld);
        assertEquals(0, result.size());
        assertEquals(3, queue.getExpectedSeq());
    }

    // =========================================================================
    // CAUSAL ORDER TESTS
    // =========================================================================

    @Test
    @DisplayName("Causal order is respected - message from broker 1 depends on broker 0")
    void testCausalDependency() {
        // Message from broker 0: VC = {0:1}
        VectorClock vc1 = createVectorClock(0, 1);
        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "Alice", "From B0", vc1);

        // Message from broker 1 that depends on broker 0's message: VC = {0:1, 1:1}
        VectorClock vc2 = new VectorClock();
        vc2.increment(0); // Saw broker 0's message
        vc2.increment(1); // Own event
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 1, "Bob", "From B1", vc2);

        // If msg2 arrives first (seq=2 before seq=1), it should be held back
        // because its causal dependency (msg1 from broker 0) hasn't been delivered

        // First deliver msg1
        List<ChatDeliverMessage> result1 = queue.enqueue(msg1);
        assertEquals(1, result1.size());

        // Then deliver msg2
        List<ChatDeliverMessage> result2 = queue.enqueue(msg2);
        assertEquals(1, result2.size());
    }

    @Test
    @DisplayName("Messages from different brokers without dependencies")
    void testConcurrentMessages() {
        // Two independent messages from different brokers
        VectorClock vc1 = createVectorClock(0, 1);
        VectorClock vc2 = createVectorClock(1, 1);

        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "Alice", "From B0", vc1);
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 1, "Bob", "From B1", vc2);

        // Both should be deliverable (no causal dependency between them)
        assertEquals(1, queue.enqueue(msg1).size());
        assertEquals(1, queue.enqueue(msg2).size());
    }

    @Test
    @DisplayName("Complex causal chain across multiple brokers")
    void testComplexCausalChain() {
        // B0 sends msg1: VC={0:1}
        VectorClock vc1 = createVectorClock(0, 1);
        ChatDeliverMessage msg1 = new ChatDeliverMessage(1, 0, "Alice", "Msg1", vc1);

        // B1 receives msg1 and sends msg2: VC={0:1, 1:1}
        VectorClock vc2 = new VectorClock();
        vc2.increment(0);
        vc2.increment(1);
        ChatDeliverMessage msg2 = new ChatDeliverMessage(2, 1, "Bob", "Msg2", vc2);

        // B2 receives msg2 and sends msg3: VC={0:1, 1:1, 2:1}
        VectorClock vc3 = new VectorClock();
        vc3.increment(0);
        vc3.increment(1);
        vc3.increment(2);
        ChatDeliverMessage msg3 = new ChatDeliverMessage(3, 2, "Charlie", "Msg3", vc3);

        // All arrive in order, should all be delivered
        assertEquals(1, queue.enqueue(msg1).size());
        assertEquals(1, queue.enqueue(msg2).size());
        assertEquals(1, queue.enqueue(msg3).size());

        assertEquals(4, queue.getExpectedSeq());
    }

    @Test
    @DisplayName("Queue blocks when Raft order violates local vector-clock order")
    void queueBlocksWhenRaftOrderViolatesLocalVectorClockOrder() {
        HoldBackQueue queue = new HoldBackQueue();

        VectorClock vc2 = new VectorClock();
        vc2.increment(1);
        vc2.increment(1);

        VectorClock vc1 = new VectorClock();
        vc1.increment(1);

        ChatDeliverMessage secondMessageFirstInRaft =
                new ChatDeliverMessage(1, 1, "alice", "second", vc2);

        ChatDeliverMessage firstMessageSecondInRaft =
                new ChatDeliverMessage(2, 1, "bob", "first", vc1);

        assertTrue(queue.enqueue(secondMessageFirstInRaft).isEmpty());
        assertTrue(queue.enqueue(firstMessageSecondInRaft).isEmpty());

        assertEquals(1, queue.getExpectedSeq());
        assertTrue(queue.hasPendingMessages());
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    /**
     * Create a simple VectorClock with one entry for the given broker.
     */
    private VectorClock createVectorClock(int brokerId, int timestamp) {
        VectorClock vc = new VectorClock();
        for (int i = 0; i < timestamp; i++) {
            vc.increment(brokerId);
        }
        return vc;
    }
}

