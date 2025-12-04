package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.ChatDeliverMessage;
import it.polimi.ds.chat.messages.ChatReqMessage;
import it.polimi.ds.chat.ordering.OrderingService;
import it.polimi.ds.chat.ordering.SequencerOrderingService;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the OrderingService abstraction.
 */
public class OrderingServiceTest {

    private SequencerOrderingService sequencerService;
    private List<ChatDeliverMessage> deliveredMessages;

    @BeforeEach
    void setUp() {
        deliveredMessages = Collections.synchronizedList(new ArrayList<>());

        // Create a sequencer config
        BrokerConfig sequencerConfig = new BrokerConfig(
                0,           // brokerId
                true,        // isSequencer
                "localhost",
                15000,       // brokerPort (not used in this test)
                15001,       // sequencerPort (not used in this test)
                null,        // sequencerHost
                0,           // sequencerPort (for connecting)
                0,           // clientCount
                new HandlerState()
        );

        sequencerService = new SequencerOrderingService(sequencerConfig, 0);
        sequencerService.onDeliver(deliveredMessages::add);
    }

    @AfterEach
    void tearDown() {
        if (sequencerService != null) {
            sequencerService.stop();
        }
    }

    @Test
    @DisplayName("OrderingService interface methods exist and work")
    void testInterfaceMethods() {
        // Test isLeader
        assertTrue(sequencerService.isLeader(), "Sequencer should be leader");

        // Test getLeaderId
        assertEquals(0, sequencerService.getLeaderId(), "Leader ID should be 0");
    }

    @Test
    @DisplayName("Sequencer assigns monotonically increasing sequence numbers")
    void testSequenceNumberAssignment() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);

        List<ChatDeliverMessage> ordered = Collections.synchronizedList(new ArrayList<>());
        sequencerService.onDeliver(msg -> {
            ordered.add(msg);
            latch.countDown();
        });

        // Propose 3 messages
        VectorClock vc1 = new VectorClock();
        vc1.increment(0);
        sequencerService.propose(new ChatReqMessage("0-1", 0, "Alice", "First", vc1));

        VectorClock vc2 = new VectorClock();
        vc2.increment(0);
        vc2.increment(0);
        sequencerService.propose(new ChatReqMessage("0-2", 0, "Alice", "Second", vc2));

        VectorClock vc3 = new VectorClock();
        vc3.increment(0);
        vc3.increment(0);
        vc3.increment(0);
        sequencerService.propose(new ChatReqMessage("0-3", 0, "Alice", "Third", vc3));

        // Wait for delivery
        assertTrue(latch.await(2, TimeUnit.SECONDS), "Messages should be delivered");

        // Verify sequence numbers are monotonically increasing
        assertEquals(3, ordered.size());
        assertEquals(1, ordered.get(0).getSeq());
        assertEquals(2, ordered.get(1).getSeq());
        assertEquals(3, ordered.get(2).getSeq());
    }

    @Test
    @DisplayName("Delivered messages contain correct content")
    void testMessageContent() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        ChatDeliverMessage[] received = new ChatDeliverMessage[1];
        sequencerService.onDeliver(msg -> {
            received[0] = msg;
            latch.countDown();
        });

        VectorClock vc = new VectorClock();
        vc.increment(1);
        sequencerService.propose(new ChatReqMessage("1-1", 1, "Bob", "Hello World", vc));

        assertTrue(latch.await(2, TimeUnit.SECONDS));

        assertNotNull(received[0]);
        assertEquals("Bob", received[0].getUsername());
        assertEquals("Hello World", received[0].getText());
        assertEquals(1, received[0].getBrokerId());
        assertEquals(1, received[0].getSeq());
    }

    @Test
    @DisplayName("Multiple callbacks receive same messages")
    void testMultipleCallbacks() throws InterruptedException {
        List<ChatDeliverMessage> callback1 = Collections.synchronizedList(new ArrayList<>());
        List<ChatDeliverMessage> callback2 = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch latch = new CountDownLatch(2);

        sequencerService.onDeliver(msg -> {
            callback1.add(msg);
            latch.countDown();
        });
        sequencerService.onDeliver(msg -> {
            callback2.add(msg);
            latch.countDown();
        });

        VectorClock vc = new VectorClock();
        vc.increment(0);
        sequencerService.propose(new ChatReqMessage("0-1", 0, "Alice", "Test", vc));

        assertTrue(latch.await(2, TimeUnit.SECONDS));

        // Both callbacks should receive the message
        assertEquals(1, callback1.size());
        assertEquals(1, callback2.size());
        assertEquals(callback1.get(0).getText(), callback2.get(0).getText());
    }

    @Test
    @DisplayName("Non-sequencer OrderingService reports not being leader")
    void testFollowerIsNotLeader() {
        BrokerConfig followerConfig = new BrokerConfig(
                -1,          // brokerId (will be assigned)
                false,       // isSequencer
                "localhost",
                15002,       // brokerPort
                15001,       // sequencerPort
                "localhost", // sequencerHost
                15001,       // sequencerPort
                0,
                null
        );

        SequencerOrderingService followerService = new SequencerOrderingService(followerConfig, -1);

        assertFalse(followerService.isLeader(), "Follower should not be leader");
        assertEquals(0, followerService.getLeaderId(), "Leader ID should be 0 (sequencer)");

        followerService.stop();
    }
}

