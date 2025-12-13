package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.ChatDeliverMessage;
import it.polimi.ds.chat.messages.ChatReqMessage;
import it.polimi.ds.chat.ordering.OrderingServiceCallback;
import it.polimi.ds.chat.ordering.SequencerOrderingService;
import it.polimi.ds.chat.utilities.HoldBackQueue;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class UdpReliabilityTest {

    private SequencerOrderingService sequencer;
    private SequencerOrderingService follower;
    private HoldBackQueue followerQueue;

    // Base ports to avoid conflicts with other tests
    private static final int BASE_PORT = 30000;
    private static final int UDP_PORT = 30005; // Data will be on 30006

    @AfterEach
    public void tearDown() {
        if (follower != null) follower.stop();
        if (sequencer != null) sequencer.stop();
    }

    /**
     * TEST 1: UDP Happy Path
     */
    @Test
    public void testUdpBroadcastDelivery() throws InterruptedException {
        startSequencer();

        CountDownLatch connectionLatch = new CountDownLatch(1);
        startFollower(connectionLatch);
        assertTrue(connectionLatch.await(5, TimeUnit.SECONDS), "Follower failed to connect");

        CountDownLatch deliverLatch = new CountDownLatch(1);
        List<ChatDeliverMessage> received = new ArrayList<>();

        follower.onDeliver(msg -> {
            System.out.println("[Test] Follower received seq=" + msg.getSeq());
            received.add(msg);
            deliverLatch.countDown();
        });

        System.out.println("[Test] Proposing message 1...");
        VectorClock vc = new VectorClock();
        vc.increment(0);
        sequencer.propose(new ChatReqMessage("msg-1", 0, "Alice", "Hello UDP", vc));

        assertTrue(deliverLatch.await(2, TimeUnit.SECONDS), "Message was not delivered via UDP");
        assertEquals(1, received.size());
        assertEquals("Hello UDP", received.get(0).getText());
        assertEquals(1, received.get(0).getSeq());
    }

    /**
     * TEST 2: Reliability (NACK)
     */
    @Test
    public void testPacketLossRecoveryWhileOnline() throws InterruptedException {
        // 1. Start Sequencer
        startSequencer();

        // 2. Start Follower
        CountDownLatch connectionLatch = new CountDownLatch(1);
        startFollower(connectionLatch);
        assertTrue(connectionLatch.await(5, TimeUnit.SECONDS));

        // 3. Setup Listener
        CountDownLatch recoveryLatch = new CountDownLatch(2); // Expect 2 messages
        List<String> deliveredText = Collections.synchronizedList(new ArrayList<>());
        followerQueue = new HoldBackQueue(); // Start at 1

        // Flag to ensure we only drop the FIRST copy of message 1 (UDP), not the second (TCP)
        AtomicBoolean hasDroppedMsg1 = new AtomicBoolean(false);

        follower.onDeliver(msg -> {
            // --- SIMULATING PACKET LOSS ---
            // Only drop if it is seq=1 AND we haven't dropped it yet.
            if (msg.getSeq() == 1 && !hasDroppedMsg1.getAndSet(true)) {
                System.out.println("[Test] SIMULATING DROP of Message 1 (Ignoring UDP)");
                return; // DROP IT!
            }
            // ------------------------------

            // Normal Broker Logic
            long expected = followerQueue.getExpectedSeq();
            if (msg.getSeq() > expected) {
                System.out.println("[Test] Gap detected! Received " + msg.getSeq() + " Expected " + expected);
                for(long i = expected; i < msg.getSeq(); i++) {
                    System.out.println("[Test] NACKing " + i);
                    follower.requestRetransmission(i);
                }
            }

            List<ChatDeliverMessage> ready = followerQueue.enqueue(msg);
            for(ChatDeliverMessage m : ready) {
                deliveredText.add(m.getText());
                recoveryLatch.countDown();
            }
        });

        // 4. Send Message 1 (Will be DROPPED by our fake logic above)
        VectorClock vc1 = new VectorClock(); vc1.increment(0);
        sequencer.propose(new ChatReqMessage("m1", 0, "Alice", "Message 1", vc1));

        Thread.sleep(200);

        // 5. Send Message 2 (Will arrive, trigger gap detection, recover Msg 1)
        VectorClock vc2 = new VectorClock(); vc2.increment(0); vc2.increment(0);
        sequencer.propose(new ChatReqMessage("m2", 0, "Alice", "Message 2", vc2));

        // 6. Assert
        assertTrue(recoveryLatch.await(5, TimeUnit.SECONDS), "Recovery failed");
        assertEquals("Message 1", deliveredText.get(0)); // Recovered via TCP
        assertEquals("Message 2", deliveredText.get(1));
    }

    // Helper
    private void startSequencer() {
        BrokerConfig seqConfig = new BrokerConfig(
                0, true, "localhost", BASE_PORT, BASE_PORT + 1,
                "localhost", BASE_PORT + 2, UDP_PORT, new HandlerState()
        );
        sequencer = new SequencerOrderingService(seqConfig, 0);
        sequencer.start();
    }

    private void startFollower(CountDownLatch latch) {
        BrokerConfig folConfig = new BrokerConfig(
                -1, false, "localhost", BASE_PORT + 3, BASE_PORT + 4,
                "localhost", BASE_PORT + 2, UDP_PORT, null
        );
        follower = new SequencerOrderingService(folConfig, -1);

        follower.setCallback(new OrderingServiceCallback() {
            @Override public void onBrokerIdAssigned(int id, long seq) {}
            @Override public void onConnectionLost() {}
            @Override public void onConnectionEstablished() { latch.countDown(); }
        });

        follower.start();
    }
}