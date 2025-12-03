package it.polimi.ds.chat.integration;

import it.polimi.ds.chat.broker.Broker;
import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.ChatDeliverMessage;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorClockIntegrationTest {

    private static class InMemoryBroker extends Broker {
        private final List<String> delivered = Collections.synchronizedList(new ArrayList<>());

        InMemoryBroker(BrokerConfig config) {
            super(config);
        }

        @Override
        public void onChatDeliver(long seq, String sender, String text) {
            delivered.add(seq + ":" + sender + ":" + text);
        }

        List<String> getDelivered() {
            return delivered;
        }
    }

    private InMemoryBroker sequencer;
    private InMemoryBroker follower;

    @Before
    public void setUp() {
        HandlerState handlerState = new HandlerState();
        BrokerConfig sequencerCfg = new BrokerConfig(
                0, true, "localhost", 5000, 5000, "localhost", 5001, 0, handlerState
        );
        BrokerConfig followerCfg = new BrokerConfig(
                1, false, "localhost", 5002, 5002, "localhost", 5001, 0, null
        );
        sequencer = new InMemoryBroker(sequencerCfg);
        follower = new InMemoryBroker(followerCfg);
    }

    @Test
    public void messagesAreDeliveredAcrossBrokers() {
        VectorClock firstClock = new VectorClock();
        firstClock.increment(0);
        ChatDeliverMessage first = new ChatDeliverMessage(1L, 0, "alice", "hi", new VectorClock(firstClock));

        VectorClock secondClock = new VectorClock(firstClock);
        secondClock.increment(1);
        ChatDeliverMessage second = new ChatDeliverMessage(2L, 1, "bob", "hey", new VectorClock(secondClock));

        VectorClock thirdClock = new VectorClock(secondClock);
        thirdClock.increment(0);
        ChatDeliverMessage third = new ChatDeliverMessage(3L, 0, "alice", "all good", new VectorClock(thirdClock));

        for (ChatDeliverMessage message : List.of(first, second, third)) {
            sequencer.handleOrderedMessage(message);
            follower.handleOrderedMessage(message);
        }

        List<String> expectedOrder = List.of(
                "1:alice:hi",
                "2:bob:hey",
                "3:alice:all good"
        );

        assertEquals("Sequencer should deliver every message to its clients", expectedOrder, sequencer.getDelivered());
        assertEquals("Follower should deliver every message to its clients", expectedOrder, follower.getDelivered());

        assertTrue("Sequencer vector clock should have recorded both brokers", sequencer.getVectorClock().getClock().size() >= 2);
        assertTrue("Follower vector clock should have recorded both brokers", follower.getVectorClock().getClock().size() >= 2);
    }
}