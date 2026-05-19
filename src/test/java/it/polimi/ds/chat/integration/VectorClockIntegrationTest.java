package it.polimi.ds.chat.integration;

import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.TestConfigs;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
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

    private InMemoryBroker broker0;
    private InMemoryBroker follower;

    @Before
    public void setUp() {
        broker0 = new InMemoryBroker(TestConfigs.raftBrokerConfig(0, 5000));
        follower = new InMemoryBroker(TestConfigs.raftBrokerConfig(1, 5002));
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
            broker0.handleOrderedMessage(message);
            follower.handleOrderedMessage(message);
        }

        List<String> expectedOrder = List.of(
                "1:alice:hi",
                "2:bob:hey",
                "3:alice:all good"
        );

        assertEquals("Broker 0 should deliver every message to its clients", expectedOrder, broker0.getDelivered());
        assertEquals("Follower should deliver every message to its clients", expectedOrder, follower.getDelivered());

        assertTrue("Broker 0 vector clock should have recorded both brokers", broker0.getVectorClock().getClock().size() >= 2);
        assertTrue("Follower vector clock should have recorded both brokers", follower.getVectorClock().getClock().size() >= 2);
    }
}
