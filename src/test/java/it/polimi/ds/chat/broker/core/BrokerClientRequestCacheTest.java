package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.TestConfigs;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.client.ClientMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerClientRequestCacheTest {

    @Test
    void successfulProposalIsRemovedFromTheRetryCache() {
        Broker broker = brokerWith(new ScriptedOrderingService(true));
        ClientMessage message = new ClientMessage(
                "alice", "client-a", 1L, "hello");

        assertTrue(broker.onClientMessage(message));

        assertEquals(0, broker.cachedClientRequestCountForTesting());
    }

    @Test
    void failedProposalRemainsAvailableForRetry() {
        ScriptedOrderingService ordering = new ScriptedOrderingService(false, false);
        Broker broker = brokerWith(ordering);
        ClientMessage message = new ClientMessage(
                "alice", "client-a", 1L, "hello");

        assertFalse(broker.onClientMessage(message));
        assertEquals(1, broker.cachedClientRequestCountForTesting());
        assertFalse(broker.onClientMessage(message));

        assertSame(ordering.proposals.get(0), ordering.proposals.get(1));
        assertEquals(
                ordering.proposals.get(0).getLocalMsgId(),
                ordering.proposals.get(1).getLocalMsgId());
        assertEquals(1, broker.cachedClientRequestCountForTesting());
    }

    @Test
    void successfulCleanupAllowsACommittedRetryToUseFreshMetadata() {
        ScriptedOrderingService ordering = new ScriptedOrderingService(true, true);
        Broker broker = brokerWith(ordering);
        ClientMessage message = new ClientMessage(
                "alice", "client-a", 1L, "hello");

        assertTrue(broker.onClientMessage(message));
        assertTrue(broker.onClientMessage(message));

        assertNotSame(ordering.proposals.get(0), ordering.proposals.get(1));
        assertEquals(1, ordering.proposals.get(0).getVectorClock().getTimeStamp(1));
        assertEquals(2, ordering.proposals.get(1).getVectorClock().getTimeStamp(1));
        assertEquals(0, broker.cachedClientRequestCountForTesting());
    }

    @Test
    void distinctProposalsAreCleanedIndependently() {
        ScriptedOrderingService ordering = new ScriptedOrderingService(
                false, true, true);
        Broker broker = brokerWith(ordering);
        ClientMessage first = new ClientMessage(
                "alice", "client-a", 1L, "first");
        ClientMessage second = new ClientMessage(
                "alice", "client-a", 2L, "second");

        assertFalse(broker.onClientMessage(first));
        assertTrue(broker.onClientMessage(second));
        assertEquals(1, broker.cachedClientRequestCountForTesting());

        assertTrue(broker.onClientMessage(first));
        assertSame(ordering.proposals.get(0), ordering.proposals.get(2));
        assertNotSame(ordering.proposals.get(0), ordering.proposals.get(1));
        assertEquals(0, broker.cachedClientRequestCountForTesting());
    }

    @Test
    void lateAppliedProposalCleansUncertainCacheWithoutASecondRetry() {
        ScriptedOrderingService ordering = new ScriptedOrderingService(
                false, false, false);
        Broker broker = brokerWith(ordering);
        ClientMessage first = new ClientMessage(
                "alice", "client-a", 1L, "first");
        ClientMessage second = new ClientMessage(
                "alice", "client-a", 2L, "second");

        assertFalse(broker.onClientMessage(first));
        ChatReqMessage original = ordering.proposals.get(0);
        assertEquals(1, broker.cachedClientRequestCountForTesting());
        assertEquals(1, broker.getSendVectorClock().getTimeStamp(1));

        // Until apply makes the outcome definitive, a retry must preserve the
        // original local id and vector timestamp.
        assertFalse(broker.onClientMessage(first));
        assertSame(original, ordering.proposals.get(1));
        assertEquals(1, broker.getSendVectorClock().getTimeStamp(1));
        assertEquals(1, broker.cachedClientRequestCountForTesting());

        assertFalse(broker.onClientMessage(second));
        assertEquals(2, broker.cachedClientRequestCountForTesting());

        broker.handleOrderedMessage(new ChatDeliverMessage(
                1L,
                original.getBrokerId(),
                original.getUsername(),
                original.getClientId(),
                original.getClientSeq(),
                original.getText(),
                new VectorClock(original.getVectorClock())
        ));

        assertEquals(1, broker.cachedClientRequestCountForTesting(),
                "late apply must clean only its completed proposal");

        // Duplicate apply notification is harmless and cannot affect another
        // proposal's still-uncertain metadata.
        broker.handleOrderedMessage(new ChatDeliverMessage(
                1L,
                original.getBrokerId(),
                original.getUsername(),
                original.getClientId(),
                original.getClientSeq(),
                original.getText(),
                new VectorClock(original.getVectorClock())
        ));
        assertEquals(1, broker.cachedClientRequestCountForTesting());
    }

    private static Broker brokerWith(OrderingService orderingService) {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(orderingService);
        return broker;
    }

    private static final class ScriptedOrderingService implements OrderingService {
        private final Deque<Boolean> outcomes = new ArrayDeque<>();
        private final List<ChatReqMessage> proposals = new ArrayList<>();

        private ScriptedOrderingService(boolean... outcomes) {
            for (boolean outcome : outcomes) {
                this.outcomes.addLast(outcome);
            }
        }

        @Override
        public boolean propose(ChatReqMessage request) {
            proposals.add(request);
            if (outcomes.isEmpty()) {
                throw new AssertionError("No scripted proposal outcome remains");
            }
            return outcomes.removeFirst();
        }

        @Override
        public boolean establishDeliveryBoundary(String boundaryId) {
            return true;
        }

        @Override
        public void onDeliver(Consumer<ChatDeliverMessage> callback) {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public int getLeaderId() {
            return 1;
        }
    }
}
