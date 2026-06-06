package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.client.ClientMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class BrokerRetryVectorClockTest {

    private static final class CapturingOrderingService implements OrderingService {
        private final List<ChatReqMessage> proposals = new ArrayList<>();

        @Override
        public boolean propose(ChatReqMessage request) {
            proposals.add(request);
            return true;
        }

        @Override
        public void onDeliver(Consumer<ChatDeliverMessage> callback) {
            // No-op: this test only verifies proposal construction.
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
            return 0;
        }
    }

    private static final class TestBroker extends Broker {
        TestBroker(BrokerConfig config) {
            super(config);
        }
    }

    @Test
    public void retryWithSameTimestampReusesTheSameChatRequest() {
        // The broker should treat retries for the same logical client message as one proposal.
        // This keeps the send vector clock stable and prevents duplicate causal increments.
        TestBroker broker = new TestBroker(TestConfigs.raftBrokerConfig(1, 5000));
        CapturingOrderingService orderingService = new CapturingOrderingService();
        broker.setOrderingService(orderingService);

        long timestamp = 123456789L;
        ClientMessage message = new ClientMessage("alice", "hello", timestamp);

        broker.onClientMessage(message);
        broker.onClientMessage(message);

        assertEquals(2, orderingService.proposals.size());
        assertSame(orderingService.proposals.get(0), orderingService.proposals.get(1));

        VectorClock proposalClock = orderingService.proposals.get(0).getVectorClock();
        assertEquals(1, proposalClock.getTimeStamp(1));
        assertEquals(1, broker.getSendVectorClock().getTimeStamp(1));
    }
}