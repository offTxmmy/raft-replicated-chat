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

    private static final class RecordingBroker extends Broker {
        private final List<String> deliveredMessages = new ArrayList<>();

        RecordingBroker(BrokerConfig config) {
            super(config);
        }

        @Override
        public void onChatDeliver(long seq, String sender, String text) {
            deliveredMessages.add(seq + ":" + sender + ":" + text);
        }

        List<String> deliveredMessages() {
            return deliveredMessages;
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

    @Test
    public void differentMessagesFromSameBrokerShouldNotBlockWhenRaftOrdersThemDifferently() {
        RecordingBroker broker = new RecordingBroker(TestConfigs.raftBrokerConfig(1, 5000));
        CapturingOrderingService orderingService = new CapturingOrderingService();
        broker.setOrderingService(orderingService);

        broker.onClientMessage(new ClientMessage("alice", "first", 100L));
        broker.onClientMessage(new ClientMessage("bob", "second", 101L));

        assertEquals(2, orderingService.proposals.size());

        ChatReqMessage firstProposal = orderingService.proposals.get(0);
        ChatReqMessage secondProposal = orderingService.proposals.get(1);
        assertEquals(1, firstProposal.getVectorClock().getTimeStamp(1));
        assertEquals(2, secondProposal.getVectorClock().getTimeStamp(1));

        broker.handleOrderedMessage(toDeliver(1L, secondProposal));
        broker.handleOrderedMessage(toDeliver(2L, firstProposal));

        assertEquals(
                List.of("1:bob:second", "2:alice:first"),
                broker.deliveredMessages());
    }

    private static ChatDeliverMessage toDeliver(long seq, ChatReqMessage request) {
        return new ChatDeliverMessage(
                seq,
                request.getBrokerId(),
                request.getUsername(),
                request.getText(),
                new VectorClock(request.getVectorClock()));
    }
}
