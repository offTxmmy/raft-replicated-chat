package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftStateMachineAdapterTest {

    @Test
    void noOpDoesNotConsumeApplicationSequenceNumber() {
        List<ChatDeliverMessage> deliveredMessages = new ArrayList<>();

        RaftStateMachineAdapter adapter = new RaftStateMachineAdapter(deliveredMessages::add);

        // Raft index 1 is an internal leader no-op.
        RaftLogEntry noOpEntry = new RaftLogEntry(1L, 1L, null);

        // The first real chat command is stored at Raft index 2.
        ChatCommand chatCommand = new ChatCommand(
                "msg-1",
                1,
                "alice",
                "hello",
                new VectorClock(),
                "client-1",
                1L);

        RaftLogEntry chatEntry = new RaftLogEntry(2L, 1L, chatCommand);

        adapter.accept(noOpEntry);
        adapter.accept(chatEntry);

        assertEquals(1, deliveredMessages.size());

        ChatDeliverMessage delivered = deliveredMessages.get(0);

        assertEquals(1L, delivered.getSeq());
        assertEquals("hello", delivered.getText());
        assertTrue(delivered.hasClientIdentity());
        assertEquals("client-1", delivered.getClientId());
        assertEquals(1L, delivered.getClientSeq());
    }

    @Test
    void noOpsBetweenChatCommandsDoNotCreateSequenceGaps() {
        List<ChatDeliverMessage> deliveredMessages = new ArrayList<>();

        RaftStateMachineAdapter adapter = new RaftStateMachineAdapter(deliveredMessages::add);

        ChatCommand firstCommand = new ChatCommand(
                "msg-1",
                1,
                "alice",
                "first",
                new VectorClock(),
                "client-1",
                1L);

        ChatCommand secondCommand = new ChatCommand(
                "msg-2",
                2,
                "bob",
                "second",
                new VectorClock(),
                "client-2",
                1L);

        ChatCommand thirdCommand = new ChatCommand(
                "msg-3",
                1,
                "alice",
                "third",
                new VectorClock(),
                "client-1",
                2L);

        // First leader term:
        adapter.accept(new RaftLogEntry(1L, 1L, null));
        adapter.accept(new RaftLogEntry(2L, 1L, firstCommand));
        adapter.accept(new RaftLogEntry(3L, 1L, secondCommand));

        // New leader in a later term appends another internal no-op.
        adapter.accept(new RaftLogEntry(4L, 2L, null));

        // First chat committed after the leader change.
        adapter.accept(new RaftLogEntry(5L, 2L, thirdCommand));

        assertEquals(3, deliveredMessages.size());

        assertEquals(1L, deliveredMessages.get(0).getSeq());
        assertEquals("first", deliveredMessages.get(0).getText());

        assertEquals(2L, deliveredMessages.get(1).getSeq());
        assertEquals("second", deliveredMessages.get(1).getText());

        assertEquals(3L, deliveredMessages.get(2).getSeq());
        assertEquals("third", deliveredMessages.get(2).getText());
    }

    @Test
    void noOpDoesNotCreateGapInHoldBackQueue() {
        List<ChatDeliverMessage> deliveredToClient = new ArrayList<>();
        it.polimi.ds.chat.common.delivery.HoldBackQueue holdBackQueue = new it.polimi.ds.chat.common.delivery.HoldBackQueue();

        RaftStateMachineAdapter adapter = new RaftStateMachineAdapter(
                message -> deliveredToClient.addAll(holdBackQueue.enqueue(message)));

        ChatCommand firstCommand = new ChatCommand(
                "msg-1",
                1,
                "alice",
                "hello",
                new VectorClock(),
                "client-1",
                1L);

        // Internal Raft entry at index 1.
        adapter.accept(new RaftLogEntry(1L, 1L, null));

        // First real chat is at Raft index 2.
        adapter.accept(new RaftLogEntry(2L, 1L, firstCommand));

        assertEquals(1, deliveredToClient.size());
        assertEquals(1L, deliveredToClient.get(0).getSeq());
        assertEquals("hello", deliveredToClient.get(0).getText());

        assertEquals(2L, holdBackQueue.getExpectedSeq());
        assertEquals(0, holdBackQueue.getPendingCount());
    }

    @Test
    void joinDeliveryBarrierIsAppliedInternallyWithoutConsumingChatSequence() {
        List<ChatDeliverMessage> deliveredMessages = new ArrayList<>();
        RaftStateMachineAdapter adapter = new RaftStateMachineAdapter(deliveredMessages::add);

        adapter.accept(new RaftLogEntry(
                1L,
                1L,
                ChatCommand.deliveryBarrier("join-barrier:session-1", 1)
        ));
        adapter.accept(new RaftLogEntry(
                2L,
                1L,
                new ChatCommand(
                        "msg-1",
                        1,
                        "alice",
                        "after join",
                        new VectorClock(),
                        "client-1",
                        1L
                )
        ));

        assertEquals(1, deliveredMessages.size());
        assertEquals(1L, deliveredMessages.get(0).getSeq());
        assertEquals("after join", deliveredMessages.get(0).getText());
    }
}
