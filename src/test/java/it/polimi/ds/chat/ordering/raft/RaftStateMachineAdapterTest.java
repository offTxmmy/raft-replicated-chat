package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftStateMachineAdapterTest {

    @Test
    void mapsACommittedCommandUsingItsRaftLogIndex() {
        List<ChatDeliverMessage> delivered = new ArrayList<>();
        RaftStateMachineAdapter adapter = new RaftStateMachineAdapter(delivered::add);

        adapter.accept(new RaftLogEntry(7L, 3L,
                new ChatCommand("alice", "client-a", 4L, "hello")));

        assertEquals(1, delivered.size());
        ChatDeliverMessage message = delivered.get(0);
        assertEquals(7L, message.getSeq());
        assertEquals("alice", message.getUsername());
        assertEquals("client-a", message.getClientId());
        assertEquals(4L, message.getClientSeq());
        assertEquals("hello", message.getText());
    }

    @Test
    void preservesNonContiguousVisibleRaftIndexesAcrossInternalNoOp() {
        List<ChatDeliverMessage> delivered = new ArrayList<>();
        RaftStateMachineAdapter adapter = new RaftStateMachineAdapter(delivered::add);

        adapter.accept(new RaftLogEntry(10L, 1L,
                new ChatCommand("alice", "client-a", 1L, "before no-op")));
        adapter.accept(new RaftLogEntry(11L, 2L, null));
        adapter.accept(new RaftLogEntry(12L, 2L,
                new ChatCommand("bob", "client-b", 1L, "after no-op")));

        assertEquals(List.of(10L, 12L),
                delivered.stream().map(ChatDeliverMessage::getSeq).toList());
    }
}
