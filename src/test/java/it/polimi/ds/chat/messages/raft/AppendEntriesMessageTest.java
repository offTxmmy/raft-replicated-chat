package it.polimi.ds.chat.messages.raft;

import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AppendEntriesMessageTest {

    @Test
    void requestShouldDefensivelyCopyEntries() {
        List<RaftLogEntry> entries = new ArrayList<>();
        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                3L,
                1,
                2L,
                2L,
                entries,
                1L
        );

        entries.add(new RaftLogEntry(3L, 3L, command("x")));

        assertTrue(request.getEntries().isEmpty());
        assertThrows(UnsupportedOperationException.class,
                () -> request.getEntries().add(new RaftLogEntry(3L, 3L, command("y"))));
    }

    @Test
    void responseShouldExposeFields() {
        AppendEntriesResponseMessage response = new AppendEntriesResponseMessage(4L, true, 2, 7L);

        assertEquals(4L, response.getTerm());
        assertTrue(response.isSuccess());
        assertEquals(2, response.getResponderId());
        assertEquals(7L, response.getMatchIndex());
    }

    private ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }
}
