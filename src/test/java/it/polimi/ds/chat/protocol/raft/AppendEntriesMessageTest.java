package it.polimi.ds.chat.protocol.raft;

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
        AppendEntriesResponseMessage response = new AppendEntriesResponseMessage(4L, true, 2, 7L, -1L, 0L);

        assertEquals(4L, response.getTerm());
        assertTrue(response.isSuccess());
        assertEquals(2, response.getResponderId());
        assertEquals(7L, response.getMatchIndex());
        assertEquals(-1L, response.getConflictTerm());
        assertEquals(0L, response.getConflictIndex());
    }

    private ChatCommand command(String clientId) {
        return new ChatCommand("alice", clientId, 1L, "msg-" + clientId);
    }
}
