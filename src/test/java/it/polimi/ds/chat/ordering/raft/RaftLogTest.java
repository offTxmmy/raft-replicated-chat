package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.ChatCommand;
import it.polimi.ds.chat.messages.raft.RaftLogEntry;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @Test
    void emptyLogShouldReturnZeroMetadata() {
        RaftLog log = new RaftLog();

        assertEquals(0L, log.lastLogIndex());
        assertEquals(0L, log.lastLogTerm());

        RaftLogMetadata snapshot = log.snapshotMetadata();
        assertEquals(0L, snapshot.lastLogIndex());
        assertEquals(0L, snapshot.lastLogTerm());
    }

    @Test
    void appendShouldIncrementIndexAndStoreCommand() {
        RaftLog log = new RaftLog();

        RaftLogEntry entry = log.append(1L, command("1"));

        assertEquals(1L, entry.getIndex());
        assertEquals(1L, entry.getTerm());
        assertEquals(1L, log.lastLogIndex());
        assertEquals(1L, log.lastLogTerm());
        assertEquals("1", entry.getCommand().getLocalMsgId());
    }

    @Test
    void matchesShouldValidateIndexAndTerm() {
        RaftLog log = new RaftLog();
        log.append(1L, command("1"));

        assertTrue(log.matches(0L, 0L));
        assertTrue(log.matches(1L, 1L));
        assertFalse(log.matches(1L, 2L));
        assertFalse(log.matches(2L, 1L));
    }

    @Test
    void appendEntriesShouldTruncateOnConflict() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));

        List<RaftLogEntry> incoming = List.of(
                new RaftLogEntry(2L, 2L, command("c")),
                new RaftLogEntry(3L, 2L, command("d"))
        );

        boolean applied = log.appendEntries(1L, 1L, incoming);

        assertTrue(applied);
        assertEquals(3L, log.lastLogIndex());
        assertEquals(2L, log.getTermAt(2L));
        assertEquals("c", log.getEntry(2L).getCommand().getLocalMsgId());
    }

    @Test
    void appendEntriesShouldRejectWhenPrevDoesNotMatch() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));

        List<RaftLogEntry> incoming = List.of(
                new RaftLogEntry(2L, 1L, command("b"))
        );

        assertFalse(log.appendEntries(1L, 2L, incoming));
        assertEquals(1L, log.lastLogIndex());
    }

    @Test
    void snapshotMetadataShouldRemainConsistentAfterAppend() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));

        RaftLogMetadata snapshot = log.snapshotMetadata();
        log.append(2L, command("b"));

        assertEquals(1L, snapshot.lastLogIndex());
        assertEquals(1L, snapshot.lastLogTerm());
        assertEquals(2L, log.lastLogIndex());
        assertEquals(2L, log.lastLogTerm());
    }

    private ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }
}
