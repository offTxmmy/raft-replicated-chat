package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;

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
        assertEquals("1", entry.getCommand().getClientId());
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
        assertEquals("c", log.getEntry(2L).getCommand().getClientId());
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

    @Test
    void appendShouldPersistEntryBeforeStoringInMemory() {
        RecordingPersistence persistence = new RecordingPersistence();
        RaftLog log = new RaftLog(persistence);

        RaftLogEntry entry = log.append(3L, command("persisted"));

        assertEquals(1, persistence.appended.size());
        assertEquals(entry, persistence.appended.get(0));
        assertEquals(entry, log.getEntry(1L));
    }

    @Test
    void appendEntriesShouldPersistOnlyNewEntries() {
        RecordingPersistence persistence = new RecordingPersistence();
        RaftLog log = new RaftLog(persistence);

        log.append(1L, command("a"));
        persistence.appended.clear();

        boolean applied = log.appendEntries(1L, 1L, List.of(
                new RaftLogEntry(2L, 2L, command("b")),
                new RaftLogEntry(3L, 2L, command("c"))
        ));

        assertTrue(applied);
        assertEquals(2, persistence.appended.size());
        assertEquals(2L, persistence.appended.get(0).getIndex());
        assertEquals(3L, persistence.appended.get(1).getIndex());
        assertEquals(3L, log.lastLogIndex());
    }

    @Test
    void appendEntriesShouldTruncatePersistedTailBeforeAppendingConflictReplacement() {
        RecordingPersistence persistence = new RecordingPersistence();
        RaftLog log = new RaftLog(persistence);

        log.append(1L, command("a"));
        log.append(1L, command("b"));
        persistence.appended.clear();

        boolean applied = log.appendEntries(1L, 1L, List.of(
                new RaftLogEntry(2L, 2L, command("replacement")),
                new RaftLogEntry(3L, 2L, command("new-tail"))
        ));

        assertTrue(applied);
        assertEquals(List.of(2L), persistence.truncatedFrom);
        assertEquals(2, persistence.appended.size());
        assertEquals("replacement", log.getEntry(2L).getCommand().getClientId());
        assertEquals("new-tail", log.getEntry(3L).getCommand().getClientId());
    }

    @Test
    void loadFromPersistenceShouldRestoreLogWithoutRewritingEntries() {
        RecordingPersistence persistence = new RecordingPersistence();
        RaftLog log = new RaftLog(persistence);

        log.loadFromPersistence(List.of(
                new RaftLogEntry(1L, 1L, command("a")),
                new RaftLogEntry(2L, 3L, command("b"))
        ));

        assertEquals(2L, log.lastLogIndex());
        assertEquals(3L, log.lastLogTerm());
        assertEquals("a", log.getEntry(1L).getCommand().getClientId());
        assertEquals("b", log.getEntry(2L).getCommand().getClientId());
        assertTrue(persistence.appended.isEmpty());
        assertTrue(persistence.truncatedFrom.isEmpty());
    }

    @Test
    void loadFromPersistenceShouldRejectNonContiguousEntries() {
        RaftLog log = new RaftLog(new RecordingPersistence());

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                log.loadFromPersistence(List.of(
                        new RaftLogEntry(1L, 1L, command("a")),
                        new RaftLogEntry(3L, 1L, command("c"))
                ))
        );

        assertTrue(ex.getMessage().contains("not contiguous"));
    }

    private ChatCommand command(String clientId) {
        return new ChatCommand("alice", clientId, 1L, "msg-" + clientId);
    }

    private static final class RecordingPersistence implements RaftPersistence {
        private final List<RaftLogEntry> appended = new ArrayList<>();
        private final List<Long> truncatedFrom = new ArrayList<>();

        @Override
        public void persistTermAndVote(long currentTerm, Integer votedFor) {
            // Not used by RaftLog tests.
        }

        @Override
        public void appendLogEntry(RaftLogEntry entry) {
            appended.add(entry);
        }

        @Override
        public void truncateLogFrom(long fromIndex) {
            truncatedFrom.add(fromIndex);
        }
    }
}
