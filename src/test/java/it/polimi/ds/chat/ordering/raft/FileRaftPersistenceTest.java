package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FileRaftPersistence}: round-trip of term/vote and log,
 * crash simulation via fresh instances on the same directory, atomic rename,
 * and tolerance to a truncated log tail.
 */
class FileRaftPersistenceTest {
    private final java.util.List<FileRaftPersistence> owned = new java.util.ArrayList<>();
    private FileRaftPersistence open(Path dir) {
        FileRaftPersistence storage = new FileRaftPersistence(dir);
        owned.add(storage);
        return storage;
    }
    @org.junit.jupiter.api.AfterEach
    void closeOwnedStorage() { owned.forEach(FileRaftPersistence::close); }


    @Test
    void emptyDirectoryLoadsDefaults(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);

        RaftPersistence.PersistedState state = p.loadTermAndVote();
        assertEquals(0L, state.currentTerm());
        assertNull(state.votedFor());
        assertTrue(p.loadLogEntries().isEmpty());
    }

    @Test
    void persistAndReloadTermAndVote(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.persistTermAndVote(7L, 3);

        // Crash + restart: fresh instance on the same directory.
        p.close();
        FileRaftPersistence restored = open(dir);
        RaftPersistence.PersistedState state = restored.loadTermAndVote();
        assertEquals(7L, state.currentTerm());
        assertEquals(Integer.valueOf(3), state.votedFor());
    }

    @Test
    void persistTermWithoutVoteRoundTrips(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.persistTermAndVote(12L, null);

        p.close();
        FileRaftPersistence restored = open(dir);
        RaftPersistence.PersistedState state = restored.loadTermAndVote();
        assertEquals(12L, state.currentTerm());
        assertNull(state.votedFor());
    }

    @Test
    void persistAndReloadCommitProgress(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.persistCommitProgress(9L, 7L);

        p.close();
        FileRaftPersistence restored = open(dir);
        RaftPersistence.CommitProgress progress = restored.loadCommitProgress();

        assertEquals(9L, progress.commitIndex());
        assertEquals(7L, progress.lastApplied());
    }

    @Test
    void overwriteKeepsLatestValue(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.persistTermAndVote(1L, 1);
        p.persistTermAndVote(2L, 2);
        p.persistTermAndVote(3L, null);

        RaftPersistence.PersistedState state = p.loadTermAndVote();
        assertEquals(3L, state.currentTerm());
        assertNull(state.votedFor());
    }

    @Test
    void appendAndLoadLogEntries(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.appendLogEntry(entry(1L, 1L, "a"));
        p.appendLogEntry(entry(2L, 1L, "b"));
        p.appendLogEntry(entry(3L, 2L, "c"));

        p.close();
        FileRaftPersistence restored = open(dir);
        List<RaftLogEntry> entries = restored.loadLogEntries();

        assertEquals(3, entries.size());
        assertEquals(1L, entries.get(0).getIndex());
        assertEquals("a", entries.get(0).getCommand().getText());
        assertEquals(2L, entries.get(2).getTerm());
        assertEquals("c", entries.get(2).getCommand().getText());
    }

    @Test
    void truncateRemovesTail(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.appendLogEntry(entry(1L, 1L, "a"));
        p.appendLogEntry(entry(2L, 1L, "b"));
        p.appendLogEntry(entry(3L, 2L, "c"));

        p.truncateLogFrom(2L);

        List<RaftLogEntry> entries = p.loadLogEntries();
        assertEquals(1, entries.size());
        assertEquals(1L, entries.get(0).getIndex());

        // Survives a restart.
        p.close();
        FileRaftPersistence restored = open(dir);
        List<RaftLogEntry> reloaded = restored.loadLogEntries();
        assertEquals(1, reloaded.size());
        assertEquals(1L, reloaded.get(0).getIndex());
    }

    @Test
    void truncateFromZeroIsNoOp(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.appendLogEntry(entry(1L, 1L, "a"));
        p.truncateLogFrom(0L);
        assertEquals(1, p.loadLogEntries().size());
    }

    @Test
    void truncateBeyondTailKeepsEverything(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.appendLogEntry(entry(1L, 1L, "a"));
        p.appendLogEntry(entry(2L, 1L, "b"));

        p.truncateLogFrom(99L);

        assertEquals(2, p.loadLogEntries().size());
    }

    @Test
    void truncatedTailFromPartialAppendIsDroppedSilently(@TempDir Path dir) throws IOException {
        FileRaftPersistence p = open(dir);
        p.appendLogEntry(entry(1L, 1L, "a"));
        p.appendLogEntry(entry(2L, 1L, "b"));

        // Simulate a crash mid-append by corrupting the tail of log.bin:
        // write a bogus length prefix without the matching payload.
        Path logFile = dir.resolve("log.bin");
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(9999); // length prefix with no payload
        }

        p.close();
        FileRaftPersistence restored = open(dir);
        List<RaftLogEntry> entries = restored.loadLogEntries();
        assertEquals(2, entries.size());
    }

    @Test
    void noLeftoverTmpFilesAfterPersistAndTruncate(@TempDir Path dir) {
        FileRaftPersistence p = open(dir);
        p.persistTermAndVote(5L, 2);
        p.persistCommitProgress(5L, 4L);
        p.appendLogEntry(entry(1L, 1L, "a"));
        p.appendLogEntry(entry(2L, 1L, "b"));
        p.truncateLogFrom(2L);

        assertTrue(Files.exists(dir.resolve("state.bin")));
        assertTrue(Files.exists(dir.resolve("commit.bin")));
        assertTrue(Files.exists(dir.resolve("log.bin")));
        assertTrue(Files.notExists(dir.resolve("state.bin.tmp")));
        assertTrue(Files.notExists(dir.resolve("commit.bin.tmp")));
        assertTrue(Files.notExists(dir.resolve("log.bin.tmp")));
    }

    // --- helpers ----------------------------------------------------------

    private static RaftLogEntry entry(long index, long term, String text) {
        ChatCommand cmd = new ChatCommand(
                "msg-" + index,
                1,
                "alice",
                text,
                new VectorClock());
        return new RaftLogEntry(index, term, cmd);
    }
}