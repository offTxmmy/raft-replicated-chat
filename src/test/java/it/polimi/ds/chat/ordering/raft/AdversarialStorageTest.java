package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class AdversarialStorageTest {
    @Test
    void incompleteCommittedRecordIsNotSilentlyDiscarded(@TempDir Path dir) throws Exception {
        try (FileRaftPersistence storage = new FileRaftPersistence(dir)) {
            storage.appendLogEntry(new RaftLogEntry(1, 1, null));
            storage.appendLogEntry(new RaftLogEntry(2, 1, null));
            storage.persistCommitProgress(2, 2);
            Path log = dir.resolve("log.bin");
            try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "rw")) { file.setLength(file.length() - 1); }
            long damagedLength = Files.size(log);
            assertThrows(FileRaftPersistence.RaftPersistenceException.class, storage::loadLogEntries);
            assertEquals(damagedLength, Files.size(log), "recovery destroyed evidence from committed data");
        }
    }
    @Test
    void storageLockSurvivesOtherJvmAndIsReleasedByProcessDeath(@TempDir Path dir) throws Exception {
        String javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java.exe").toString();
        if (!Files.exists(Path.of(javaExecutable))) javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process owner = new ProcessBuilder(javaExecutable, "-cp", System.getProperty("java.class.path"),
                StorageOwner.class.getName(), dir.toString()).redirectErrorStream(true).start();
        try {
            var reader = new java.io.BufferedReader(new java.io.InputStreamReader(owner.getInputStream()));
            java.util.concurrent.CompletableFuture<String> ready = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try { return reader.readLine(); } catch (java.io.IOException e) { throw new RuntimeException(e); }
            });
            assertEquals("OWNED", ready.get(5, java.util.concurrent.TimeUnit.SECONDS));
            assertThrows(FileRaftPersistence.RaftPersistenceException.class, () -> new FileRaftPersistence(dir));
            owner.destroyForcibly();
            assertTrue(owner.waitFor(5, java.util.concurrent.TimeUnit.SECONDS));
            try (FileRaftPersistence replacement = new FileRaftPersistence(dir)) {
                assertEquals(7, replacement.loadTermAndVote().currentTerm());
            }
        } finally { owner.destroyForcibly(); owner.waitFor(5, java.util.concurrent.TimeUnit.SECONDS); }
    }

    public static class StorageOwner {
        public static void main(String[] args) throws Exception {
            try (FileRaftPersistence storage = new FileRaftPersistence(Path.of(args[0]))) {
                storage.persistTermAndVote(7, 1);
                System.out.println("OWNED");
                System.out.flush();
                System.in.read();
            }
        }
    }
    @Test
    void transientWindowsRenameRetriesSameAtomicOperation(@TempDir Path dir) throws Exception {
        Path source = dir.resolve("commit.bin.tmp");
        Path target = dir.resolve("commit.bin");
        Files.writeString(source, "new durable progress");
        Files.writeString(target, "old durable progress");
        AtomicInteger attempts = new AtomicInteger();
        FileRaftPersistence.atomicReplace(source, target, (from, to) -> {
            assertEquals("old durable progress", Files.readString(to));
            if (attempts.incrementAndGet() < 3) throw new AccessDeniedException(from.toString());
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        });
        assertEquals(3, attempts.get());
        assertEquals("new durable progress", Files.readString(target));
        assertFalse(Files.exists(source));
    }

    @Test
    void permanentRenameFailurePreservesOldFileAndSurfacesError(@TempDir Path dir) throws Exception {
        Path source = dir.resolve("state.bin.tmp");
        Path target = dir.resolve("state.bin");
        Files.writeString(source, "new");
        Files.writeString(target, "old");
        AtomicInteger attempts = new AtomicInteger();
        assertThrows(AccessDeniedException.class, () -> FileRaftPersistence.atomicReplace(source, target, (from, to) -> {
            attempts.incrementAndGet();
            throw new AccessDeniedException(from.toString());
        }));
        assertEquals(5, attempts.get());
        assertEquals("old", Files.readString(target));
        assertEquals("new", Files.readString(source));
    }

    @Test
    void everyDurableWriteFailureFencesReadsAndLaterWrites() {
        for (String operation : java.util.List.of("term", "append", "truncate", "commit")) {
            AtomicInteger failures = new AtomicInteger();
            FileRaftPersistence.RaftPersistenceException injected =
                    new FileRaftPersistence.RaftPersistenceException("injected " + operation, null);
            RaftPersistence delegate = new RaftPersistence() {
                public void persistTermAndVote(long term, Integer vote) { throw injected; }
                public void appendLogEntry(RaftLogEntry entry) { throw injected; }
                public void truncateLogFrom(long index) { throw injected; }
                public void persistCommitProgress(long commit, long applied) { throw injected; }
            };
            GuardedRaftPersistence guarded = new GuardedRaftPersistence(delegate, e -> failures.incrementAndGet());
            Runnable write = switch (operation) {
                case "term" -> () -> guarded.persistTermAndVote(1, 1);
                case "append" -> () -> guarded.appendLogEntry(new RaftLogEntry(1, 1, null));
                case "truncate" -> () -> guarded.truncateLogFrom(1);
                default -> () -> guarded.persistCommitProgress(1, 1);
            };
            assertSame(injected, assertThrows(FileRaftPersistence.RaftPersistenceException.class, write::run));
            assertSame(injected, assertThrows(FileRaftPersistence.RaftPersistenceException.class, guarded::loadTermAndVote));
            assertSame(injected, assertThrows(FileRaftPersistence.RaftPersistenceException.class,
                    () -> new RaftNode(1, guarded).handleRequestVote(new RequestVoteRequestMessage(0, 2, 0, 0), new RaftLog())));
            assertEquals(1, failures.get());
            guarded.close();
        }
    }
    // Works on the baseline too, where the storage has no ownership lifecycle.
    private static void closeStorage(Object storage) throws Exception {
        if (storage instanceof AutoCloseable closeable) closeable.close();
    }

    @Test
    void repairedTailSurvivesSecondAndThirdRestart(@TempDir Path dir) throws Exception {
        FileRaftPersistence storage = new FileRaftPersistence(dir);
        try {
            storage.appendLogEntry(new RaftLogEntry(1, 1, null));
            for (int cycle = 0; cycle < 3; cycle++) {
                closeStorage(storage);
                try (RandomAccessFile file = new RandomAccessFile(dir.resolve("log.bin").toFile(), "rw")) {
                    file.seek(file.length());
                    if (cycle == 1) file.writeByte(1); // partial length prefix
                    else { file.writeInt(9999); file.writeByte(1); }
                }
                storage = new FileRaftPersistence(dir);
                assertEquals(cycle + 1, storage.loadLogEntries().size());
                storage.appendLogEntry(new RaftLogEntry(cycle + 2, 2, null));
                closeStorage(storage);
                storage = new FileRaftPersistence(dir);
                assertEquals(cycle + 2, storage.loadLogEntries().size(), "valid append lost after another restart");
            }
        } finally { closeStorage(storage); }
    }

    @Test
    void secondOwnerIsRejectedUntilFirstCloses(@TempDir Path dir) throws Exception {
        FileRaftPersistence first = new FileRaftPersistence(dir);
        try {
            assertThrows(FileRaftPersistence.RaftPersistenceException.class, () -> {
                FileRaftPersistence second = new FileRaftPersistence(dir.resolve("."));
                closeStorage(second);
            });
        } finally { closeStorage(first); }
        closeStorage(new FileRaftPersistence(dir));
    }

    @Test
    void failedVoteCannotBecomeSuccessfulOnDuplicateRpc() {
        AtomicInteger writes = new AtomicInteger();
        RaftNode node = new RaftNode(1, (term, vote) -> {
            writes.incrementAndGet();
            throw new FileRaftPersistence.RaftPersistenceException("injected vote write failure", null);
        }, 4, null);
        RequestVoteRequestMessage request = new RequestVoteRequestMessage(4, 2, 0, 0);
        assertThrows(FileRaftPersistence.RaftPersistenceException.class,
                () -> node.handleRequestVote(request, new RaftLog()));
        assertThrows(FileRaftPersistence.RaftPersistenceException.class,
                () -> node.handleRequestVote(request, new RaftLog()), "duplicate must not expose an unpersisted vote");
        assertEquals(1, writes.get(), "a failed node must remain fenced");
    }
}
