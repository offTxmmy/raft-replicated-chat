package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * File-based implementation of {@link RaftPersistence}.
 *
 * <p>Layout under {@code storageDir}:
 * <ul>
 *   <li>{@code state.bin} — (currentTerm, votedFor) written via tmp-file + fsync + atomic rename.
 *   <li>{@code commit.bin} — (commitIndex, lastApplied) written via tmp-file + fsync + atomic rename.
 *   <li>{@code log.bin}   — append-only, length-prefixed serialized {@link RaftLogEntry} records.
 * </ul>
 *
 * <p>Crash safety:
 * <ul>
 *   <li>State writes go to {@code state.bin.tmp} first, are fsynced, then atomically renamed.
 *       The storage directory is fsynced after the rename when the platform supports it.
 *       A crash mid-write leaves the previous {@code state.bin} intact.
 *   <li>Commit-progress writes follow the same tmp-file + fsync + atomic rename strategy,
 *       so {@code commit.bin} is either old or new, never partially updated.
 *   <li>Log truncation rewrites through {@code log.bin.tmp}, fsyncs the tmp file,
 *       atomically renames it, and fsyncs the storage directory when supported.
 *   <li>Log appends are fsynced after each write. A crash during a partial append leaves a
 *       truncated tail, which {@link #loadLogEntries()} silently drops.
 * </ul>

 * <p>The persisted commit progress is used only for local restart recovery
 * ({@code commitIndex}/{@code lastApplied}); Raft consensus for new commits
 * still follows leader/quorum rules at runtime.
 *
 * <p>Thread safety: all public methods are {@code synchronized} on the instance.
 */
public final class FileRaftPersistence implements RaftPersistence {

    private static final String STATE_FILE = "state.bin";
    private static final String STATE_TMP  = "state.bin.tmp";
    private static final String COMMIT_FILE = "commit.bin";
    private static final String COMMIT_TMP  = "commit.bin.tmp";
    private static final String LOG_FILE   = "log.bin";
    private static final String LOG_TMP    = "log.bin.tmp";

    private final Path stateFile;
    private final Path stateTmp;
    private final Path commitFile;
    private final Path commitTmp;
    private final Path logFile;
    private final Path logTmp;

    public FileRaftPersistence(Path storageDir) {
        try {
            Files.createDirectories(storageDir);
        } catch (IOException e) {
            throw new RaftPersistenceException("Cannot create storage dir " + storageDir, e);
        }
        this.stateFile = storageDir.resolve(STATE_FILE);
        this.stateTmp  = storageDir.resolve(STATE_TMP);
        this.commitFile = storageDir.resolve(COMMIT_FILE);
        this.commitTmp  = storageDir.resolve(COMMIT_TMP);
        this.logFile   = storageDir.resolve(LOG_FILE);
        this.logTmp    = storageDir.resolve(LOG_TMP);

        if (!Files.exists(logFile)) {
            try {
                Files.createFile(logFile);
            } catch (IOException e) {
                throw new RaftPersistenceException("Cannot create log file " + logFile, e);
            }
        }
    }

    // --- Term / vote -------------------------------------------------------

    @Override
    public synchronized void persistTermAndVote(long currentTerm, Integer votedFor) {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(stateTmp,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)))) {
            out.writeLong(currentTerm);
            out.writeBoolean(votedFor != null);
            if (votedFor != null) {
                out.writeInt(votedFor);
            }
            out.flush();
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to write state tmp", e);
        }
        fsync(stateTmp);
        try {
            Files.move(stateTmp, stateFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to rename state file", e);
        }
        fsyncParentDirectory(stateFile);
    }

    @Override
    public synchronized PersistedState loadTermAndVote() {
        if (!Files.exists(stateFile)) {
            return PersistedState.EMPTY;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(stateFile)))) {
            long term = in.readLong();
            boolean hasVote = in.readBoolean();
            Integer votedFor = hasVote ? in.readInt() : null;
            return new PersistedState(term, votedFor);
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to read state file", e);
        }
    }

    // --- Commit progress (commitIndex / lastApplied) ----------------------
    @Override
    public synchronized void persistCommitProgress(long commitIndex, long lastApplied) {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(commitTmp,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)))) {
            out.writeLong(commitIndex);
            out.writeLong(lastApplied);
            out.flush();
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to write commit progress tmp", e);
        }
        fsync(commitTmp);
        try {
            Files.move(commitTmp, commitFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to rename commit progress file", e);
        }
        fsyncParentDirectory(commitFile);
    }

    @Override
    public synchronized CommitProgress loadCommitProgress() {
        if (!Files.exists(commitFile)) {
            return CommitProgress.EMPTY;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(commitFile)))) {
            long commitIndex = in.readLong();
            long lastApplied = in.readLong();
            return new CommitProgress(commitIndex, lastApplied);
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to read commit progress file", e);
        }
    }

    // --- Log ---------------------------------------------------------------

    @Override
    public synchronized void appendLogEntry(RaftLogEntry entry) {
        byte[] payload = serialize(entry);
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(logFile,
                        StandardOpenOption.WRITE, StandardOpenOption.APPEND)))) {
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to append log entry", e);
        }
        fsync(logFile);
    }

    @Override
    public synchronized void truncateLogFrom(long fromIndex) {
        if (fromIndex <= 0L) {
            return;
        }
        List<RaftLogEntry> kept = new ArrayList<>();
        for (RaftLogEntry e : loadLogEntries()) {
            if (e.getIndex() >= fromIndex) {
                break;
            }
            kept.add(e);
        }
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(logTmp,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)))) {
            for (RaftLogEntry e : kept) {
                byte[] payload = serialize(e);
                out.writeInt(payload.length);
                out.write(payload);
            }
            out.flush();
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to write log tmp", e);
        }
        fsync(logTmp);
        try {
            Files.move(logTmp, logFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to rename log file", e);
        }
        fsyncParentDirectory(logFile);
    }

    @Override
    public synchronized List<RaftLogEntry> loadLogEntries() {
        List<RaftLogEntry> entries = new ArrayList<>();
        if (!Files.exists(logFile)) {
            return entries;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(logFile)))) {
            while (true) {
                int len;
                try {
                    len = in.readInt();
                } catch (EOFException eof) {
                    break;
                }
                if (len <= 0) {
                    break; // corrupted tail
                }
                byte[] payload = in.readNBytes(len);
                if (payload.length < len) {
                    break; // truncated tail from a crash mid-append: drop silently
                }
                entries.add(deserialize(payload));
            }
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to read log file", e);
        }
        return entries;
    }

    // --- Helpers -----------------------------------------------------------

    private static byte[] serialize(RaftLogEntry entry) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(entry);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to serialize log entry", e);
        }
    }

    private static RaftLogEntry deserialize(byte[] payload) {
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(payload))) {
            return (RaftLogEntry) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RaftPersistenceException("Failed to deserialize log entry", e);
        }
    }

    private static void fsync(Path path) {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.force(true);
        } catch (IOException e) {
            throw new RaftPersistenceException("Failed to fsync " + path, e);
        }
    }

    private static void fsyncParentDirectory(Path path) {
        Path parent = path.toAbsolutePath().getParent();
        if (parent == null) {
            return;
        }
        try (FileChannel ch = FileChannel.open(parent, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Some platforms, including common Windows configurations, do not
            // expose directory fsync through FileChannel.
        }
    }

    /**
     * Unchecked exception wrapping any I/O failure in the persistence layer.
     * Persistence failures are not recoverable inside the Raft state machine.
     */
    public static final class RaftPersistenceException extends RuntimeException {
        public RaftPersistenceException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
