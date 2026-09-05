package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import static it.polimi.ds.chat.ordering.raft.FileRaftPersistence.RaftPersistenceException;

/** One sticky failure fence shared by the node, log and commit/application path. */
final class GuardedRaftPersistence implements RaftPersistence {
    private final RaftPersistence delegate;
    private final Consumer<RaftPersistenceException> onFailure;
    private volatile RaftPersistenceException failure;
    private volatile boolean closed;

    GuardedRaftPersistence(RaftPersistence delegate, Consumer<RaftPersistenceException> onFailure) {
        this.delegate = delegate;
        this.onFailure = onFailure;
    }

    @Override public void checkHealthy() {
        if (failure != null) throw failure;
        if (closed) throw new RaftPersistenceException("Raft storage is closed", null);
    }

    private synchronized <T> T access(Supplier<T> operation) {
        checkHealthy();
        try {
            return operation.get();
        } catch (RuntimeException cause) {
            failure = cause instanceof RaftPersistenceException storageFailure ? storageFailure
                    : new RaftPersistenceException("Durable Raft operation failed", cause);
            // This callback only signals/schedules teardown; it must never take Raft locks.
            onFailure.accept(failure);
            throw failure;
        }
    }

    @Override public void persistTermAndVote(long term, Integer vote) {
        access(() -> { delegate.persistTermAndVote(term, vote); return null; });
    }
    @Override public PersistedState loadTermAndVote() { return access(delegate::loadTermAndVote); }
    @Override public void appendLogEntry(RaftLogEntry entry) {
        access(() -> { delegate.appendLogEntry(entry); return null; });
    }
    @Override public void truncateLogFrom(long index) {
        access(() -> { delegate.truncateLogFrom(index); return null; });
    }
    @Override public List<RaftLogEntry> loadLogEntries() { return access(delegate::loadLogEntries); }
    @Override public void persistCommitProgress(long commit, long applied) {
        access(() -> { delegate.persistCommitProgress(commit, applied); return null; });
    }
    @Override public CommitProgress loadCommitProgress() { return access(delegate::loadCommitProgress); }
    @Override public synchronized void close() {
        closed = true;
        delegate.close();
    }
}
