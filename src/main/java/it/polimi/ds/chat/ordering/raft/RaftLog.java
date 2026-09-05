package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * In-memory Raft log implementation.
 *
 * This class owns log entry storage and supports append and conflict handling.
 * A consistent snapshot of the log tip is available via snapshotMetadata().
 */
public class RaftLog implements RaftLogMetadata {

    private final List<RaftLogEntry> entries = new ArrayList<>();
    private final RaftPersistence persistence;

    private Consumer<RaftLogEntry> truncationHook = entry -> {};

    private LongSupplier commitIndexSupplier = () -> 0L;

    public RaftLog() {
        this(RaftPersistence.NO_OP);
    }

    public RaftLog(RaftPersistence persistence) {
        this.persistence = persistence;
    }

    /**
     * Registers a hook called once per entry removed by {@link #truncateFrom(long)}.
     * Passing {@code null} restores the default no-op hook.
     */
    public synchronized void setTruncationHook(Consumer<RaftLogEntry> hook) {
        this.truncationHook = (hook != null) ? hook : entry -> {};
    }

    /**
     * Registers a supplier used by {@link #truncateFrom(long)} to enforce the
     * Raft invariant that committed entries are never truncated.
     * Passing {@code null} restores the default supplier returning 0.
     */
    public synchronized void setCommitIndexSupplier(LongSupplier supplier) {
        this.commitIndexSupplier = (supplier != null) ? supplier : () -> 0L;
    }

    @Override
    public synchronized long lastLogIndex() {
        if (entries.isEmpty()) {
            return 0L;
        }
        return entries.get(entries.size() - 1).getIndex();
    }

    @Override
    public synchronized long lastLogTerm() {
        if (entries.isEmpty()) {
            return 0L;
        }
        return entries.get(entries.size() - 1).getTerm();
    }

    /**
     * Returns a RaftLogMetadata view backed by a single log tip snapshot.
     * Use this when passing metadata to the election layer.
     */
    public synchronized RaftLogMetadata snapshotMetadata() {
        final long snapshotIndex;
        final long snapshotTerm;

        if (entries.isEmpty()) {
            snapshotIndex = 0L;
            snapshotTerm = 0L;
        } else {
            RaftLogEntry last = entries.get(entries.size() - 1);
            snapshotIndex = last.getIndex();
            snapshotTerm = last.getTerm();
        }

        return new RaftLogMetadata() {
            @Override
            public long lastLogIndex() {
                return snapshotIndex;
            }

            @Override
            public long lastLogTerm() {
                return snapshotTerm;
            }
        };
    }

    public synchronized long getTermAt(long index) {
        if (index == 0L) {
            return 0L;
        }
        if (index < 0L || index > lastLogIndex()) {
            throw new IllegalArgumentException("Invalid log index: " + index);
        }
        return entries.get((int) index - 1).getTerm();
    }

    public synchronized long firstIndexOfTerm(long term) {
        if (term <= 0L) {
            return 0L;
        }
        for (RaftLogEntry entry : entries) {
            if (entry.getTerm() == term) {
                return entry.getIndex();
            }
        }
        return 0L;
    }

    public synchronized long lastIndexOfTerm(long term) {
        if (term <= 0L) {
            return 0L;
        }
        for (int i = entries.size() - 1; i >= 0; i--) {
            RaftLogEntry entry = entries.get(i);
            if (entry.getTerm() == term) {
                return entry.getIndex();
            }
        }
        return 0L;
    }

    public synchronized RaftLogEntry getEntry(long index) {
        if (index <= 0L || index > lastLogIndex()) {
            return null;
        }
        return entries.get((int) index - 1);
    }

    public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex) {
        return getEntriesFrom(startIndex, Integer.MAX_VALUE);
    }

    public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex, int maximumEntries) {
        if (maximumEntries <= 0) throw new IllegalArgumentException("maximumEntries must be positive");
        if (startIndex <= 0L || startIndex > lastLogIndex()) {
            return Collections.emptyList();
        }
        int from = (int) startIndex - 1;
        int end = (int) Math.min(entries.size(), (long) from + maximumEntries);
        return Collections.unmodifiableList(new ArrayList<>(entries.subList(from, end)));
    }

    public synchronized void loadFromPersistence(List<RaftLogEntry> persistedEntries) {
        if (!entries.isEmpty()) {
            throw new IllegalStateException("Cannot load persisted entries into a non-empty RaftLog");
        }

        if (persistedEntries == null || persistedEntries.isEmpty()) {
            return;
        }

        long expectedIndex = 1L;
        for (RaftLogEntry entry : persistedEntries) {
            if (entry.getIndex() != expectedIndex) {
                throw new IllegalStateException("Persisted log is not contiguous at index: " + expectedIndex);
            }
            entries.add(entry);
            expectedIndex++;
        }
    }

    /**
     * Appends a single entry at the end of the log atomically with persistence.
     * Either both in-memory state and durable storage are updated, or neither.
     *
     * @param term    term of the entry
     * @param command chat command payload (may be null for a no-op entry)
     * @return the appended entry
     */
    public synchronized RaftLogEntry append(long term, ChatCommand command) {
        long index = lastLogIndex() + 1L;
        RaftLogEntry entry = new RaftLogEntry(index, term, command);
        entries.add(entry);
        try {
            persistence.appendLogEntry(entry);
        } catch (RuntimeException e) {
            entries.remove(entries.size() - 1);
            throw e;
        }
        return entry;
    }

    public synchronized boolean matches(long index, long term) {
        if (index == 0L && term == 0L) {
            return true;
        }
        if (index <= 0L || index > lastLogIndex()) {
            return false;
        }
        return entries.get((int) index - 1).getTerm() == term;
    }

    public synchronized boolean appendEntries(long prevLogIndex, long prevLogTerm, List<RaftLogEntry> newEntries) {
        if (!matches(prevLogIndex, prevLogTerm)) {
            return false;
        }

        if (newEntries == null || newEntries.isEmpty()) {
            return true;
        }

        long expectedIndex = prevLogIndex + 1L;
        int cursor = 0;

        while (cursor < newEntries.size() && expectedIndex <= lastLogIndex()) {
            RaftLogEntry incoming = newEntries.get(cursor);
            if (incoming.getIndex() != expectedIndex) {
                throw new IllegalArgumentException("Non-contiguous entries at index " + expectedIndex);
            }

            RaftLogEntry local = entries.get((int) expectedIndex - 1);
            if (local.getTerm() != incoming.getTerm()) {
                truncateFrom(expectedIndex);
                break;
            }

            cursor++;
            expectedIndex++;
        }

        for (int i = cursor; i < newEntries.size(); i++) {
            RaftLogEntry incoming = newEntries.get(i);
            if (incoming.getIndex() != expectedIndex) {
                throw new IllegalArgumentException("Non-contiguous entries at index " + expectedIndex);
            }
            entries.add(incoming);
            try {
                persistence.appendLogEntry(incoming);
            } catch (RuntimeException e) {
                entries.remove(entries.size() - 1);
                throw e;
            }
            expectedIndex++;
        }

        return true;
    }

    /**
     * Removes all entries from the given index (inclusive) to the end.
     *
     * <p>Refuses to truncate entries that fall within the committed prefix,
     * as required by the Raft safety invariant: a committed entry is durable
     * by virtue of being on a majority and must never be overwritten.
     *
     * @param fromIndex index to truncate from
     * @throws IllegalStateException if {@code fromIndex} targets a committed entry
     */
    public synchronized void truncateFrom(long fromIndex) {
        if (fromIndex <= 0L || fromIndex > lastLogIndex()) {
            return;
        }

        long committedUpTo = commitIndexSupplier.getAsLong();
        if (fromIndex <= committedUpTo) {
            throw new IllegalStateException(
                    "Refusing to truncate committed entry at index " + fromIndex
                            + " (commitIndex=" + committedUpTo + ")");
        }

        int from = (int) fromIndex - 1;
        List<RaftLogEntry> removed = new ArrayList<>(entries.subList(from, entries.size()));

        persistence.truncateLogFrom(fromIndex);
        entries.subList(from, entries.size()).clear();

        for (RaftLogEntry e : removed) {
            try {
                truncationHook.accept(e);
            } catch (RuntimeException hookFailure) {
                // A hook failure must not corrupt the truncated state. Surface only the first.
                Objects.requireNonNull(hookFailure);
            }
        }
    }
}
