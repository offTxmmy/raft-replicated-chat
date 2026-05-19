package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-memory Raft log implementation.
 *
 * This class owns log entry storage and supports append and conflict handling.
 * A consistent snapshot of the log tip is available via snapshotMetadata().
 */
public class RaftLog implements RaftLogMetadata {

    private final List<RaftLogEntry> entries = new ArrayList<>();
    private final RaftPersistence persistence;

    public RaftLog() {
        this(RaftPersistence.NO_OP);
    }

    public RaftLog(RaftPersistence persistence) {
        this.persistence = persistence;
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
     * 
     * FOR PERSON C: This can be used to pass a consistent log metadata snapshot to the RaftElectionManager.
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

    /**
     * Returns the term at the given log index.
     *
     * @param index log index (1-based). Index 0 returns term 0 by convention.
     * @return the term for that index
     * @throws IllegalArgumentException if index is negative or beyond the log tip
     */
    public synchronized long getTermAt(long index) {
        if (index == 0L) {
            return 0L;
        }
        if (index < 0L || index > lastLogIndex()) {
            throw new IllegalArgumentException("Invalid log index: " + index);
        }
        return entries.get((int) index - 1).getTerm();
    }

    /**
     * Returns the first index where the given term appears, or 0 if not present.
     */
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

    /**
     * Returns the last index where the given term appears, or 0 if not present.
     */
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

    /**
     * Returns the entry at the given index, or null if not present.
     *
     * @param index log index (1-based)
     * @return entry or null
     */
    public synchronized RaftLogEntry getEntry(long index) {
        if (index <= 0L || index > lastLogIndex()) {
            return null;
        }
        return entries.get((int) index - 1);
    }

    /**
     * Returns a copy of all entries from the given index (inclusive).
     *
     * @param startIndex first index to include
     * @return immutable list of entries
     */
    public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex) {
        if (startIndex <= 0L || startIndex > lastLogIndex()) {
            return Collections.emptyList();
        }
        int from = (int) startIndex - 1;
        return Collections.unmodifiableList(new ArrayList<>(entries.subList(from, entries.size())));
    }

    /**
     * Rebuilds the in-memory log from persisted entries during startup.
     * This method must not write the loaded entries back to persistence.
     *
     * @param persistedEntries entries loaded from durable storage
     */
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
     * Appends a single entry at the end of the log.
     *
     * @param term    term of the entry
     * @param command chat command payload
     * @return the appended entry
     */
    public synchronized RaftLogEntry append(long term, ChatCommand command) {
        long index = lastLogIndex() + 1L;
        RaftLogEntry entry = new RaftLogEntry(index, term, command);
        persistence.appendLogEntry(entry);
        entries.add(entry);
        return entry;
    }

    /**
     * Checks whether the log contains an entry matching index and term.
     * Index 0 with term 0 always matches the empty base.
     */
    public synchronized boolean matches(long index, long term) {
        if (index == 0L && term == 0L) {
            return true;
        }
        if (index <= 0L || index > lastLogIndex()) {
            return false;
        }
        return entries.get((int) index - 1).getTerm() == term;
    }

    /**
     * Applies an AppendEntries-style update.
     *
     * @param prevLogIndex index immediately preceding new entries
     * @param prevLogTerm term for prevLogIndex
     * @param newEntries entries to append (may be empty for heartbeat)
     * @return true if the log matched prevLogIndex/prevLogTerm and entries were applied
     */
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
            persistence.appendLogEntry(incoming);
            entries.add(incoming);
            expectedIndex++;
        }

        return true;
    }

    /**
     * Removes all entries from the given index (inclusive) to the end.
     *
     * @param fromIndex index to truncate from
     */
    public synchronized void truncateFrom(long fromIndex) {
        if (fromIndex <= 0L || fromIndex > lastLogIndex()) {
            return;
        }
        persistence.truncateLogFrom(fromIndex);
        int from = (int) fromIndex - 1;
        entries.subList(from, entries.size()).clear();
    }
}
