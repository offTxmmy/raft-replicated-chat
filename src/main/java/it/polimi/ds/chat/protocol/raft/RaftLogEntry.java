package it.polimi.ds.chat.protocol.raft;

import java.io.Serializable;

/**
 * Single Raft log entry.
 * The command is a ChatCommand, interpreted by the application layer.
 */
public class RaftLogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long index;
    private final long term;
    private final ChatCommand command;

    public RaftLogEntry(long index, long term, ChatCommand command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public ChatCommand getCommand() {
        return command;
    }
}
