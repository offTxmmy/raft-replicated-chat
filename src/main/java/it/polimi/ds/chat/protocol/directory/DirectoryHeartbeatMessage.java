package it.polimi.ds.chat.protocol.directory;

import java.io.Serializable;

/** Renews one broker registration and publishes its current local load. */
public record DirectoryHeartbeatMessage(int clientCount) implements Serializable {
    private static final long serialVersionUID = 1L;

    public DirectoryHeartbeatMessage {
        if (clientCount < 0) {
            throw new IllegalArgumentException("clientCount must be non-negative");
        }
    }
}
