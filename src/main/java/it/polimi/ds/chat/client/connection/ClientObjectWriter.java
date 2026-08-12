package it.polimi.ds.chat.client.connection;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The single serialized write boundary for one client TCP connection.
 *
 * <p>{@link ObjectOutputStream} keeps protocol state and must not be used by
 * independent concurrent writers. Every producer associated with a connection
 * generation (JOIN/QUIT, chat, retry and heartbeat) therefore shares one
 * instance of this class.</p>
 */
public final class ClientObjectWriter {

    private final ObjectOutputStream output;
    private final Object writeLock = new Object();
    private final AtomicBoolean active = new AtomicBoolean(true);

    public ClientObjectWriter(ObjectOutputStream output) {
        this.output = Objects.requireNonNull(output, "output");
    }

    /**
     * Writes and flushes one complete protocol object atomically with respect
     * to all other writers for this connection generation.
     */
    public void send(Object message) throws IOException {
        synchronized (writeLock) {
            if (!active.get()) {
                throw new IOException("Client connection generation is no longer active");
            }

            output.writeObject(message);
            output.flush();
        }
    }

    /** Prevents future sends without waiting behind a blocked socket write. */
    void deactivate() {
        active.set(false);
    }

    /** Waits until any send that passed the pre-deactivation check has ended. */
    void awaitQuiescence() {
        synchronized (writeLock) {
            // Acquiring and releasing the lock is the quiescence barrier.
        }
    }

    public boolean isActive() {
        return active.get();
    }

}
