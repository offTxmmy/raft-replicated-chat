package it.polimi.ds.chat.client.connection;

import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Immutable resources belonging to one client-to-broker TCP connection.
 *
 * <p>Receivers and heartbeat workers retain this object rather than consulting
 * the connection's current state. Consequently, an old worker can never start
 * using the writer of a newer connection generation.</p>
 */
public final class ClientConnectionGeneration implements AutoCloseable {

    private final long id;
    private final String host;
    private final int port;
    private final Socket socket;
    private final ObjectInputStream input;
    private final ClientObjectWriter writer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile Object inboundDispatchLock = new Object();

    ClientConnectionGeneration(long id,
                               String host,
                               int port,
                               Socket socket,
                               ObjectInputStream input,
                               ClientObjectWriter writer) {
        this.id = id;
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.socket = Objects.requireNonNull(socket, "socket");
        this.input = Objects.requireNonNull(input, "input");
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    public long getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ObjectInputStream getInputStream() {
        return input;
    }

    public ClientObjectWriter getWriter() {
        return writer;
    }

    public boolean isActive() {
        // Only owner teardown invalidates a generation. A socket closed by a
        // write deadline is still this owner's failed connection: its receiver
        // must report that failure so the runtime can quarantine and reconnect.
        return !closed.get() && writer.isActive();
    }

    /**
     * Dispatches one inbound event only if this generation is still active.
     *
     * <p>The lock is shared by all generations of the owning connection. The
     * active check and the complete dispatch are therefore one linearized
     * operation with respect to dispatch from replacement generations. Closing
     * a generation only flips its atomic state and never waits for this gate:</p>
     *
     * <ul>
     *   <li>if close wins before the check, the stale event is discarded;</li>
     *   <li>if dispatch wins, a newer generation waits until it completes.</li>
     * </ul>
     *
     * @return {@code true} if the event was accepted and dispatched
     */
    public boolean dispatchInboundIfActive(Runnable dispatch) {
        Objects.requireNonNull(dispatch, "dispatch");
        synchronized (inboundDispatchLock) {
            if (!isActive()) {
                return false;
            }
            dispatch.run();
            return true;
        }
    }

    /** Installs the connection-wide gate before this generation is published. */
    void useInboundDispatchLock(Object inboundDispatchLock) {
        this.inboundDispatchLock = Objects.requireNonNull(
                inboundDispatchLock,
                "inboundDispatchLock"
        );
    }

    /**
     * Marks the writer inactive without waiting, then closes the socket to
     * interrupt any in-flight blocking write. The final quiescence barrier
     * ensures that after this method returns no retained worker is still in
     * the old writer.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        writer.deactivate();
        try {
            socket.close();
        } catch (Exception ignored) {
        }
        writer.awaitQuiescence();
    }
}
