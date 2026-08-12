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
        return !closed.get() && writer.isActive() && !socket.isClosed();
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
