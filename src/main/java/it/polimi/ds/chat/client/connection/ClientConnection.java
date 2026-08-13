package it.polimi.ds.chat.client.connection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the successive TCP connection generations between a client and a broker.
 *
 * <p>A generation is fully constructed before it is published. Replacing it
 * invalidates the previous generation while holding the lifecycle lock, so old
 * receiver/heartbeat/sender workers cannot migrate accidentally to the new
 * stream.</p>
 */
public class ClientConnection {

    private static final int CONNECT_TIMEOUT_MS = 2_000;
    private static final int STREAM_HANDSHAKE_TIMEOUT_MS = 2_000;

    protected volatile String host;
    protected volatile int port;

    private final Object lifecycleLock = new Object();
    // Serializes inbound dispatch across every generation owned by this
    // connection. Generations still use their own streams and workers, but a
    // replacement cannot make a message visible while an older generation is
    // completing an already accepted inbound event.
    private final Object inboundDispatchLock = new Object();
    private final AtomicLong nextGenerationId = new AtomicLong(0L);
    private final AtomicBoolean acceptingGenerations = new AtomicBoolean(true);
    private volatile ClientConnectionGeneration currentGeneration;

    public ClientConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** Opens and atomically installs a new connection to the configured endpoint. */
    public void open() throws IOException {
        openTo(host, port);
    }

    /**
     * Builds a complete socket/stream generation before publishing it.
     * Subclasses use this method after selecting an endpoint.
     */
    protected final ClientConnectionGeneration openTo(String newHost, int newPort)
            throws IOException {
        ClientConnectionGeneration candidate = createGeneration(newHost, newPort);
        candidate.useInboundDispatchLock(inboundDispatchLock);

        synchronized (lifecycleLock) {
            if (!acceptingGenerations.get()) {
                candidate.close();
                throw new IOException("Client connection is shutting down");
            }
            ClientConnectionGeneration previous = currentGeneration;
            if (previous != null) {
                previous.close();
            }

            host = newHost;
            port = newPort;
            currentGeneration = candidate;
        }

        return candidate;
    }

    /**
     * Creates a generation using local variables so a failed connect or stream
     * handshake cannot expose partially initialized connection state.
     */
    protected ClientConnectionGeneration createGeneration(String newHost, int newPort)
            throws IOException {
        Socket candidateSocket = new Socket();
        try {
            candidateSocket.connect(
                    new InetSocketAddress(newHost, newPort),
                    CONNECT_TIMEOUT_MS
            );
            candidateSocket.setSoTimeout(STREAM_HANDSHAKE_TIMEOUT_MS);

            ObjectOutputStream objectOut =
                    new ObjectOutputStream(candidateSocket.getOutputStream());
            objectOut.flush();
            ObjectInputStream objectIn =
                    new ObjectInputStream(candidateSocket.getInputStream());

            candidateSocket.setSoTimeout(0);
            ClientObjectWriter writer = new ClientObjectWriter(objectOut);
            return new ClientConnectionGeneration(
                    nextGenerationId.incrementAndGet(),
                    newHost,
                    newPort,
                    candidateSocket,
                    objectIn,
                    writer
            );
        } catch (IOException | RuntimeException e) {
            try {
                candidateSocket.close();
            } catch (IOException ignored) {
            }
            throw e;
        }
    }

    public ClientConnectionGeneration getCurrentGeneration() {
        return currentGeneration;
    }

    /**
     * Closes the generation only if it is still current. Passing an obsolete
     * generation can therefore never close a newer connection.
     */
    public void closeGeneration(ClientConnectionGeneration expectedGeneration) {
        if (expectedGeneration == null) {
            return;
        }

        synchronized (lifecycleLock) {
            if (currentGeneration == expectedGeneration) {
                currentGeneration = null;
            }
            expectedGeneration.close();
        }
    }

    /** Hook used by directory-aware connections to quarantine a failed endpoint. */
    public void markEndpointFailed(ClientConnectionGeneration failedGeneration) {
        // Direct connections have no alternate endpoint discovery policy.
    }

    public boolean isOpen() {
        ClientConnectionGeneration generation = currentGeneration;
        return generation != null && generation.isActive();
    }

    public void close() {
        ClientConnectionGeneration generation;
        synchronized (lifecycleLock) {
            generation = currentGeneration;
            currentGeneration = null;
            if (generation != null) {
                generation.close();
            }
        }
    }

    /**
     * Permanently closes this connection owner and prevents an in-flight
     * reconnect attempt from publishing a generation after client shutdown.
     */
    public void shutdown() {
        acceptingGenerations.set(false);
        close();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

}
