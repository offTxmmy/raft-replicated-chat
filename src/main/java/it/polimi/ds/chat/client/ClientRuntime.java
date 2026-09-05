package it.polimi.ds.chat.client;

import it.polimi.ds.chat.client.connection.ClientConnection;
import it.polimi.ds.chat.client.connection.ClientConnectionGeneration;
import it.polimi.ds.chat.client.messaging.ClientHeartbeatManager;
import it.polimi.ds.chat.client.messaging.ClientMessageReceiver;
import it.polimi.ds.chat.client.messaging.ClientMessageSender;
import it.polimi.ds.chat.protocol.client.ClientJoinMessage;
import it.polimi.ds.chat.protocol.client.ClientQuitMessage;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Coordinates one client's connection-generation lifecycle.
 *
 * <p>Failure callbacks carry the generation they belong to. A callback from an
 * obsolete receiver or heartbeat worker is ignored and can never close the
 * replacement connection.</p>
 */
final class ClientRuntime {

    private static final long INITIAL_RECONNECT_BACKOFF_MS = 250L;
    private static final long MAX_RECONNECT_BACKOFF_MS = 2_000L;
    private static final long QUIT_SEND_GRACE_MS = 250L;

    private final ClientConnection connection;
    private final ClientMessageSender sender;
    private final String username;
    private final String clientId;
    private final Consumer<String> chatOutput;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();
    private final ClientReconnectManager reconnectManager;

    private SessionRuntime currentSession;

    ClientRuntime(ClientConnection connection,
                  ClientMessageSender sender,
                  String username,
                  String clientId) {
        this(connection, sender, username, clientId, System.out::println);
    }

    ClientRuntime(ClientConnection connection,
                  ClientMessageSender sender,
                  String username,
                  String clientId,
                  Consumer<String> chatOutput) {
        this(
                connection,
                sender,
                username,
                clientId,
                INITIAL_RECONNECT_BACKOFF_MS,
                MAX_RECONNECT_BACKOFF_MS,
                Thread::sleep,
                chatOutput
        );
    }

    ClientRuntime(ClientConnection connection,
                  ClientMessageSender sender,
                  String username,
                  String clientId,
                  long initialReconnectBackoffMs,
                  long maxReconnectBackoffMs,
                  ClientReconnectManager.BackoffWaiter backoffWaiter) {
        this(
                connection,
                sender,
                username,
                clientId,
                initialReconnectBackoffMs,
                maxReconnectBackoffMs,
                backoffWaiter,
                System.out::println
        );
    }

    ClientRuntime(ClientConnection connection,
                  ClientMessageSender sender,
                  String username,
                  String clientId,
                  long initialReconnectBackoffMs,
                  long maxReconnectBackoffMs,
                  ClientReconnectManager.BackoffWaiter backoffWaiter,
                  Consumer<String> chatOutput) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.username = Objects.requireNonNull(username, "username");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.chatOutput = Objects.requireNonNull(chatOutput, "chatOutput");
        this.reconnectManager = new ClientReconnectManager(
                this::reconnectOnce,
                initialReconnectBackoffMs,
                maxReconnectBackoffMs,
                backoffWaiter
        );
    }

    /**
     * Installs an already-open generation, or starts the same eventual reconnect
     * loop used after runtime failures when no healthy initial connection exists.
     */
    void start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Client runtime was already started");
        }

        ClientConnectionGeneration generation =
                connection.getCurrentGeneration();
        if (generation == null || !generation.isActive()) {
            reconnectManager.requestReconnect();
            return;
        }
        try {
            installGeneration(generation);
        } catch (IOException e) {
            if (!running.get()) {
                throw e;
            }
            connection.markEndpointFailed(generation);
            connection.closeGeneration(generation);
            reconnectManager.requestReconnect();
        }
    }

    /** Best-effort QUIT followed by cancellation and generation teardown. */
    void shutdown(boolean sendQuit) {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        reconnectManager.shutdown();

        SessionRuntime session;
        synchronized (lifecycleLock) {
            session = currentSession;
            currentSession = null;
        }

        if (session != null) {
            session.stopWorkers();

            if (sendQuit && session.generation.isActive()) {
                sendQuitWithBoundedGrace(session.generation);
            }

            // The sender deliberately holds its state lock across writer.send().
            // Close the transport first so a blocked write is interrupted before
            // detachWriter()/shutdown() attempt to acquire that state lock.
            connection.shutdown();
            sender.detachWriter(session.generation.getWriter());
        } else {
            connection.shutdown();
        }
        sender.shutdown();
    }

    boolean isRunning() {
        return running.get();
    }

    boolean isReconnecting() {
        return reconnectManager.isReconnecting();
    }

    private void installGeneration(ClientConnectionGeneration generation)
            throws IOException {
        SessionRuntime session = null;
        try {
            generation.getWriter().send(
                    ClientJoinMessage.joinCommand(username, clientId)
            );

            ClientHeartbeatManager heartbeatManager =
                    new ClientHeartbeatManager(
                            generation.getWriter(),
                            generation::isActive,
                            () -> handleConnectionFailure(generation)
                    );
            ClientMessageReceiver receiver =
                    new ClientMessageReceiver(
                            generation,
                            sender,
                            clientId,
                            heartbeatManager,
                            () -> handleConnectionFailure(generation),
                            chatOutput
                    );
            session = new SessionRuntime(
                    generation,
                    receiver,
                    heartbeatManager
            );

            // JOIN is serialized before any retained pending chat message.
            sender.attachWriter(generation.getWriter());

            synchronized (lifecycleLock) {
                if (!running.get()) {
                    throw new IOException("Client is shutting down");
                }
                if (currentSession != null) {
                    throw new IOException("A client connection is already active");
                }
                currentSession = session;
            }

            session.startWorkers();
        } catch (IOException | RuntimeException e) {
            if (session != null) {
                session.stopWorkers();
            }
            synchronized (lifecycleLock) {
                if (currentSession == session) {
                    currentSession = null;
                }
            }
            // As in the normal failure path, transport close precedes any
            // sender-state lock acquisition.
            connection.closeGeneration(generation);
            // attachWriter publishes the writer before its forced head send;
            // compare-and-detach is therefore needed even when attach throws.
            sender.detachWriter(generation.getWriter());
            throw e;
        }
    }

    private void handleConnectionFailure(
            ClientConnectionGeneration failedGeneration
    ) {
        SessionRuntime failedSession;
        synchronized (lifecycleLock) {
            if (!running.get()
                    || currentSession == null
                    || currentSession.generation != failedGeneration) {
                return;
            }
            failedSession = currentSession;
            currentSession = null;
        }

        failedSession.stopWorkers();
        connection.markEndpointFailed(failedGeneration);
        // deactivate + socket close interrupts a sender that may currently hold
        // its state lock while blocked in writer.send().
        connection.closeGeneration(failedGeneration);
        sender.detachWriter(failedGeneration.getWriter());
        reconnectManager.requestReconnect();
    }

    /**
     * Gives a healthy connection a short opportunity to serialize QUIT without
     * allowing a congested socket to make shutdown unbounded. If it does not
     * complete, the caller closes the generation and interrupts the write.
     */
    private void sendQuitWithBoundedGrace(
            ClientConnectionGeneration generation
    ) {
        CountDownLatch completed = new CountDownLatch(1);
        Thread quitThread = new Thread(
                () -> {
                    try {
                        generation.getWriter().send(
                                ClientQuitMessage.quitCommand()
                        );
                    } catch (IOException ignored) {
                        // Closing the generation is the fallback quit signal.
                    } finally {
                        completed.countDown();
                    }
                },
                "ClientQuit-g" + generation.getId()
        );
        quitThread.setDaemon(true);
        quitThread.start();

        try {
            completed.await(QUIT_SEND_GRACE_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void reconnectOnce() throws IOException {
        if (!running.get()) {
            throw new IOException("Client is shutting down");
        }

        connection.open();
        ClientConnectionGeneration generation =
                connection.getCurrentGeneration();
        if (generation == null) {
            throw new IOException("Reconnect did not create a connection generation");
        }

        if (!running.get()) {
            connection.closeGeneration(generation);
            throw new IOException("Client is shutting down");
        }

        try {
            installGeneration(generation);
        } catch (IOException e) {
            if (running.get()) {
                connection.markEndpointFailed(generation);
            }
            connection.closeGeneration(generation);
            throw e;
        }
    }

    private static final class SessionRuntime {
        private final ClientConnectionGeneration generation;
        private final ClientMessageReceiver receiver;
        private final ClientHeartbeatManager heartbeatManager;
        private final Thread receiverThread;
        private final Thread heartbeatThread;

        private SessionRuntime(ClientConnectionGeneration generation,
                               ClientMessageReceiver receiver,
                               ClientHeartbeatManager heartbeatManager) {
            this.generation = generation;
            this.receiver = receiver;
            this.heartbeatManager = heartbeatManager;
            this.receiverThread = new Thread(
                    receiver,
                    "MessageReceiver-g" + generation.getId()
            );
            this.heartbeatThread = new Thread(
                    heartbeatManager,
                    "Heartbeat-g" + generation.getId()
            );
            receiverThread.setDaemon(true);
            heartbeatThread.setDaemon(true);
        }

        private void startWorkers() {
            receiverThread.start();
            heartbeatThread.start();
        }

        private void stopWorkers() {
            receiver.stop();
            heartbeatManager.stop();
            receiverThread.interrupt();
            heartbeatThread.interrupt();
        }
    }
}
