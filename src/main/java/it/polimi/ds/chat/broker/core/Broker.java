package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.discovery.PeerRegistry;
import it.polimi.ds.chat.broker.session.ClientHandler;
import it.polimi.ds.chat.discovery.LanDiscoveryService;
import it.polimi.ds.chat.protocol.broker.*;
import it.polimi.ds.chat.protocol.chat.*;
import it.polimi.ds.chat.protocol.client.*;
import it.polimi.ds.chat.protocol.directory.*;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.ordering.api.OrderingServiceCallback;
import it.polimi.ds.chat.common.delivery.HoldBackQueue;
import it.polimi.ds.chat.common.clock.VectorClock;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker node in the replicated chat infrastructure.
 *
 * Responsibilities:
 * - Accept TCP connections from chat clients.
 * - Use Raft-backed OrderingService for message ordering.
 * - Deliver ordered messages to its local clients.
 */
public class Broker implements Serializable, OrderingServiceCallback {

    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    private static final long DIRECTORY_RECONNECT_DELAY_MS = 1000;
    private static final int DIRECTORY_CONNECT_TIMEOUT_MS = 1000;

    // Connection to directory service
    private transient volatile Socket directorySocket;
    private transient ObjectOutputStream directoryOut;
    private final transient Object directoryLock = new Object();
    private transient volatile Thread directoryThread;

    // Peer registry for broker-to-broker discovery
    private transient PeerRegistry peerRegistry;

    // Static configuration for this broker (ports, host, Raft settings, etc.)
    private final BrokerConfig config;

    // Runtime brokerId (may differ from initial config value for followers)
    private int brokerId;

    // Vector clock for tracking causal dependencies when sending messages
    private final VectorClock vectorClock = new VectorClock();

    // Hold-back queue for ordered delivery (enforces total order + causal order)
    private final HoldBackQueue holdBackQueue = new HoldBackQueue();

    // All clients currently connected to this broker.
    private final List<ClientHandler> clients = Collections.synchronizedList(new ArrayList<>());

    // Every accepted session, including sockets that have not completed JOIN.
    // Keeping this separate from the fan-out set lets stop() reclaim pre-JOIN
    // handlers without exposing them to chat history.
    private final Set<ClientHandler> sessions = ConcurrentHashMap.newKeySet();
    private final Map<ClientHandler, Thread> sessionThreads = new ConcurrentHashMap<>();
    private final transient Object sessionLifecycleLock = new Object();

    // Ordering service for message ordering (decoupled from networking)
    private transient OrderingService orderingService;

    // Peer discovery over LAN using UDP broadcast
    private transient LanDiscoveryService lanDiscoveryService;

    // Local per-broker message counter to build unique localMsgId values.
    private long localMsgCounter = 0;

    // Cache of client proposals keyed by stable client id + client sequence.
    // Retries must reuse the same ChatReqMessage so the vector clock advances once.
    private final Map<String, ChatReqMessage> cachedClientRequests = new ConcurrentHashMap<>();

    // Latch to synchronize startup with ID assignment
    private final CountDownLatch brokerIdLatch = new CountDownLatch(1);

    // Monotonic per-broker sequence number used as id for directory heartbeats.
    private final transient AtomicLong directoryHeartbeatSeq = new AtomicLong(0);
    private final transient AtomicBoolean directoryClientCountDirty = new AtomicBoolean(true);

    private final transient CountDownLatch clientListenerReady = new CountDownLatch(1);
    private transient volatile ServerSocket clientServerSocket;
    private transient volatile boolean running;

    /**
     * Construct a broker with the given configuration.
     * Initializes the OrderingService based on configuration.
     */
    public Broker(BrokerConfig config) {
        this.config = config;
        this.brokerId = config.getBrokerId(); // 0 for leader, -1 for followers at startup

        // Initialize OrderingService
        initializeOrderingService();

        // Initialize PeerRegistry (LAN discovery only)
        this.peerRegistry = new PeerRegistry(brokerId);
        this.lanDiscoveryService = new LanDiscoveryService(config, peerRegistry);
    }

    /**
     * Initialize the Raft ordering service.
     */
    private void initializeOrderingService() {
        it.polimi.ds.chat.ordering.raft.RaftOrderingService raftService =
                new it.polimi.ds.chat.ordering.raft.RaftOrderingService(config);
        raftService.setCallback(this);
        raftService.onDeliver(this::handleOrderedMessage);
        this.orderingService = raftService;
        brokerIdLatch.countDown();
    }

    // =========================================================================
    // OrderingServiceCallback implementation
    // =========================================================================

    /**
     * Callback invoked when the ordering service assigns a broker ID to this instance.
     *
     * Updates internal brokerId, logs the change, and reinitializes the PeerRegistry with the new id.
     *
     * @param newBrokerId the broker id assigned by the ordering service
     */
    @Override
    public void onBrokerIdAssigned(int newBrokerId, long currentSeq) {
        System.out.println("[Broker] Broker ID updated from " + this.brokerId + " to " + newBrokerId);
        System.out.println("[Broker] Syncing sequence number to " + currentSeq);

        this.brokerId = newBrokerId;
        this.holdBackQueue.syncToSequence(currentSeq);

        // Update peer registry with new broker ID
        if (this.peerRegistry != null) {
            this.peerRegistry = new PeerRegistry(newBrokerId);
        }

        if (this.lanDiscoveryService != null) {
            this.lanDiscoveryService.setBrokerId(newBrokerId);
        }

        // Signal that thet ID is assigned, allowing start() to proceed
        brokerIdLatch.countDown();
    }

    /**
     * Callback invoked when the connection to the ordering service is lost.
     *
     * This method should perform any required cleanup or reconnection logic (currently logs an error).
     */
    @Override
    public void onConnectionLost() {
        System.err.println("[Broker] Connection to ordering service lost!");
    }

    /**
     * Callback invoked when the connection to the ordering service is established.
     *
     * Can be used to notify the operator or trigger follow-up actions.
     */
    @Override
    public void onConnectionEstablished() {
        System.out.println("[Broker] Connected to ordering service");
    }

    @Override
    public void onLeaderChanged(int newLeaderId, long term) {
        System.out.println("[Broker] Raft leader changed: leaderId=" + newLeaderId + ", term=" + term);
    }

    // =========================================================================
    // Getters
    // =========================================================================

    /**
     * Get the vector clock tracking delivered messages.
     */
    public VectorClock getVectorClock() {
        return holdBackQueue.getDeliveredClock();
    }

    /**
     * Get the vector clock used for outgoing messages.
     */
    public VectorClock getSendVectorClock() {
        return vectorClock;
    }

    /**
     * Get the broker identifier assigned to this broker instance.
     *
     * @return current broker id
     */
    public int getBrokerId() {
        return brokerId;
    }

    /**
     * Get the ordering service used by this broker.
     */
    public OrderingService getOrderingService() {
        return orderingService;
    }

    /**
     * Set a custom ordering service, mainly for tests.
     */
    public void setOrderingService(OrderingService orderingService) {
        this.orderingService = orderingService;
    }

    /**
     * Get the peer registry for broker-to-broker discovery.
     */
    public PeerRegistry getPeerRegistry() {
        return peerRegistry;
    }

    // =========================================================================
    // Broker lifecycle
    // =========================================================================

    /**
     * Start the broker:
     * - Start the Raft ordering service
     * - Open TCP listener for clients
     * - Connect to directory service
     * - Start peer discovery
     *
     * This method blocks in a loop accepting client connections.
     *
     * @throws IOException if the server socket cannot be opened
     */
    public void start() throws IOException {
        try {
            // Raft must be ready before clients can submit commands.
            orderingService.start();

            boolean assigned = brokerIdLatch.await(10, TimeUnit.SECONDS);
            if (!assigned) {
                throw new IOException("Failed to initialize broker id within timeout.");
            }

            // Bind the client listener before publishing the endpoint. Once the
            // ServerSocket is bound, the OS accept backlog is already active even
            // though this thread has not entered accept() yet.
            clientServerSocket = new ServerSocket(config.getClientPort());
            running = true;
            clientListenerReady.countDown();

            // Complete the remaining local startup before registration. These
            // operations either succeed or are explicitly best-effort; a failure
            // before this point is rolled back without ever publishing the broker.
            startPeerDiscovery();
            startDirectoryRegistrationLoop();

            System.out.println("Broker " + brokerId + " listening for clients on port "
                    + clientServerSocket.getLocalPort());

            while (running) {
                try {
                    Socket clientSocket = clientServerSocket.accept();
                    System.out.println("New client connected from "
                            + clientSocket.getRemoteSocketAddress());

                    startAcceptedClientSession(clientSocket);
                } catch (SocketException e) {
                    if (running) {
                        throw e;
                    }
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
            throw new IOException("Interrupted while waiting for broker id initialization.", e);
        } catch (IOException | RuntimeException e) {
            stop();
            throw e;
        } finally {
            if (!running) {
                stop();
            }
        }
    }

    /**
     * Stops all broker-owned resources. The method is idempotent and is also
     * used as the inverse rollback path for partial startup.
     */
    public void stop() {
        running = false;

        ServerSocket listener = clientServerSocket;
        clientServerSocket = null;
        if (listener != null) {
            try {
                listener.close();
            } catch (IOException ignored) {
            }
        }

        Thread heartbeat = directoryThread;
        if (heartbeat != null) {
            heartbeat.interrupt();
        }
        closeDirectoryConnection();

        if (lanDiscoveryService != null) {
            lanDiscoveryService.stop();
        }

        List<ClientHandler> sessionSnapshot;
        List<Thread> threadSnapshot;
        synchronized (sessionLifecycleLock) {
            sessionSnapshot = new ArrayList<>(sessions);
            threadSnapshot = new ArrayList<>(sessionThreads.values());
        }
        for (ClientHandler handler : sessionSnapshot) {
            handler.closeSession();
        }
        for (Thread thread : threadSnapshot) {
            joinBounded(thread, 1_000L);
        }

        // Socket close above must also release a Directory writer blocked in
        // flush. Wait only after closing it, never while holding its writer lock.
        joinBounded(heartbeat, 1_000L);

        if (orderingService != null) {
            orderingService.stop();
        }
    }

    boolean awaitClientListenerReady(long timeout, TimeUnit unit) throws InterruptedException {
        return clientListenerReady.await(timeout, unit);
    }

    boolean isDirectoryRegistrationLoopStartedForTesting() {
        return directoryThread != null;
    }

    void installDirectoryConnectionForTesting(
            Socket socket,
            ObjectOutputStream output) {
        synchronized (directoryLock) {
            directorySocket = socket;
            directoryOut = output;
        }
    }

    void sendDirectoryObjectForTesting(Object message) throws IOException {
        sendDirectoryObject(message);
    }

    int activeSessionCountForTesting() {
        return sessions.size();
    }

    void startClientSession(Socket clientSocket) {
        startClientSession(clientSocket, false);
    }

    private void startAcceptedClientSession(Socket clientSocket) {
        startClientSession(clientSocket, true);
    }

    private void startClientSession(Socket clientSocket, boolean requireRunning) {
        synchronized (sessionLifecycleLock) {
            if (requireRunning && !running) {
                try {
                    clientSocket.close();
                } catch (IOException ignored) {
                }
                return;
            }

            ClientHandler handler = new ClientHandler(clientSocket, this);
            sessions.add(handler);

            Thread thread = new Thread(() -> {
                try {
                    handler.run();
                } finally {
                    removeClient(handler);
                    sessionThreads.remove(handler, Thread.currentThread());
                }
            }, "ClientHandler-" + clientSocket.getPort());
            thread.setDaemon(true);
            sessionThreads.put(handler, thread);
            try {
                thread.start();
            } catch (RuntimeException e) {
                sessionThreads.remove(handler, thread);
                sessions.remove(handler);
                handler.closeSession();
                throw e;
            }
        }
    }

    private static void joinBounded(Thread thread, long timeoutMillis) {
        if (thread == null || thread == Thread.currentThread()) {
            return;
        }
        try {
            thread.join(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Remove a client from the internal list (called by ClientHandler when the client disconnects).
     *
     * @param handler the client handler to remove
     */
    public void removeClient(ClientHandler handler) {
        sessions.remove(handler);
        if (clients.remove(handler)) {
            sendClientCountUpdate();
        }
    }

    /**
     * Activates a newly joined session only after a committed Raft boundary has
     * been applied locally. Until this method succeeds, the handler is not part
     * of the client fan-out set and therefore cannot observe delayed history.
     */
    public boolean activateClient(ClientHandler handler) {
        if (handler == null || handler.isSessionClosed()) {
            return false;
        }

        String boundaryId = "join:" + brokerId + ":" + UUID.randomUUID();
        AtomicBoolean activated = new AtomicBoolean(false);
        boolean established = orderingService.establishDeliveryBoundary(boundaryId, () -> {
            // This callback runs in the serialized state-machine application
            // path. Holding the same monitor used by fan-out snapshots makes
            // WELCOME + recipient activation indivisible with respect to the
            // next locally applied chat entry.
            synchronized (clients) {
                if (handler.isSessionClosed()
                        || !handler.activateAtDeliveryBoundary()) {
                    return;
                }

                // WELCOME is already queued when the handler becomes visible
                // to subsequent chat fan-out.
                clients.add(handler);
                activated.set(true);
                sendClientCountUpdate();
            }
        });
        return established && activated.get();
    }

    /**
     * Deliver a message that has already been assigned a global sequence number
     * to all locally connected clients.
     *
     * The sender will not receive the message back.
     *
     * @param seq    global sequence number
     * @param sender username of the sender
     * @param text   message text
     */
    public void onChatDeliver(long seq, String sender, String text) {
        onChatDeliver(seq, sender, null, text);
    }

    /**
     * Deliver a message to all locally connected clients except the originating client.
     *
     * The client id is used as technical identity; username is only a display name.
     */
    public void onChatDeliver(long seq, String sender, String senderClientId, String text) {
        List<ClientHandler> snapshot;
        synchronized (clients) {
            snapshot = new ArrayList<>(clients);
        }

        // Never hold the shared client-list monitor while interacting with a
        // session. ClientHandler only enqueues here; its single outbound worker
        // owns the actual ObjectOutputStream write.
        for (ClientHandler handler : snapshot) {
            if (senderClientId == null || !senderClientId.equals(handler.getClientId())) {
                handler.sendMessageToClient(seq, sender, text);
            }
        }
    }

    /**
     * Entry point for messages sent by clients connected to THIS broker.
     *
     * Wraps the client message into a ChatReqMessage (including vector clock) and proposes it
     * to the OrderingService for global sequencing.
     *
     * @param message raw client message received by this broker
     */
    public boolean onClientMessage(ClientMessage message) {
        // In Raft mode followers proxy the proposal to the known leader, so
        // clients can stay connected to the broker selected by DirectoryService.
        ChatReqMessage chatReq = buildChatReq(
                message.getUsername(),
                message.getClientId(),
                message.getClientSeq(),
                message.getText());

        // Propose to ordering service.
        boolean accepted = orderingService.propose(chatReq);

        if (accepted) {
            // OrderingService true is a definitive commit confirmation. Remove
            // only the exact cached instance used by this attempt: a delayed
            // concurrent confirmation must not evict a newer retry entry.
            cachedClientRequests.remove(
                    clientProposalKey(message.getClientId(), message.getClientSeq()),
                    chatReq);
        } else {
            System.err.println("[Broker " + brokerId + "] Proposal rejected for "
                    + message.getUsername()
                    + ". Known leader = " + orderingService.getLeaderId());
        }
        return accepted;
    }

    /**
     * Handle an ordered message from the OrderingService.
     *
     * Enqueues the delivered ChatDeliverMessage into the HoldBackQueue to enforce causal +
     * total order, obtains all messages that are now ready, and delivers them locally.
     *
     * @param chatDeliver message delivered by the ordering service (contains global seq)
     */
    public void handleOrderedMessage(ChatDeliverMessage chatDeliver) {
        long incomingSeq = chatDeliver.getSeq();
        long expectedSeq = holdBackQueue.getExpectedSeq();

        // Check for Gaps (Reliability Layer)
        if (incomingSeq > expectedSeq) {
            System.out.println("[Broker " + brokerId + "] Gap detected! Received seq = " + incomingSeq + ", expected = " + expectedSeq);

            System.err.println("[Broker " + brokerId + "] Unexpected gap in Raft delivery; waiting for log catch-up.");
        }

        // Enqueue message and get all messages ready for delivery
        List<ChatDeliverMessage> readyMessages = holdBackQueue.enqueue(chatDeliver);

        // Merge causal knowledge for each message that is actually released,
        // before making that message visible to any local client. A message that
        // is merely buffered must not influence subsequent outgoing proposals.
        for (ChatDeliverMessage msg : readyMessages) {
            if (msg.getVectorClock() != null) {
                synchronized (this) {
                    vectorClock.update(msg.getVectorClock());
                }
            }
            onChatDeliver(msg.getSeq(), msg.getUsername(), msg.getClientId(), msg.getText());
        }
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    /**
     * Build or reuse a ChatReqMessage for the given stable client message id.
     *
     * The first request for a stable client id/sequence increments the broker send vector clock,
     * creates a local message id, and stores the resulting proposal in a local cache.
     * Retries with the same client id and sequence reuse the cached request so the
     * vector clock is not incremented again. The entry is retained across failed
     * attempts and removed only after the ordering service confirms commitment.
     *
     * @param username sender username
     * @param clientId stable client process id
     * @param clientSeq stable per-client message sequence
     * @param text     message text
     * @return constructed ChatReqMessage ready for proposing to the ordering service
     */
    private synchronized ChatReqMessage buildChatReq(String username, String clientId, long clientSeq, String text) {
        String proposalKey = clientProposalKey(clientId, clientSeq);
        return cachedClientRequests.computeIfAbsent(proposalKey, key -> {
            vectorClock.increment(brokerId);
            String localMsgId = brokerId + "-" + (++localMsgCounter);
            return new ChatReqMessage(
                    localMsgId,
                    brokerId,
                    username,
                    text,
                    new VectorClock(vectorClock),
                    clientId,
                    clientSeq);
        });
    }

    private static String clientProposalKey(String clientId, long clientSeq) {
        return clientId + ":" + clientSeq;
    }

    int cachedClientRequestCountForTesting() {
        return cachedClientRequests.size();
    }

    /** Starts one reconnecting Directory session owner for this broker. */
    private void startDirectoryRegistrationLoop() {
        if (directoryThread != null && directoryThread.isAlive()) {
            return;
        }

        Thread thread = new Thread(this::directoryRegistrationLoop, "DirectorySession-" + brokerId);
        thread.setDaemon(true);
        directoryThread = thread;
        thread.start();
    }

    private void directoryRegistrationLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                connectAndRegisterWithDirectoryService();
                directoryClientCountDirty.set(true);

                while (running && !Thread.currentThread().isInterrupted()) {
                    sendDirectoryObject(new HeartbeatMessage(
                            this.brokerId,
                            directoryHeartbeatSeq.incrementAndGet()));
                    if (directoryClientCountDirty.compareAndSet(true, false)) {
                        try {
                            sendDirectoryObject(new ClientCountUpdateMessage(
                                    brokerId,
                                    clients.size()));
                        } catch (IOException e) {
                            directoryClientCountDirty.set(true);
                            throw e;
                        }
                    }
                    Thread.sleep(directoryHeartbeatIntervalMs());
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("Directory connection lost; will re-register at "
                            + config.getDirectoryHost() + ":" + config.getDirectoryPort()
                            + ": " + e.getMessage());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } finally {
                closeDirectoryConnection();
            }

            if (running) {
                try {
                    Thread.sleep(directoryReconnectDelayMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    protected long directoryHeartbeatIntervalMs() {
        return HEARTBEAT_INTERVAL_MS;
    }

    protected long directoryReconnectDelayMs() {
        return DIRECTORY_RECONNECT_DELAY_MS;
    }

    private void connectAndRegisterWithDirectoryService() throws IOException {
        Socket socket = new Socket();
        ObjectOutputStream output = null;
        boolean installed = false;
        try {
            socket.connect(
                    new InetSocketAddress(config.getDirectoryHost(), config.getDirectoryPort()),
                    DIRECTORY_CONNECT_TIMEOUT_MS);
            output = new ObjectOutputStream(socket.getOutputStream());

            DirectoryRegisterMessage registration = new DirectoryRegisterMessage(
                    this.brokerId,
                    config.getBrokerHost(),
                    config.getClientPort());

            synchronized (directoryLock) {
                if (!running) {
                    throw new IOException("Broker is stopping");
                }
                directorySocket = socket;
                directoryOut = output;
                output.writeObject(registration);
                output.flush();
                installed = true;
            }

            System.out.println("Registered broker in Directory Service at "
                    + config.getDirectoryHost() + ":" + config.getDirectoryPort()
                    + ": " + registration);
        } finally {
            if (!installed) {
                if (output != null) {
                    try {
                        output.close();
                    } catch (IOException ignored) {
                    }
                }
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private void sendDirectoryObject(Object message) throws IOException {
        synchronized (directoryLock) {
            if (directoryOut == null) {
                throw new IOException("No active Directory connection");
            }
            directoryOut.writeObject(message);
            directoryOut.flush();
            // Heartbeats and count updates are transient; resetting prevents the
            // ObjectOutputStream handle table from growing for the lifetime of the broker.
            directoryOut.reset();
        }
    }

    private void closeDirectoryConnection() {
        // Closing a socket is safe concurrently with a blocked write and is what
        // releases that write. Do this before acquiring directoryLock; otherwise
        // stop/reconnect could deadlock behind ObjectOutputStream.flush().
        Socket socketToClose = directorySocket;
        if (socketToClose != null) {
            try {
                socketToClose.close();
            } catch (IOException ignored) {
            }
        }

        synchronized (directoryLock) {
            if (directorySocket == socketToClose) {
                directorySocket = null;
                directoryOut = null;
            }
        }
    }

    /**
     * Send an update message to the Directory Service reporting the current number of connected clients.
     *
     * Errors while sending are logged.
     */
    private void sendClientCountUpdate() {
        // Only the Directory session owner writes its ObjectOutputStream. Client
        // handlers merely mark the latest count dirty, keeping Raft fan-out free
        // from unrelated Directory I/O.
        directoryClientCountDirty.set(true);
    }

    /**
     * Start the peer discovery mechanism.
     * Registers for peer list updates and logs changes.
     */
    private void startPeerDiscovery() {
        if (peerRegistry == null) {
            System.err.println("[Broker] PeerRegistry not initialized");
            return;
        }

        // Register listener for peer changes
        peerRegistry.addPeerChangeListener(peers -> {
            System.out.println("[Broker " + brokerId + "] Peer list updated: " + peers.size() + " peers");
            for (PeerInfo peer : peers) {
                System.out.println("  - " + peer);
            }
        });

        // Only LAN discovery is used
        if (lanDiscoveryService != null) {
            try {
                lanDiscoveryService.start();
                for (int i = 0; i < 3; i++) {
                    lanDiscoveryService.announcePresence();
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {}
                }
            } catch (SocketException e) {
                System.err.println("[Broker " + brokerId + "] Failed to start LAN discovery: " + e.getMessage());
            }
        }
        System.out.println("[Broker " + brokerId + "] Peer discovery started (LAN broadcast only)");
    }

}
