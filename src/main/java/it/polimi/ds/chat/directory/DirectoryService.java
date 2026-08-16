package it.polimi.ds.chat.directory;

import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.client.HeartbeatMessage;
import it.polimi.ds.chat.protocol.directory.ClientCountUpdateMessage;
import it.polimi.ds.chat.protocol.directory.DirectoryRegisterMessage;
import it.polimi.ds.chat.protocol.directory.GetBrokerRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetBrokerResponseMessage;
import it.polimi.ds.chat.protocol.directory.GetClusterRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetClusterResponseMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Directory for broker bootstrap and client-side broker selection.
 *
 * <p>The configured Raft voter set is immutable and independent from the live
 * broker registry. The latter is a lease-style availability view used only for
 * client selection. Each broker id owns one atomic registry slot, so endpoint,
 * client count and heartbeat time cannot get out of sync across independent
 * maps.</p>
 */
public class DirectoryService {

    static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 10_000L;
    static final long DEFAULT_REAPER_INTERVAL_MS = 5_000L;

    private final Object registryLock = new Object();
    private final Object lifecycleLock = new Object();
    private final Map<Integer, BrokerSlot> brokerSlots = new HashMap<>();
    private final Set<Socket> activeConnections = ConcurrentHashMap.newKeySet();
    private final Set<Thread> connectionThreads = ConcurrentHashMap.newKeySet();
    private final AtomicLong registrationEpoch = new AtomicLong();

    private final Map<Integer, RaftPeerEndpoint> clusterVoters;
    private final LongSupplier clock;
    private final long heartbeatTimeoutMs;
    private final long reaperIntervalMs;

    private final CountDownLatch brokerListenerReady = new CountDownLatch(1);
    private final CountDownLatch clientListenerReady = new CountDownLatch(1);

    private volatile boolean running = true;
    private volatile ServerSocket brokerListenerSocket;
    private volatile ServerSocket clientListenerSocket;
    private volatile Thread brokerListenerThread;
    private volatile Thread clientListenerThread;
    private volatile Thread reaperThread;

    /**
     * Main entry point.
     *
     * @param args voters CSV followed by optional broker/client listener ports
     */
    public static void main(String[] args) {
        if (args.length < 1 || args.length > 3) {
            System.err.println("Usage: DirectoryService <votersCSV> [brokerPort] [clientPort]");
            System.err.println("  votersCSV: id@host:rpcPort[:clientPort],id@host:rpcPort[:clientPort],...");
            System.exit(2);
        }

        int brokerPort = args.length >= 2 ? Integer.parseInt(args[1]) : 60000;
        int clientPort = args.length >= 3 ? Integer.parseInt(args[2]) : 60001;
        Map<Integer, RaftPeerEndpoint> voters = parseVoters(args[0]);

        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: DIRECTORY SERVICE---");
        System.out.println("Configured cluster voters: " + voters.keySet());

        DirectoryService service = new DirectoryService(voters);
        try {
            service.start(brokerPort, clientPort);
        } catch (IOException | RuntimeException e) {
            service.stop();
            System.err.println("Directory Service startup failed: " + e.getMessage());
            System.exit(1);
        }
    }

    public DirectoryService(Map<Integer, RaftPeerEndpoint> clusterVoters) {
        this(
                clusterVoters,
                System::currentTimeMillis,
                DEFAULT_HEARTBEAT_TIMEOUT_MS,
                DEFAULT_REAPER_INTERVAL_MS,
                true);
    }

    public DirectoryService() {
        this(Collections.emptyMap());
    }

    DirectoryService(
            Map<Integer, RaftPeerEndpoint> clusterVoters,
            LongSupplier clock,
            long heartbeatTimeoutMs,
            long reaperIntervalMs,
            boolean startReaper) {
        this.clusterVoters = clusterVoters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(clusterVoters));
        this.clock = Objects.requireNonNull(clock, "clock");
        if (heartbeatTimeoutMs <= 0L || reaperIntervalMs <= 0L) {
            throw new IllegalArgumentException("Directory timing values must be > 0");
        }
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.reaperIntervalMs = reaperIntervalMs;
        if (startReaper) {
            startReaperThread();
        }
    }

    /**
     * Starts both public listeners as one lifecycle transaction. Both sockets
     * are bound before either endpoint or accept thread is published. A failure
     * therefore leaves no half-started Directory service behind.
     */
    public void start(int brokerPort, int clientPort) throws IOException {
        ServerSocket brokerSocket = null;
        ServerSocket clientSocket = null;
        try {
            brokerSocket = new ServerSocket(brokerPort);
            clientSocket = new ServerSocket(clientPort);
            ServerSocket boundBrokerSocket = brokerSocket;
            ServerSocket boundClientSocket = clientSocket;

            synchronized (lifecycleLock) {
                if (!running) {
                    throw new IOException("Directory Service has been stopped");
                }
                if (brokerListenerSocket != null || clientListenerSocket != null) {
                    throw new IllegalStateException("Directory Service listeners already started");
                }

                brokerListenerSocket = brokerSocket;
                clientListenerSocket = clientSocket;
                brokerListenerThread = new Thread(
                        () -> runBrokerListener(boundBrokerSocket, true),
                        "Dir-BrokerListener");
                clientListenerThread = new Thread(
                        () -> runClientListener(boundClientSocket, true),
                        "Dir-ClientListener");
                brokerListenerThread.start();
                clientListenerThread.start();
            }
        } catch (IOException | RuntimeException e) {
            closeQuietly(clientSocket);
            closeQuietly(brokerSocket);
            stop();
            throw e;
        }
    }

    /** Backwards-compatible single listener entry point used by focused tests. */
    public void startBrokersListener(int port) {
        try {
            ServerSocket socket = new ServerSocket(port);
            synchronized (lifecycleLock) {
                if (!running || brokerListenerSocket != null) {
                    closeQuietly(socket);
                    return;
                }
                brokerListenerSocket = socket;
                brokerListenerThread = Thread.currentThread();
            }
            runBrokerListener(socket, false);
        } catch (IOException e) {
            if (running) {
                System.err.println("Directory Service error: " + e.getMessage());
            }
        }
    }

    /** Backwards-compatible single listener entry point used by focused tests. */
    public void startClientsListener(int port) {
        try {
            ServerSocket socket = new ServerSocket(port);
            synchronized (lifecycleLock) {
                if (!running || clientListenerSocket != null) {
                    closeQuietly(socket);
                    return;
                }
                clientListenerSocket = socket;
                clientListenerThread = Thread.currentThread();
            }
            runClientListener(socket, false);
        } catch (IOException e) {
            if (running) {
                System.err.println("Directory Service client listener error: " + e.getMessage());
            }
        }
    }

    /** Stops listeners, handlers and the reaper, waiting for bounded teardown. */
    public void stop() {
        Thread brokerListener;
        Thread clientListener;
        Thread reaper;
        synchronized (lifecycleLock) {
            running = false;
            brokerListener = brokerListenerThread;
            clientListener = clientListenerThread;
            reaper = reaperThread;
        }

        closeQuietly(brokerListenerSocket);
        closeQuietly(clientListenerSocket);
        closeActiveConnections();
        if (reaper != null) {
            reaper.interrupt();
        }

        List<Thread> listeners = new ArrayList<>(2);
        listeners.add(brokerListener);
        listeners.add(clientListener);
        joinThreadsBounded(listeners, 1_000L);

        // No listener can add another handler after the previous join. Close a
        // second snapshot to cover an accept that completed concurrently with
        // the first close, then wait for all currently owned workers.
        closeActiveConnections();
        joinThreadsBounded(new ArrayList<>(connectionThreads), 1_000L);
        joinThreadsBounded(Collections.singletonList(reaper), 1_000L);
    }

    private void runBrokerListener(ServerSocket serverSocket, boolean coupledLifecycle) {
        boolean failed = false;
        brokerListenerReady.countDown();
        System.out.println("Directory Service listening on port " + serverSocket.getLocalPort());
        try (serverSocket) {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    startConnectionHandler(
                            socket,
                            () -> handleBrokerConnection(socket),
                            "Dir-BrokerConnection-" + socket.getRemoteSocketAddress());
                } catch (SocketException e) {
                    if (running) {
                        failed = true;
                        System.err.println("Directory Service broker listener error: "
                                + e.getMessage());
                    }
                    break;
                }
            }
        } catch (IOException | RuntimeException e) {
            if (running) {
                failed = true;
                System.err.println("Directory Service broker listener error: " + e.getMessage());
            }
        } finally {
            synchronized (lifecycleLock) {
                if (brokerListenerSocket == serverSocket) {
                    brokerListenerSocket = null;
                }
            }
        }

        if (failed && coupledLifecycle) {
            stop();
        }
    }

    private void runClientListener(ServerSocket serverSocket, boolean coupledLifecycle) {
        boolean failed = false;
        clientListenerReady.countDown();
        System.out.println("Directory Service listening for CLIENTS on port "
                + serverSocket.getLocalPort());
        try (serverSocket) {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    startConnectionHandler(
                            socket,
                            () -> handleClientConnection(socket),
                            "Dir-ClientConnection-" + socket.getRemoteSocketAddress());
                } catch (SocketException e) {
                    if (running) {
                        failed = true;
                        System.err.println("Directory Service client listener error: "
                                + e.getMessage());
                    }
                    break;
                }
            }
        } catch (IOException | RuntimeException e) {
            if (running) {
                failed = true;
                System.err.println("Directory Service client listener error: " + e.getMessage());
            }
        } finally {
            synchronized (lifecycleLock) {
                if (clientListenerSocket == serverSocket) {
                    clientListenerSocket = null;
                }
            }
        }

        if (failed && coupledLifecycle) {
            stop();
        }
    }

    private void startConnectionHandler(Socket socket, Runnable action, String threadName) {
        synchronized (lifecycleLock) {
            if (!running) {
                closeQuietly(socket);
                return;
            }

            activeConnections.add(socket);
            Thread handler = new Thread(() -> {
                try {
                    action.run();
                } finally {
                    activeConnections.remove(socket);
                    connectionThreads.remove(Thread.currentThread());
                }
            }, threadName);
            handler.setDaemon(true);
            connectionThreads.add(handler);
            try {
                handler.start();
            } catch (RuntimeException e) {
                connectionThreads.remove(handler);
                activeConnections.remove(socket);
                closeQuietly(socket);
                throw e;
            }
        }
    }

    private void handleClientConnection(Socket socket) {
        try (socket;
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Object obj = in.readObject();
            if (obj instanceof GetBrokerRequestMessage request) {
                handleGetBrokerRequest(out, request);
            } else {
                System.out.println("Unknown client request object: " + obj);
            }
        } catch (IOException | ClassNotFoundException e) {
            if (running) {
                System.err.println("Error handling client directory request: " + e.getMessage());
            }
        }
    }

    private void handleGetBrokerRequest(
            ObjectOutputStream out,
            GetBrokerRequestMessage request
    ) throws IOException {
        BrokerRecord best = chooseBestBroker(request.getExcludedBrokerIds());
        GetBrokerResponseMessage response;
        if (best == null) {
            response = new GetBrokerResponseMessage(false, null, -1, -1);
        } else {
            response = new GetBrokerResponseMessage(
                    true,
                    best.host(),
                    best.port(),
                    best.brokerId());
        }
        out.writeObject(response);
        out.flush();
    }

    private void handleBrokerConnection(Socket socket) {
        RegistrationLease lease = null;
        try (socket; ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            Object first = in.readObject();
            if (first instanceof GetClusterRequestMessage request) {
                handleGetClusterRequest(socket, request);
                return;
            }
            if (!(first instanceof DirectoryRegisterMessage registration)) {
                System.out.println("Unknown first object from " + socket.getRemoteSocketAddress()
                        + ": " + first);
                return;
            }

            lease = registerBroker(registration);
            while (running) {
                Object obj = in.readObject();
                if (obj instanceof HeartbeatMessage) {
                    recordHeartbeat(lease);
                } else if (obj instanceof ClientCountUpdateMessage update) {
                    updateClientCount(lease, update);
                } else {
                    System.out.println("Unknown object from broker " + lease.brokerId() + ": " + obj);
                }
            }
        } catch (IOException e) {
            if (running) {
                System.err.println("Connection with broker died: " + e.getMessage());
            }
        } catch (ClassNotFoundException e) {
            System.err.println("Unknown class from broker: " + e.getMessage());
        } finally {
            activeConnections.remove(socket);
            if (lease != null) {
                deactivateLease(lease);
            }
        }
    }

    private void handleGetClusterRequest(Socket socket, GetClusterRequestMessage request)
            throws IOException {
        int nodeId = request.getNodeId();
        boolean ok = !clusterVoters.isEmpty() && clusterVoters.containsKey(nodeId);
        GetClusterResponseMessage response = new GetClusterResponseMessage(
                ok,
                ok ? clusterVoters : Collections.emptyMap());

        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(response);
        out.flush();
    }

    RegistrationLease registerBroker(DirectoryRegisterMessage message) {
        Objects.requireNonNull(message, "message");
        long epoch = registrationEpoch.incrementAndGet();
        RegistrationLease lease = new RegistrationLease(
                epoch,
                message.getBrokerId(),
                message.getBrokerHost(),
                message.getBrokerPort());
        BrokerRecord record = lease.toRecord(0, clock.getAsLong());

        synchronized (registryLock) {
            brokerSlots.put(message.getBrokerId(), new BrokerSlot(epoch, record));
            registryLock.notifyAll();
        }

        System.out.println("Registered broker: id=" + record.brokerId() + " "
                + record.host() + ":" + record.port() + ", clientCount=0, epoch=" + epoch);
        return lease;
    }

    void recordHeartbeat(RegistrationLease lease) {
        long now = clock.getAsLong();
        synchronized (registryLock) {
            BrokerSlot slot = brokerSlots.get(lease.brokerId());
            if (slot == null || slot.epoch() != lease.epoch()) {
                return;
            }
            int clientCount = slot.active() == null ? 0 : slot.active().clientCount();
            brokerSlots.put(
                    lease.brokerId(),
                    new BrokerSlot(lease.epoch(), lease.toRecord(clientCount, now)));
            registryLock.notifyAll();
        }
    }

    private void deactivateLease(RegistrationLease lease) {
        synchronized (registryLock) {
            BrokerSlot slot = brokerSlots.get(lease.brokerId());
            if (slot != null && slot.epoch() == lease.epoch()) {
                brokerSlots.put(lease.brokerId(), new BrokerSlot(slot.epoch(), null));
                registryLock.notifyAll();
            }
        }
    }

    private void updateClientCount(RegistrationLease lease, ClientCountUpdateMessage update) {
        if (update.getBrokerId() != lease.brokerId()) {
            return;
        }
        long now = clock.getAsLong();
        synchronized (registryLock) {
            BrokerSlot slot = brokerSlots.get(lease.brokerId());
            if (slot == null || slot.epoch() != lease.epoch()) {
                return;
            }
            brokerSlots.put(
                    lease.brokerId(),
                    new BrokerSlot(lease.epoch(), lease.toRecord(update.getClientCount(), now)));
            registryLock.notifyAll();
        }
    }

    private void startReaperThread() {
        Thread thread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(reaperIntervalMs);
                    reapExpiredBrokers();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Directory-Reaper");
        thread.setDaemon(true);
        reaperThread = thread;
        thread.start();
    }

    void reapExpiredBrokers() {
        long now = clock.getAsLong();
        synchronized (registryLock) {
            for (Map.Entry<Integer, BrokerSlot> entry : brokerSlots.entrySet()) {
                BrokerSlot slot = entry.getValue();
                BrokerRecord record = slot.active();
                if (record != null && now - record.lastHeartbeatMillis() > heartbeatTimeoutMs) {
                    System.out.println("Broker " + record.brokerId()
                            + " considered DEAD. Removing active endpoint from directory.");
                    // Preserve the epoch tombstone. A late heartbeat from this same
                    // connection may reactivate it, while an older generation can
                    // never overwrite a newer registration.
                    entry.setValue(new BrokerSlot(slot.epoch(), null));
                    registryLock.notifyAll();
                }
            }
        }
    }

    private BrokerRecord chooseBestBroker(Set<Integer> excludedBrokerIds) {
        synchronized (registryLock) {
            BrokerRecord best = null;
            for (BrokerSlot slot : brokerSlots.values()) {
                BrokerRecord candidate = slot.active();
                if (candidate == null || excludedBrokerIds.contains(candidate.brokerId())) {
                    continue;
                }
                if (best == null
                        || candidate.clientCount() < best.clientCount()
                        || (candidate.clientCount() == best.clientCount()
                        && candidate.brokerId() < best.brokerId())) {
                    best = candidate;
                }
            }
            return best;
        }
    }

    BrokerRecord brokerRecordForTesting(int brokerId) {
        synchronized (registryLock) {
            BrokerSlot slot = brokerSlots.get(brokerId);
            return slot == null ? null : slot.active();
        }
    }

    int activeBrokerCountForTesting() {
        synchronized (registryLock) {
            int count = 0;
            for (BrokerSlot slot : brokerSlots.values()) {
                if (slot.active() != null) {
                    count++;
                }
            }
            return count;
        }
    }

    boolean awaitActiveBrokerForTesting(
            int brokerId,
            String expectedHost,
            int expectedPort,
            long timeout,
            TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remainingNanos;
        synchronized (registryLock) {
            while (remainingNanos > 0L) {
                BrokerSlot slot = brokerSlots.get(brokerId);
                BrokerRecord record = slot == null ? null : slot.active();
                if (record != null
                        && expectedHost.equals(record.host())
                        && expectedPort == record.port()) {
                    return true;
                }
                TimeUnit.NANOSECONDS.timedWait(registryLock, remainingNanos);
                remainingNanos = deadline - System.nanoTime();
            }
            return false;
        }
    }

    int getBoundBrokerPortForTesting() {
        ServerSocket socket = brokerListenerSocket;
        return socket == null ? -1 : socket.getLocalPort();
    }

    int getBoundClientPortForTesting() {
        ServerSocket socket = clientListenerSocket;
        return socket == null ? -1 : socket.getLocalPort();
    }

    boolean awaitBrokerListenerReady(long timeout, TimeUnit unit) throws InterruptedException {
        return brokerListenerReady.await(timeout, unit);
    }

    boolean awaitClientListenerReady(long timeout, TimeUnit unit) throws InterruptedException {
        return clientListenerReady.await(timeout, unit);
    }

    boolean isReaperAliveForTesting() {
        Thread thread = reaperThread;
        return thread != null && thread.isAlive();
    }

    int activeConnectionCountForTesting() {
        return activeConnections.size();
    }

    private void closeActiveConnections() {
        for (Socket socket : new ArrayList<>(activeConnections)) {
            try {
                socket.setSoLinger(true, 0);
            } catch (SocketException ignored) {
            }
            closeQuietly(socket);
        }
    }

    private static void joinThreadsBounded(Iterable<Thread> threads, long timeoutMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        for (Thread thread : threads) {
            if (thread == null || thread == Thread.currentThread()) {
                continue;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void closeQuietly(ServerSocket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static Map<Integer, RaftPeerEndpoint> parseVoters(String csv) {
        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            int at = trimmed.indexOf('@');
            if (at <= 0) {
                throw new IllegalArgumentException("Bad voter token: '" + trimmed
                        + "' (expected id@host:rpcPort[:clientPort])");
            }

            int id = Integer.parseInt(trimmed.substring(0, at));
            String[] parts = trimmed.substring(at + 1).split(":");
            if (parts.length != 2 && parts.length != 3) {
                throw new IllegalArgumentException("Bad voter token: '" + trimmed
                        + "' (expected id@host:rpcPort[:clientPort])");
            }

            String host = parts[0];
            int rpcPort = Integer.parseInt(parts[1]);
            int clientPort = parts.length == 3 ? Integer.parseInt(parts[2]) : 50000 + id;
            voters.put(id, new RaftPeerEndpoint(id, host, rpcPort, clientPort));
        }
        return voters;
    }

    private record BrokerSlot(long epoch, BrokerRecord active) {
    }

    record RegistrationLease(long epoch, int brokerId, String host, int port) {
        BrokerRecord toRecord(int clientCount, long timestamp) {
            return new BrokerRecord(brokerId, host, port, clientCount, timestamp);
        }
    }

    record BrokerRecord(
            int brokerId,
            String host,
            int port,
            int clientCount,
            long lastHeartbeatMillis) {
        BrokerRecord withClientCount(int newClientCount, long timestamp) {
            return new BrokerRecord(brokerId, host, port, newClientCount, timestamp);
        }
    }
}
