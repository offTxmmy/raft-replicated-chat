package it.polimi.ds.chat.integration;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.directory.DirectoryService;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.protocol.client.ClientAckMessages;
import it.polimi.ds.chat.protocol.client.ClientJoinMessage;
import it.polimi.ds.chat.protocol.directory.GetBrokerRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetBrokerResponseMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Application-level integration coverage using the real Directory, Broker,
 * Raft TCP transport and client TCP protocol. All nodes are separate runtime
 * objects but deliberately share one JVM and the loopback interface.
 */
class ReplicatedChatApplicationIntegrationTest {

    private static final String LOOPBACK = "127.0.0.1";
    private static final long ELECTION_TIMEOUT_MIN_MS = 300L;
    private static final long ELECTION_TIMEOUT_MAX_MS = 600L;
    private static final long HEARTBEAT_INTERVAL_MS = 60L;
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(12L);
    private static final Duration MESSAGE_TIMEOUT = Duration.ofSeconds(8L);
    private static final Duration ABSENCE_WINDOW = Duration.ofMillis(500L);

    @Test
    void realSocketsPreserveJoinBoundaryForwardingAndOrderingAcrossLeaderFailure(
            @TempDir Path storageRoot) throws Exception {
        try (ApplicationCluster cluster = ApplicationCluster.start(storageRoot)) {
            int originalLeader = cluster.awaitLeaderAgreement(cluster.liveNodeIds(), -1);
            List<Integer> followers = cluster.liveNodeIds().stream()
                    .filter(nodeId -> nodeId != originalLeader)
                    .sorted()
                    .toList();
            assertEquals(2, followers.size(), "a three-node cluster must expose two followers");

            GetBrokerResponseMessage directoryRoute = cluster.awaitDirectoryRoute();
            assertTrue(directoryRoute.isAvailable(), "Directory did not publish any live broker");
            assertTrue(cluster.clientPorts().contains(directoryRoute.getBrokerPort()),
                    "Directory returned an endpoint outside the configured cluster");

            int senderBrokerId = followers.get(0);
            int observerBrokerId = followers.get(1);
            assertFalse(cluster.broker(senderBrokerId).getOrderingService().isLeader(),
                    "sender must initially use a follower to exercise proposal forwarding");
            assertFalse(cluster.broker(observerBrokerId).getOrderingService().isLeader(),
                    "observer must be connected to a different follower");

            try (WireClient sender = WireClient.connect(
                    LOOPBACK, cluster.clientPort(senderBrokerId), "alice", "client-alice")) {
                sender.awaitWelcome();

                sender.sendMessageAndAwaitAck(1L, "committed before observer join");
                sender.assertNoInboundObject(ABSENCE_WINDOW,
                        "the originating client must not receive its own pre-JOIN message");

                try (WireClient observer = WireClient.connect(
                        LOOPBACK, cluster.clientPort(observerBrokerId), "bob", "client-bob")) {
                    observer.awaitWelcome();
                    observer.assertNoInboundObject(ABSENCE_WINDOW,
                            "JOIN boundary leaked a chat committed before this connection became active");

                    sender.sendMessageAndAwaitAck(2L, "visible before failover");
                    observer.awaitExactWireMessage("MSG 2 alice:visible before failover");
                    sender.assertNoInboundObject(ABSENCE_WINDOW,
                            "the originating client received an echo before failover");

                    cluster.stopBroker(originalLeader);
                    Set<Integer> survivors = new HashSet<>(followers);
                    int replacementLeader = cluster.awaitLeaderAgreement(survivors, originalLeader);
                    assertNotEquals(originalLeader, replacementLeader,
                            "the stopped leader cannot remain authoritative");

                    sender.sendMessageAndAwaitAck(3L, "visible after failover");
                    observer.awaitExactWireMessage("MSG 3 alice:visible after failover");
                    sender.assertNoInboundObject(ABSENCE_WINDOW,
                            "the originating client received an echo after failover");
                }
            }
        }
    }

    @Test
    void realSocketsPreserveCausalAndConcurrentTotalOrderAcrossBrokers(
            @TempDir Path storageRoot) throws Exception {
        try (ApplicationCluster cluster = ApplicationCluster.start(storageRoot)) {
            cluster.awaitLeaderAgreement(cluster.liveNodeIds(), -1);

            GetBrokerResponseMessage aliceRoute = cluster.awaitDirectoryRouteForBroker(0);
            GetBrokerResponseMessage bobRoute = cluster.awaitDirectoryRouteForBroker(1);
            GetBrokerResponseMessage firstObserverRoute = cluster.awaitDirectoryRouteForBroker(2);
            GetBrokerResponseMessage secondObserverRoute = cluster.awaitDirectoryRouteForBroker(0);

            assertNotEquals(aliceRoute.getBrokerId(), bobRoute.getBrokerId(),
                    "causally related senders must exercise different receiving brokers");
            assertEquals(Set.of(0, 1, 2), Set.of(
                    aliceRoute.getBrokerId(),
                    bobRoute.getBrokerId(),
                    firstObserverRoute.getBrokerId()),
                    "the scenario must exercise all three brokers through Directory routing");

            try (WireClient alice = WireClient.connect(
                    aliceRoute.getBrokerHost(), aliceRoute.getBrokerPort(),
                    "alice", "client-alice");
                 WireClient bob = WireClient.connect(
                         bobRoute.getBrokerHost(), bobRoute.getBrokerPort(),
                         "bob", "client-bob");
                 WireClient observerOne = WireClient.connect(
                         firstObserverRoute.getBrokerHost(), firstObserverRoute.getBrokerPort(),
                         "observer-one", "client-observer-one");
                 WireClient observerTwo = WireClient.connect(
                         secondObserverRoute.getBrokerHost(), secondObserverRoute.getBrokerPort(),
                         "observer-two", "client-observer-two")) {
                alice.awaitWelcome();
                bob.awaitWelcome();
                observerOne.awaitWelcome();
                observerTwo.awaitWelcome();

                alice.sendMessageAndAwaitAck(1L, "causal-m1");
                bob.awaitExactWireMessage("MSG 1 alice:causal-m1");

                // Bob sends only after its real socket reader delivered m1. This
                // creates the application-level causal chain m1 -> m2.
                bob.sendMessageAndAwaitAck(1L, "causal-m2");
                alice.awaitExactWireMessage("MSG 2 bob:causal-m2");
                observerOne.awaitExactWireMessages(List.of(
                        "MSG 1 alice:causal-m1",
                        "MSG 2 bob:causal-m2"));
                observerTwo.awaitExactWireMessages(List.of(
                        "MSG 1 alice:causal-m1",
                        "MSG 2 bob:causal-m2"));

                alice.assertNoInboundObject(ABSENCE_WINDOW,
                        "alice received a self-echo or duplicate in the causal phase");
                bob.assertNoInboundObject(ABSENCE_WINDOW,
                        "bob received a self-echo or duplicate in the causal phase");
                observerOne.assertNoInboundObject(ABSENCE_WINDOW,
                        "first observer received a duplicate in the causal phase");
                observerTwo.assertNoInboundObject(ABSENCE_WINDOW,
                        "second observer received a duplicate in the causal phase");

                sendConcurrently(
                        alice, 2L, "concurrent-alice",
                        bob, 2L, "concurrent-bob");

                List<String> firstOrder = observerOne.awaitWireMessages(2);
                List<String> secondOrder = observerTwo.awaitWireMessages(2);
                assertEquals(firstOrder, secondOrder,
                        "common observers disagreed on the order of concurrent messages");
                assertConcurrentPair(firstOrder);

                assertSenderConcurrentOutcome(
                        alice.awaitObjects(2),
                        "client-alice", 2L,
                        "bob", "concurrent-bob");
                assertSenderConcurrentOutcome(
                        bob.awaitObjects(2),
                        "client-bob", 2L,
                        "alice", "concurrent-alice");

                alice.assertNoInboundObject(ABSENCE_WINDOW,
                        "alice received a self-echo or duplicate in the concurrent phase");
                bob.assertNoInboundObject(ABSENCE_WINDOW,
                        "bob received a self-echo or duplicate in the concurrent phase");
                observerOne.assertNoInboundObject(ABSENCE_WINDOW,
                        "first observer received a duplicate in the concurrent phase");
                observerTwo.assertNoInboundObject(ABSENCE_WINDOW,
                        "second observer received a duplicate in the concurrent phase");
            }
        }
    }

    private static void sendConcurrently(WireClient first,
                                         long firstSequence,
                                         String firstText,
                                         WireClient second,
                                         long secondSequence,
                                         String secondText) throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread firstSender = concurrentSender(
                "ApplicationE2E-ConcurrentSender-1",
                first, firstSequence, firstText,
                ready, release, completed, failure);
        Thread secondSender = concurrentSender(
                "ApplicationE2E-ConcurrentSender-2",
                second, secondSequence, secondText,
                ready, release, completed, failure);
        firstSender.start();
        secondSender.start();

        assertTrue(ready.await(3L, TimeUnit.SECONDS),
                "concurrent senders did not reach the launch barrier");
        release.countDown();
        assertTrue(completed.await(3L, TimeUnit.SECONDS),
                "concurrent sender writes did not complete");
        firstSender.join(1_000L);
        secondSender.join(1_000L);
        assertFalse(firstSender.isAlive() || secondSender.isAlive(),
                "concurrent sender thread did not terminate");
        if (failure.get() != null) {
            throw new AssertionError("concurrent client send failed", failure.get());
        }
    }

    private static Thread concurrentSender(String threadName,
                                           WireClient client,
                                           long clientSequence,
                                           String text,
                                           CountDownLatch ready,
                                           CountDownLatch release,
                                           CountDownLatch completed,
                                           AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            ready.countDown();
            try {
                if (!release.await(3L, TimeUnit.SECONDS)) {
                    throw new AssertionError("concurrent send launch barrier timed out");
                }
                client.sendMessage(clientSequence, text);
            } catch (Throwable sendFailure) {
                failure.compareAndSet(null, sendFailure);
            } finally {
                completed.countDown();
            }
        }, threadName);
        thread.setDaemon(true);
        return thread;
    }

    private static void assertConcurrentPair(List<String> messages) {
        assertEquals(2, messages.size(), "observers must receive exactly two concurrent messages");
        Set<Long> sequences = new HashSet<>();
        Set<String> payloads = new HashSet<>();
        for (String message : messages) {
            String[] parts = message.split(" ", 3);
            assertEquals(3, parts.length, "malformed client-visible message: " + message);
            assertEquals("MSG", parts[0], "unexpected client-visible object: " + message);
            sequences.add(Long.parseLong(parts[1]));
            payloads.add(parts[2]);
        }
        assertEquals(Set.of(3L, 4L), sequences,
                "concurrent messages were lost, duplicated or assigned non-contiguous order");
        assertEquals(Set.of("alice:concurrent-alice", "bob:concurrent-bob"), payloads,
                "concurrent messages were lost or duplicated");
    }

    private static void assertSenderConcurrentOutcome(List<Object> objects,
                                                      String clientId,
                                                      long clientSequence,
                                                      String remoteSender,
                                                      String remoteText) {
        String expectedAck = ClientAckMessages.buildAck(clientId, clientSequence);
        int ackCount = 0;
        int remoteMessageCount = 0;
        for (Object object : objects) {
            if (expectedAck.equals(object)) {
                ackCount++;
            } else if (object instanceof String line
                    && line.matches("MSG [34] "
                    + java.util.regex.Pattern.quote(remoteSender + ":" + remoteText))) {
                remoteMessageCount++;
            } else {
                fail("sender received a self-echo, duplicate or unexpected object: " + object);
            }
        }
        assertEquals(1, ackCount, "sender did not receive exactly one matching ACK");
        assertEquals(1, remoteMessageCount,
                "sender did not receive exactly one copy of the remote concurrent message");
    }

    private static final class ApplicationCluster implements AutoCloseable {
        private final TcpPortReservations reservations;
        private final DirectoryService directory;
        private final int directoryClientPort;
        private final int[] clientPorts;
        private final Broker[] brokers;
        private final Thread[] brokerThreads;
        private final List<Throwable> startupFailures = new CopyOnWriteArrayList<>();
        private final Set<Integer> stoppedBrokers = new HashSet<>();

        private ApplicationCluster(TcpPortReservations reservations,
                                   DirectoryService directory,
                                   int directoryClientPort,
                                   int[] clientPorts,
                                   Broker[] brokers,
                                   Thread[] brokerThreads) {
            this.reservations = reservations;
            this.directory = directory;
            this.directoryClientPort = directoryClientPort;
            this.clientPorts = clientPorts;
            this.brokers = brokers;
            this.brokerThreads = brokerThreads;
        }

        static ApplicationCluster start(Path storageRoot) throws Exception {
            TcpPortReservations reservations = TcpPortReservations.reserve(8);
            DirectoryService directory = null;
            Broker[] brokers = new Broker[3];
            Thread[] threads = new Thread[3];
            ApplicationCluster cluster = null;
            try {
                int directoryBrokerPort = reservations.port(0);
                int directoryClientPort = reservations.port(1);
                int[] rpcPorts = {reservations.port(2), reservations.port(3), reservations.port(4)};
                int[] clientPorts = {reservations.port(5), reservations.port(6), reservations.port(7)};

                Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
                for (int nodeId = 0; nodeId < 3; nodeId++) {
                    voters.put(nodeId, new RaftPeerEndpoint(
                            nodeId, LOOPBACK, rpcPorts[nodeId], clientPorts[nodeId]));
                }

                directory = new DirectoryService(voters);
                reservations.release(0);
                reservations.release(1);
                directory.start(directoryBrokerPort, directoryClientPort);

                for (int nodeId = 0; nodeId < 3; nodeId++) {
                    Path nodeStorage = storageRoot.resolve("n" + nodeId);
                    Files.createDirectories(nodeStorage);
                    RaftConfig raftConfig = new RaftConfig(
                            ELECTION_TIMEOUT_MIN_MS,
                            ELECTION_TIMEOUT_MAX_MS,
                            HEARTBEAT_INTERVAL_MS,
                            rpcPorts[nodeId],
                            RaftTransportMode.LOCAL_TCP,
                            RaftConfig.DEFAULT_RAFT_BROADCAST_PORT,
                            RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES,
                            "application-e2e-" + storageRoot.getFileName(),
                            nodeStorage,
                            voters);
                    brokers[nodeId] = new Broker(new BrokerConfig(
                            nodeId,
                            LOOPBACK,
                            clientPorts[nodeId],
                            clientPorts[nodeId],
                            freeUdpPort(),
                            raftConfig,
                            LOOPBACK,
                            directoryBrokerPort));
                }

                ApplicationCluster startedCluster = new ApplicationCluster(
                        reservations,
                        directory,
                        directoryClientPort,
                        clientPorts,
                        brokers,
                        threads);
                cluster = startedCluster;

                for (int reservationIndex = 2; reservationIndex < 8; reservationIndex++) {
                    reservations.release(reservationIndex);
                }
                for (int nodeId = 0; nodeId < 3; nodeId++) {
                    final int id = nodeId;
                    Thread thread = new Thread(() -> {
                        try {
                            brokers[id].start();
                        } catch (Throwable failure) {
                            startedCluster.startupFailures.add(failure);
                        }
                    }, "ApplicationE2E-Broker-" + nodeId);
                    thread.setDaemon(true);
                    threads[nodeId] = thread;
                    thread.start();
                }
                return cluster;
            } catch (Throwable failure) {
                if (cluster != null) {
                    cluster.close();
                } else {
                    for (Broker broker : brokers) {
                        if (broker != null) {
                            broker.stop();
                        }
                    }
                    if (directory != null) {
                        directory.stop();
                    }
                    reservations.close();
                }
                throw failure;
            }
        }

        Broker broker(int nodeId) {
            return brokers[nodeId];
        }

        int clientPort(int nodeId) {
            return clientPorts[nodeId];
        }

        Set<Integer> liveNodeIds() {
            Set<Integer> ids = new HashSet<>();
            for (int nodeId = 0; nodeId < brokers.length; nodeId++) {
                if (!stoppedBrokers.contains(nodeId)) {
                    ids.add(nodeId);
                }
            }
            return ids;
        }

        Set<Integer> clientPorts() {
            Set<Integer> ports = new HashSet<>();
            for (int port : clientPorts) {
                ports.add(port);
            }
            return ports;
        }

        int awaitLeaderAgreement(Set<Integer> participants, int excludedLeader) throws Exception {
            AtomicReference<Integer> agreedLeader = new AtomicReference<>(-1);
            boolean agreed = awaitCondition(() -> {
                if (!startupFailures.isEmpty()) {
                    return true;
                }
                int leader = -1;
                int leaderCount = 0;
                for (int nodeId : participants) {
                    if (brokers[nodeId].getOrderingService().isLeader()) {
                        leader = nodeId;
                        leaderCount++;
                    }
                }
                if (leaderCount != 1 || leader == excludedLeader) {
                    return false;
                }
                for (int nodeId : participants) {
                    if (brokers[nodeId].getOrderingService().getLeaderId() != leader) {
                        return false;
                    }
                }
                agreedLeader.set(leader);
                return true;
            }, STARTUP_TIMEOUT);
            assertNoStartupFailure();
            assertTrue(agreed, "Raft nodes did not converge on one live leader; states=" + raftStates());
            return agreedLeader.get();
        }

        GetBrokerResponseMessage awaitDirectoryRoute() throws Exception {
            AtomicReference<GetBrokerResponseMessage> response = new AtomicReference<>();
            boolean available = awaitCondition(() -> {
                try {
                    GetBrokerResponseMessage current = requestDirectoryRoute(directoryClientPort);
                    if (current.isAvailable()) {
                        response.set(current);
                        return true;
                    }
                } catch (IOException ignored) {
                    // Guarded retry while asynchronous broker registrations arrive.
                }
                return false;
            }, STARTUP_TIMEOUT);
            assertNoStartupFailure();
            assertTrue(available, "Directory never returned a registered broker");
            return response.get();
        }

        GetBrokerResponseMessage awaitDirectoryRouteForBroker(int brokerId) throws Exception {
            Set<Integer> excluded = new HashSet<>(liveNodeIds());
            excluded.remove(brokerId);
            AtomicReference<GetBrokerResponseMessage> response = new AtomicReference<>();
            boolean available = awaitCondition(() -> {
                try {
                    GetBrokerResponseMessage current = requestDirectoryRoute(
                            directoryClientPort, excluded);
                    if (current.isAvailable() && current.getBrokerId() == brokerId) {
                        response.set(current);
                        return true;
                    }
                } catch (IOException ignored) {
                    // Guarded retry while asynchronous broker registrations arrive.
                }
                return false;
            }, STARTUP_TIMEOUT);
            assertNoStartupFailure();
            assertTrue(available, "Directory never returned broker " + brokerId);
            GetBrokerResponseMessage route = response.get();
            assertEquals(clientPorts[brokerId], route.getBrokerPort(),
                    "Directory returned the wrong client endpoint for broker " + brokerId);
            return route;
        }

        void stopBroker(int nodeId) throws InterruptedException {
            stoppedBrokers.add(nodeId);
            brokers[nodeId].stop();
            Thread thread = brokerThreads[nodeId];
            if (thread != null) {
                thread.join(3_000L);
                assertFalse(thread.isAlive(), "stopped broker thread did not terminate: " + nodeId);
            }
        }

        private void assertNoStartupFailure() {
            if (!startupFailures.isEmpty()) {
                AssertionError error = new AssertionError("broker startup failed");
                startupFailures.forEach(error::addSuppressed);
                throw error;
            }
        }

        private String raftStates() {
            List<String> states = new ArrayList<>();
            for (int nodeId : liveNodeIds()) {
                states.add(nodeId + "={leader="
                        + brokers[nodeId].getOrderingService().isLeader()
                        + ", knownLeader="
                        + brokers[nodeId].getOrderingService().getLeaderId() + "}");
            }
            return states.toString();
        }

        @Override
        public void close() {
            for (int nodeId = 0; nodeId < brokers.length; nodeId++) {
                brokers[nodeId].stop();
            }
            directory.stop();
            reservations.close();
            for (Thread thread : brokerThreads) {
                if (thread == null || thread == Thread.currentThread()) {
                    continue;
                }
                try {
                    thread.join(3_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static final class WireClient implements Closeable {
        private final String username;
        private final String clientId;
        private final Socket socket;
        private final ObjectOutputStream out;
        private final ObjectInputStream in;
        private final BlockingQueue<Object> inbound = new LinkedBlockingQueue<>();
        private final AtomicBoolean closing = new AtomicBoolean(false);
        private final AtomicReference<Throwable> readFailure = new AtomicReference<>();
        private final Thread readerThread;

        private WireClient(String host, int port, String username, String clientId) throws Exception {
            this.username = username;
            this.clientId = clientId;
            this.socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), 3_000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5_000);
            this.out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            try {
                this.in = new ObjectInputStream(socket.getInputStream());
            } catch (SocketTimeoutException e) {
                throw new IOException("broker did not complete the object-stream handshake", e);
            }
            socket.setSoTimeout(0);
            this.readerThread = new Thread(this::readLoop, "ApplicationE2E-ClientReader-" + username);
            readerThread.setDaemon(true);
            readerThread.start();
        }

        static WireClient connect(String host, int port, String username, String clientId) throws Exception {
            WireClient client = new WireClient(host, port, username, clientId);
            client.write(ClientJoinMessage.joinCommand(username, clientId));
            return client;
        }

        void awaitWelcome() throws Exception {
            Object welcome = awaitNext(MESSAGE_TIMEOUT, "WELCOME for " + username);
            assertEquals(ClientJoinMessage.welcome(username), welcome);
        }

        void sendMessageAndAwaitAck(long clientSequence, String text) throws Exception {
            sendMessage(clientSequence, text);
            List<Object> preceding = new ArrayList<>();
            Object ack = awaitInbound(obj -> {
                if (!(obj instanceof String line) || !ClientAckMessages.isAck(line)) {
                    preceding.add(obj);
                    return false;
                }
                return clientId.equals(ClientAckMessages.parseClientId(line))
                        && clientSequence == ClientAckMessages.parseClientSeq(line);
            }, MESSAGE_TIMEOUT, "ACK " + clientId + " " + clientSequence);
            assertEquals(ClientAckMessages.buildAck(clientId, clientSequence), ack);
            assertTrue(preceding.isEmpty(),
                    "sender received unexpected data before its ACK: " + preceding);
        }

        void sendMessage(long clientSequence, String text) throws IOException {
            write("MSG " + clientId + " " + clientSequence + " " + text);
        }

        void awaitExactWireMessage(String expected) throws Exception {
            Object actual = awaitNext(MESSAGE_TIMEOUT, expected);
            assertEquals(expected, actual);
        }

        void awaitExactWireMessages(List<String> expected) throws Exception {
            assertEquals(expected, awaitWireMessages(expected.size()));
        }

        List<String> awaitWireMessages(int count) throws Exception {
            List<Object> objects = awaitObjects(count);
            List<String> messages = new ArrayList<>(count);
            for (Object object : objects) {
                assertTrue(object instanceof String,
                        "expected a client-visible wire string, received=" + object);
                messages.add((String) object);
            }
            return messages;
        }

        List<Object> awaitObjects(int count) throws Exception {
            long deadline = System.nanoTime() + MESSAGE_TIMEOUT.toNanos();
            List<Object> objects = new ArrayList<>(count);
            while (objects.size() < count && System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                Object object = inbound.poll(Math.min(
                        TimeUnit.NANOSECONDS.toMillis(remaining) + 1L,
                        200L), TimeUnit.MILLISECONDS);
                if (object != null) {
                    objects.add(object);
                }
                assertReadHealthy();
            }
            assertEquals(count, objects.size(),
                    "timed out waiting for " + count + " wire objects; received=" + objects);
            return objects;
        }

        void assertNoInboundObject(Duration duration, String message) throws Exception {
            Object unexpected = inbound.poll(duration.toMillis(), TimeUnit.MILLISECONDS);
            assertReadHealthy();
            assertEquals(null, unexpected, message + "; received=" + unexpected);
        }

        private Object awaitNext(Duration timeout, String description) throws Exception {
            Object next = inbound.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            assertReadHealthy();
            if (next == null) {
                fail("Timed out waiting for " + description);
            }
            return next;
        }

        private Object awaitInbound(Predicate<Object> predicate,
                                    Duration timeout,
                                    String description) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            List<Object> unmatched = new ArrayList<>();
            while (System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                Object obj = inbound.poll(Math.min(
                        TimeUnit.NANOSECONDS.toMillis(remaining) + 1L,
                        200L), TimeUnit.MILLISECONDS);
                if (obj != null) {
                    if (predicate.test(obj)) {
                        return obj;
                    }
                    unmatched.add(obj);
                }
                assertReadHealthy();
            }
            fail("Timed out waiting for " + description + "; unmatched=" + unmatched);
            return null;
        }

        private void write(Object object) throws IOException {
            synchronized (out) {
                out.writeObject(object);
                out.flush();
                out.reset();
            }
        }

        private void readLoop() {
            try {
                while (!closing.get()) {
                    inbound.put(in.readObject());
                }
            } catch (EOFException | SocketException e) {
                if (!closing.get()) {
                    readFailure.compareAndSet(null, e);
                }
            } catch (Throwable failure) {
                if (!closing.get()) {
                    readFailure.compareAndSet(null, failure);
                }
            }
        }

        private void assertReadHealthy() {
            Throwable failure = readFailure.get();
            if (failure != null) {
                throw new AssertionError("client reader failed for " + username, failure);
            }
        }

        @Override
        public void close() throws IOException {
            closing.set(true);
            socket.close();
            try {
                readerThread.join(1_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static GetBrokerResponseMessage requestDirectoryRoute(int directoryClientPort)
            throws IOException {
        return requestDirectoryRoute(directoryClientPort, Set.of());
    }

    private static GetBrokerResponseMessage requestDirectoryRoute(
            int directoryClientPort,
            Set<Integer> excludedBrokerIds) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(LOOPBACK, directoryClientPort), 1_000);
            socket.setSoTimeout(2_000);
            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                out.flush();
                try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                    out.writeObject(new GetBrokerRequestMessage(excludedBrokerIds));
                    out.flush();
                    Object response = in.readObject();
                    assertTrue(response instanceof GetBrokerResponseMessage,
                            "unexpected Directory response: " + response);
                    return (GetBrokerResponseMessage) response;
                } catch (ClassNotFoundException e) {
                    throw new IOException("unknown Directory response type", e);
                }
            }
        }
    }

    private static boolean awaitCondition(BooleanSupplier condition, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(25L);
        }
        return condition.getAsBoolean();
    }

    private static int freeUdpPort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getByName(LOOPBACK))) {
            return socket.getLocalPort();
        }
    }

    /** Keeps all selected TCP ports bound until the component that owns them starts. */
    private static final class TcpPortReservations implements Closeable {
        private final ServerSocket[] sockets;
        private final int[] ports;

        private TcpPortReservations(ServerSocket[] sockets, int[] ports) {
            this.sockets = sockets;
            this.ports = ports;
        }

        static TcpPortReservations reserve(int count) throws IOException {
            ServerSocket[] sockets = new ServerSocket[count];
            int[] ports = new int[count];
            try {
                for (int i = 0; i < count; i++) {
                    sockets[i] = new ServerSocket(0, 50, InetAddress.getByName(LOOPBACK));
                    ports[i] = sockets[i].getLocalPort();
                }
                return new TcpPortReservations(sockets, ports);
            } catch (IOException e) {
                Arrays.stream(sockets)
                        .filter(socket -> socket != null)
                        .forEach(socket -> {
                            try {
                                socket.close();
                            } catch (IOException ignored) {
                            }
                        });
                throw e;
            }
        }

        int port(int index) {
            return ports[index];
        }

        void release(int index) throws IOException {
            ServerSocket socket = sockets[index];
            if (socket != null) {
                socket.close();
                sockets[index] = null;
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < sockets.length; i++) {
                try {
                    release(i);
                } catch (IOException ignored) {
                }
            }
        }
    }
}
