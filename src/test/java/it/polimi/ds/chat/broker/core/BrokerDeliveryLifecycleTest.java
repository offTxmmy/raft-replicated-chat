package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.TestConfigs;
import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.session.ClientHandler;
import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.client.ClientJoinMessage;
import it.polimi.ds.chat.protocol.client.ClientQuitMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerDeliveryLifecycleTest {

    @Test
    void joinBoundaryActivatesInsideApplyAndQueuesWelcomeBeforeFirstChat() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        BlockingBoundaryOrderingService ordering = new BlockingBoundaryOrderingService();
        broker.setOrderingService(ordering);
        RecordingClientHandler handler = new RecordingClientHandler(broker);
        AtomicBoolean activated = new AtomicBoolean(false);

        // Model the next committed entry being applied immediately after the
        // boundary callback, before establishDeliveryBoundary returns to JOIN.
        ordering.afterApplied = () -> broker.handleOrderedMessage(delivery(2L, 0, "new"));

        Thread join = new Thread(() -> activated.set(broker.activateClient(handler)), "test-join");
        join.start();
        assertTrue(ordering.boundaryEntered.await(1, TimeUnit.SECONDS));

        // This represents a command committed before JOIN but applied by this
        // lagging broker while the JOIN fence is still pending.
        broker.handleOrderedMessage(delivery(1L, 0, "old"));
        assertEquals(List.of(), handler.outbound);

        ordering.releaseBoundary.countDown();
        join.join(1_000L);
        assertFalse(join.isAlive());
        assertTrue(activated.get());

        assertEquals(
                List.of(ClientJoinMessage.welcome("anonymous"), "2:new"),
                handler.outbound,
                "WELCOME must be queued by the boundary callback before the next chat");
        assertEquals(1, ordering.boundaryIds.size());
        assertTrue(ordering.boundaryIds.get(0).startsWith("join:1:"));
    }

    @Test
    void outgoingVectorClockAdvancesOnlyForEachActuallyReleasedMessage() {
        ClockObservingBroker broker = new ClockObservingBroker(
                TestConfigs.raftBrokerConfig(1, 5000));

        VectorClock secondClock = new VectorClock();
        secondClock.increment(2);
        secondClock.increment(2);
        broker.handleOrderedMessage(new ChatDeliverMessage(
                2L, 2, "bob", "client-b", "second", secondClock));

        assertEquals(0, broker.getSendVectorClock().getTimeStamp(2),
                "a held-back message must not leak causal knowledge");
        assertEquals(List.of(), broker.observations);

        VectorClock firstClock = new VectorClock();
        firstClock.increment(1);
        broker.handleOrderedMessage(new ChatDeliverMessage(
                1L, 1, "alice", "client-a", "first", firstClock));

        assertEquals(
                List.of("1:1:0", "2:1:2"),
                broker.observations,
                "each released prefix element must be merged before visibility");
    }

    @Test
    void clientPortBindFailureRollsBackBeforeDirectoryPublication(@TempDir Path tempDir)
            throws Exception {
        try (ServerSocket occupied = new ServerSocket(0)) {
            int clientPort = occupied.getLocalPort();
            RaftConfig raft = new RaftConfig(
                    150,
                    300,
                    30,
                    7000,
                    tempDir,
                    Map.of(0, new RaftPeerEndpoint(0, "127.0.0.1", 7000, clientPort)));
            BrokerConfig config = new BrokerConfig(
                    0,
                    "127.0.0.1",
                    clientPort,
                    raft,
                    "127.0.0.1",
                    62000);
            Broker broker = new Broker(config);
            ImmediateBoundaryOrderingService ordering = new ImmediateBoundaryOrderingService();
            broker.setOrderingService(ordering);

            assertThrows(IOException.class, broker::start);
            assertTrue(ordering.started.get());
            assertTrue(ordering.stopped.get(), "partial startup must stop the ordering service");
            assertFalse(broker.isDirectoryRegistrationLoopStartedForTesting(),
                    "the endpoint must never be published before a successful bind");
        }
    }

    @Test
    void stopClosesAcceptedSessionThatNeverJoined() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(new ImmediateBoundaryOrderingService());

        try (ServerSocket listener = new ServerSocket(0);
             Socket peer = new Socket("127.0.0.1", listener.getLocalPort());
             Socket accepted = listener.accept()) {
            broker.startClientSession(accepted);

            ObjectOutputStream peerOut = new ObjectOutputStream(peer.getOutputStream());
            peerOut.flush();
            ObjectInputStream peerIn = new ObjectInputStream(peer.getInputStream());
            assertEquals(1, broker.activeSessionCountForTesting());

            assertTimeout(Duration.ofSeconds(2), broker::stop);
            assertEquals(0, broker.activeSessionCountForTesting());
            assertThrows(EOFException.class, peerIn::readObject,
                    "a pre-JOIN peer must observe broker teardown");
        }
    }

    @Test
    void clientStatusTracksOnlyPostJoinActiveRecipients() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(new ImmediateBoundaryOrderingService());

        try (ServerSocket listener = new ServerSocket(0);
             Socket peer = new Socket("127.0.0.1", listener.getLocalPort());
             Socket accepted = listener.accept()) {
            peer.setSoTimeout(2_000);
            broker.startClientSession(accepted);

            ObjectOutputStream peerOut = new ObjectOutputStream(peer.getOutputStream());
            peerOut.flush();
            ObjectInputStream peerIn = new ObjectInputStream(peer.getInputStream());

            assertEquals(List.of(), broker.activeClientUsernamesSnapshot(),
                    "an accepted pre-JOIN session is not an active recipient");
            assertEquals("[Broker 1] Connected clients (0): ",
                    broker.currentClientStatusLine());

            peerOut.writeObject(ClientJoinMessage.joinCommand("alice", "status-client"));
            peerOut.flush();
            assertEquals(ClientJoinMessage.welcome("alice"), peerIn.readObject());

            awaitCondition(() -> broker.activeClientUsernamesSnapshot().equals(List.of("alice")));
            assertEquals("[Broker 1] Connected clients (1): alice",
                    broker.currentClientStatusLine());

            peerOut.writeObject(ClientQuitMessage.quitCommand());
            peerOut.flush();
            awaitCondition(() -> broker.activeClientUsernamesSnapshot().isEmpty());
            assertEquals("[Broker 1] Connected clients (0): ",
                    broker.currentClientStatusLine());
        } finally {
            broker.stop();
        }
    }

    @Test
    void clientStatusSnapshotSortsAndPreservesDuplicateUsernames() {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(new ImmediateBoundaryOrderingService());
        List<NamedClientHandler> handlers = List.of(
                new NamedClientHandler(broker, "zeta"),
                new NamedClientHandler(broker, "alice"),
                new NamedClientHandler(broker, "alice"));

        try {
            for (NamedClientHandler handler : handlers) {
                assertTrue(broker.activateClient(handler));
            }

            assertEquals(List.of("alice", "alice", "zeta"),
                    broker.activeClientUsernamesSnapshot());
        } finally {
            for (NamedClientHandler handler : handlers) {
                handler.closeSession();
            }
            broker.stop();
        }
    }

    @Test
    void clientStatusSnapshotIsSafeDuringConcurrentActivationAndRemoval() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(new ImmediateBoundaryOrderingService());
        List<NamedClientHandler> handlers = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            handlers.add(new NamedClientHandler(broker, "client-" + (i % 20)));
        }

        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean mutationsDone = new AtomicBoolean(false);
        try {
            Future<?> snapshotter = executor.submit(() -> {
                awaitUnchecked(start);
                int inspections = 0;
                do {
                    List<String> snapshot = broker.activeClientUsernamesSnapshot();
                    List<String> sorted = new ArrayList<>(snapshot);
                    sorted.sort(String::compareTo);
                    assertEquals(sorted, snapshot);
                    inspections++;
                } while (!mutationsDone.get() || inspections < 1_000);
            });

            List<Future<?>> mutators = new ArrayList<>();
            for (int worker = 0; worker < 4; worker++) {
                int offset = worker;
                mutators.add(executor.submit(() -> {
                    awaitUnchecked(start);
                    for (int i = offset; i < handlers.size(); i += 4) {
                        NamedClientHandler handler = handlers.get(i);
                        assertTrue(broker.activateClient(handler));
                        Thread.yield();
                        broker.removeClient(handler);
                    }
                }));
            }

            start.countDown();
            for (Future<?> mutator : mutators) {
                mutator.get(3, TimeUnit.SECONDS);
            }
            mutationsDone.set(true);
            snapshotter.get(3, TimeUnit.SECONDS);
            assertEquals(List.of(), broker.activeClientUsernamesSnapshot());
        } finally {
            mutationsDone.set(true);
            executor.shutdownNow();
            for (NamedClientHandler handler : handlers) {
                handler.closeSession();
            }
            broker.stop();
        }
    }

    @Test
    void stopTerminatesClientStatusLoop() throws Exception {
        int clientPort;
        try (ServerSocket availablePort = new ServerSocket(0)) {
            clientPort = availablePort.getLocalPort();
        }

        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, clientPort));
        broker.setOrderingService(new ImmediateBoundaryOrderingService());
        CountDownLatch firstRender = new CountDownLatch(1);
        AtomicReference<String> renderedLine = new AtomicReference<>();
        broker.setClientStatusOutput(line -> {
            renderedLine.set(line);
            firstRender.countDown();
        });
        AtomicReference<Throwable> startupFailure = new AtomicReference<>();
        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Throwable failure) {
                startupFailure.set(failure);
            }
        }, "test-broker-main");

        brokerThread.start();
        try {
            assertTrue(broker.awaitClientListenerReady(2, TimeUnit.SECONDS));
            assertTrue(firstRender.await(2, TimeUnit.SECONDS));
            assertEquals("[Broker 1] Connected clients (0): ", renderedLine.get());
            assertTrue(broker.isClientStatusLoopAliveForTesting());

            assertTimeout(Duration.ofSeconds(2), broker::stop);
            brokerThread.join(2_000L);

            assertFalse(brokerThread.isAlive());
            assertFalse(broker.isClientStatusLoopAliveForTesting());
            assertNull(startupFailure.get());
        } finally {
            broker.stop();
            brokerThread.join(2_000L);
        }
    }

    @Test
    void stopClosesDirectorySocketBeforeWaitingForBlockedWriterLock() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(new ImmediateBoundaryOrderingService());
        CloseSignalSocket socket = new CloseSignalSocket();
        BlockingDirectoryOutputStream output = new BlockingDirectoryOutputStream(socket);
        broker.installDirectoryConnectionForTesting(socket, output);
        AtomicBoolean writeFailed = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            try {
                broker.sendDirectoryObjectForTesting("heartbeat");
            } catch (IOException expected) {
                writeFailed.set(true);
            }
        }, "blocked-directory-writer");
        writer.start();
        assertTrue(output.writeStarted.await(1, TimeUnit.SECONDS));

        assertTimeout(Duration.ofSeconds(2), broker::stop,
                "stop must close the socket without first waiting for directoryLock");
        writer.join(1_000L);

        assertFalse(writer.isAlive());
        assertTrue(socket.closed.get());
        assertTrue(writeFailed.get());
    }

    private static ChatDeliverMessage delivery(long seq, int senderBrokerId, String text) {
        return new ChatDeliverMessage(
                seq,
                senderBrokerId,
                "sender",
                "sender-client",
                text,
                new VectorClock());
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met before timeout");
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("test worker interrupted", e);
        }
    }

    private static class ImmediateBoundaryOrderingService implements OrderingService {
        private final AtomicBoolean started = new AtomicBoolean(false);
        private final AtomicBoolean stopped = new AtomicBoolean(false);

        @Override
        public boolean propose(ChatReqMessage request) {
            return true;
        }

        @Override
        public boolean establishDeliveryBoundary(String boundaryId) {
            return true;
        }

        @Override
        public void onDeliver(Consumer<ChatDeliverMessage> callback) {
        }

        @Override
        public void start() {
            started.set(true);
        }

        @Override
        public void stop() {
            stopped.set(true);
        }

        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public int getLeaderId() {
            return 0;
        }
    }

    private static final class BlockingBoundaryOrderingService
            extends ImmediateBoundaryOrderingService {
        private final CountDownLatch boundaryEntered = new CountDownLatch(1);
        private final CountDownLatch releaseBoundary = new CountDownLatch(1);
        private final List<String> boundaryIds = new ArrayList<>();
        private Runnable afterApplied = () -> { };

        @Override
        public boolean establishDeliveryBoundary(String boundaryId, Runnable onApplied) {
            boundaryIds.add(boundaryId);
            boundaryEntered.countDown();
            try {
                if (!releaseBoundary.await(1, TimeUnit.SECONDS)) {
                    return false;
                }
                onApplied.run();
                afterApplied.run();
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    private static final class RecordingClientHandler extends ClientHandler {
        private final List<String> outbound = new ArrayList<>();

        private RecordingClientHandler(Broker broker) {
            super(new Socket(), broker);
        }

        @Override
        public void sendLine(Object object) {
            outbound.add(object.toString());
        }

        @Override
        public void sendMessageToClient(long seq, String sender, String text) {
            outbound.add(seq + ":" + text);
        }
    }

    private static final class NamedClientHandler extends ClientHandler {
        private final String username;

        private NamedClientHandler(Broker broker, String username) {
            super(new Socket(), broker);
            this.username = username;
        }

        @Override
        public String getUsername() {
            return username;
        }

        @Override
        public void sendLine(Object object) {
            // No socket writer is needed for this active-recipient model test.
        }
    }

    private static final class CloseSignalSocket extends Socket {
        private final CountDownLatch closeSignal = new CountDownLatch(1);
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public synchronized void close() {
            closed.set(true);
            closeSignal.countDown();
        }
    }

    private static final class BlockingDirectoryOutputStream extends ObjectOutputStream {
        private final CloseSignalSocket socket;
        private final CountDownLatch writeStarted = new CountDownLatch(1);

        private BlockingDirectoryOutputStream(CloseSignalSocket socket) throws IOException {
            super();
            this.socket = socket;
        }

        @Override
        protected void writeObjectOverride(Object object) throws IOException {
            writeStarted.countDown();
            try {
                if (!socket.closeSignal.await(1, TimeUnit.SECONDS)) {
                    throw new IOException("socket was not closed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
            throw new SocketException("socket closed");
        }
    }

    private static final class ClockObservingBroker extends Broker {
        private final List<String> observations = new ArrayList<>();

        private ClockObservingBroker(BrokerConfig config) {
            super(config);
        }

        @Override
        public void onChatDeliver(long seq, String sender, String senderClientId, String text) {
            observations.add(seq
                    + ":" + getSendVectorClock().getTimeStamp(1)
                    + ":" + getSendVectorClock().getTimeStamp(2));
        }
    }
}
