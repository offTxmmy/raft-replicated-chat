package it.polimi.ds.chat.directory;

import it.polimi.ds.chat.client.discovery.ClientDirectory;
import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.directory.DirectoryRegisterMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectoryServiceTest {

    @Test
    void supersededRegistrationSocketIsClosedSoLiveBrokerCanReregister() throws Exception {
        DirectoryService directory = startDirectory();
        try (var old = register(directory.getBoundBrokerPortForTesting(), 7, "old", 50007)) {
            assertTrue(directory.awaitActiveBrokerForTesting(7, "old", 50007, 1, TimeUnit.SECONDS));
            try (var current = register(directory.getBoundBrokerPortForTesting(), 7, "new", 51007)) {
                assertTrue(directory.awaitActiveBrokerForTesting(7, "new", 51007, 1, TimeUnit.SECONDS));
                old.socket().setSoTimeout(1000);
                old.output().writeObject(new it.polimi.ds.chat.protocol.client.HeartbeatMessage(1L));
                old.output().flush();
                assertEquals(-1, old.socket().getInputStream().read(),
                        "superseded live connection keeps writing heartbeats that Directory silently ignores forever");
                assertEquals("new", directory.brokerRecordForTesting(7).host());
            }
        }
    }

    @Test
    void delayedRegistrationCannotOverwriteNewerEpochOrDeactivateIt() throws Exception {
        var oldAllocated = new java.util.concurrent.CountDownLatch(1);
        var releaseOld = new java.util.concurrent.CountDownLatch(1);
        var first = new java.util.concurrent.atomic.AtomicBoolean(true);
        AtomicLong now = new AtomicLong(100);
        DirectoryService directory = new DirectoryService(Map.of(), () -> {
            if (first.compareAndSet(true, false)) {
                oldAllocated.countDown();
                try { assertTrue(releaseOld.await(2, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new RuntimeException(e); }
            }
            return now.get();
        }, 10, 10, false);
        var worker = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var oldRegistration = worker.submit(() -> directory.registerBroker(
                    new DirectoryRegisterMessage(7, "old", 50007)));
            assertTrue(oldAllocated.await(1, TimeUnit.SECONDS));
            var current = directory.registerBroker(new DirectoryRegisterMessage(7, "new", 51007));
            releaseOld.countDown();
            var obsolete = oldRegistration.get(1, TimeUnit.SECONDS);
            assertEquals("new", directory.brokerRecordForTesting(7).host(), "late epoch replaced current registration");
            directory.recordHeartbeat(obsolete);
            var deactivate = DirectoryService.class.getDeclaredMethod("deactivateLease", DirectoryService.RegistrationLease.class);
            deactivate.setAccessible(true);
            deactivate.invoke(directory, obsolete);
            assertEquals("new", directory.brokerRecordForTesting(7).host());
            now.set(200);
            directory.reapExpiredBrokers();
            directory.recordHeartbeat(current);
            assertEquals("new", directory.brokerRecordForTesting(7).host(), "current connection can no longer renew its lease");
        } finally { releaseOld.countDown(); worker.shutdownNow(); directory.stop(); }
    }

    private DirectoryService runningService;

    @AfterEach
    void stopService() {
        if (runningService != null) {
            runningService.stop();
        }
    }

    @Test
    void sameBrokerIdAtomicallyReplacesEndpointAndRejectsOldGenerationHeartbeat() {
        AtomicLong now = new AtomicLong(100L);
        DirectoryService directory = new DirectoryService(
                Map.of(), now::get, 10L, 10L, false);

        DirectoryService.RegistrationLease oldLease = directory.registerBroker(
                new DirectoryRegisterMessage(7, "old-host", 50007));
        now.set(105L);
        DirectoryService.RegistrationLease newLease = directory.registerBroker(
                new DirectoryRegisterMessage(7, "new-host", 51007));

        assertEquals(1, directory.activeBrokerCountForTesting());
        assertEquals("new-host", directory.brokerRecordForTesting(7).host());
        assertEquals(51007, directory.brokerRecordForTesting(7).port());

        now.set(200L);
        directory.recordHeartbeat(oldLease);
        assertEquals(105L, directory.brokerRecordForTesting(7).lastHeartbeatMillis(),
                "an obsolete connection must not mutate the replacement record");

        directory.reapExpiredBrokers();
        assertNull(directory.brokerRecordForTesting(7));

        directory.recordHeartbeat(oldLease);
        assertNull(directory.brokerRecordForTesting(7),
                "the old generation must not resurrect a replacement");

        directory.recordHeartbeat(newLease);
        DirectoryService.BrokerRecord restored = directory.brokerRecordForTesting(7);
        assertEquals("new-host", restored.host());
        assertEquals(51007, restored.port());
        assertEquals(200L, restored.lastHeartbeatMillis());
    }

    @Test
    void heartbeatBeforeAtomicReapKeepsCurrentRecordAlive() {
        AtomicLong now = new AtomicLong(0L);
        DirectoryService directory = new DirectoryService(
                Map.of(), now::get, 10L, 10L, false);
        DirectoryService.RegistrationLease lease = directory.registerBroker(
                new DirectoryRegisterMessage(1, "host", 50001));

        now.set(11L);
        directory.recordHeartbeat(lease);
        directory.reapExpiredBrokers();

        assertEquals(11L, directory.brokerRecordForTesting(1).lastHeartbeatMillis());
    }

    @Test
    void lookupReturnsOnlyTheReplacementEndpoint() throws Exception {
        DirectoryService directory = startDirectory();
        try (RegistrationConnection first = register(
                directory.getBoundBrokerPortForTesting(), 2, "old", 50002);
             RegistrationConnection replacement = register(
                     directory.getBoundBrokerPortForTesting(), 2, "new", 51002)) {
            assertTrue(directory.awaitActiveBrokerForTesting(
                    2, "new", 51002, 1, TimeUnit.SECONDS));

            ClientDirectory.BrokerInfo info = new ClientDirectory(
                    "127.0.0.1",
                    directory.getBoundClientPortForTesting()).getBestBroker();

            assertEquals(2, info.getBrokerId());
            assertEquals("new", info.getHost());
            assertEquals(51002, info.getPort());
        }
    }

    @Test
    void lookupExclusionsSelectTheNextReachableBroker() throws Exception {
        DirectoryService directory = startDirectory();
        try (RegistrationConnection preferred = register(
                directory.getBoundBrokerPortForTesting(), 1, "preferred", 50001);
             RegistrationConnection alternate = register(
                     directory.getBoundBrokerPortForTesting(), 2, "alternate", 50002)) {
            assertTrue(directory.awaitActiveBrokerForTesting(
                    1, "preferred", 50001, 1, TimeUnit.SECONDS));
            assertTrue(directory.awaitActiveBrokerForTesting(
                    2, "alternate", 50002, 1, TimeUnit.SECONDS));

            ClientDirectory clientDirectory = new ClientDirectory(
                    "127.0.0.1",
                    directory.getBoundClientPortForTesting());
            assertEquals(1, clientDirectory.getBestBroker().getBrokerId());

            ClientDirectory.BrokerInfo fallback =
                    clientDirectory.getBestBroker(Set.of(1));
            assertEquals(2, fallback.getBrokerId());
            assertEquals("alternate", fallback.getHost());
            assertEquals(50002, fallback.getPort());
        }
    }

    @Test
    void brokerPublishesAndBindsConfiguredClientPort(
            @TempDir Path tempDir) throws Exception {
        DirectoryService directory = new DirectoryService(Map.of());
        runningService = directory;
        directory.start(0, 0);
        int directoryBrokerPort = directory.getBoundBrokerPortForTesting();

        int clientPort;
        try (ServerSocket clientReservation = new ServerSocket(0)) {
            clientPort = clientReservation.getLocalPort();
        }

        RaftConfig raft = new RaftConfig(
                150,
                300,
                30,
                7001,
                tempDir,
                Map.of(1, new RaftPeerEndpoint(
                        1, "127.0.0.1", 7001, clientPort)));
        BrokerConfig config = new BrokerConfig(
                1,
                "127.0.0.1",
                clientPort,
                raft,
                "127.0.0.1",
                directoryBrokerPort);
        FastDirectoryBroker broker = new FastDirectoryBroker(config);
        broker.setOrderingService(new NoOpOrderingService());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "test-client-endpoint-publication");

        try {
            brokerThread.start();
            assertTrue(directory.awaitActiveBrokerForTesting(
                    1, "127.0.0.1", clientPort, 5, TimeUnit.SECONDS));
            try (Socket connected = new Socket("127.0.0.1", clientPort)) {
                assertTrue(connected.isConnected());
            }
        } finally {
            broker.stop();
            brokerThread.join(2_000L);
        }

        assertFalse(brokerThread.isAlive());
        assertNull(failure.get());
    }

    @Test
    void brokerCanRegisterAgainAfterDirectoryRestartOnTheSamePort() throws Exception {
        int brokerPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            brokerPort = reservation.getLocalPort();
        }

        DirectoryService first = new DirectoryService(Map.of());
        Thread firstListener = startBrokerListener(first, brokerPort);
        assertTrue(first.awaitBrokerListenerReady(1, TimeUnit.SECONDS));

        try (RegistrationConnection ignored = register(brokerPort, 3, "broker", 50003)) {
            assertTrue(first.awaitActiveBrokerForTesting(
                    3, "broker", 50003, 1, TimeUnit.SECONDS));
        }

        first.stop();
        firstListener.join(1_000L);
        assertFalse(firstListener.isAlive());

        DirectoryService second = new DirectoryService(Map.of());
        runningService = second;
        Thread secondListener = startBrokerListener(second, brokerPort);
        assertTrue(second.awaitBrokerListenerReady(1, TimeUnit.SECONDS));

        try (RegistrationConnection ignored = register(brokerPort, 3, "broker", 50003)) {
            assertTrue(second.awaitActiveBrokerForTesting(
                    3, "broker", 50003, 1, TimeUnit.SECONDS));
        }
        second.stop();
        secondListener.join(1_000L);
        assertFalse(secondListener.isAlive());
    }

    @Test
    void transactionalStartRollsBackFirstBindAndReaperWhenSecondBindFails()
            throws Exception {
        DirectoryService directory = new DirectoryService(Map.of());
        runningService = directory;

        try (ServerSocket brokerReservation = new ServerSocket(0);
             ServerSocket occupiedClientPort = new ServerSocket(0)) {
            int brokerPort = brokerReservation.getLocalPort();
            brokerReservation.close();

            assertThrows(IOException.class, () -> directory.start(
                    brokerPort,
                    occupiedClientPort.getLocalPort()));

            assertFalse(directory.awaitBrokerListenerReady(100, TimeUnit.MILLISECONDS));
            assertFalse(directory.awaitClientListenerReady(100, TimeUnit.MILLISECONDS));
            assertEquals(-1, directory.getBoundBrokerPortForTesting());
            assertEquals(-1, directory.getBoundClientPortForTesting());
            assertFalse(directory.isReaperAliveForTesting());

            // The first bind must have been rolled back even though it had
            // already succeeded before the client-port failure.
            try (ServerSocket rebound = new ServerSocket(brokerPort)) {
                assertEquals(brokerPort, rebound.getLocalPort());
            }
        }
    }

    @Test
    void transactionalStopWaitsForBothListenersAndReaper() throws Exception {
        DirectoryService directory = new DirectoryService(Map.of());
        runningService = directory;
        directory.start(0, 0);

        assertTrue(directory.awaitBrokerListenerReady(1, TimeUnit.SECONDS));
        assertTrue(directory.awaitClientListenerReady(1, TimeUnit.SECONDS));
        assertTrue(directory.isReaperAliveForTesting());

        directory.stop();

        assertEquals(-1, directory.getBoundBrokerPortForTesting());
        assertEquals(-1, directory.getBoundClientPortForTesting());
        assertEquals(0, directory.activeConnectionCountForTesting());
        assertFalse(directory.isReaperAliveForTesting());
    }

    @Test
    void brokerEventuallyReregistersAfterDirectoryRestart(@TempDir Path tempDir)
            throws Exception {
        DirectoryService first = new DirectoryService(Map.of());
        runningService = first;
        first.start(0, 0);
        assertTrue(first.awaitBrokerListenerReady(1, TimeUnit.SECONDS));
        assertTrue(first.awaitClientListenerReady(1, TimeUnit.SECONDS));
        int directoryBrokerPort = first.getBoundBrokerPortForTesting();
        int directoryClientPort = first.getBoundClientPortForTesting();

        int clientPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            clientPort = reservation.getLocalPort();
        }
        RaftConfig raft = new RaftConfig(
                150,
                300,
                30,
                7001,
                tempDir,
                Map.of(1, new RaftPeerEndpoint(
                        1, "127.0.0.1", 7001, clientPort)));
        BrokerConfig config = new BrokerConfig(
                1,
                "127.0.0.1",
                clientPort,
                raft,
                "127.0.0.1",
                directoryBrokerPort);
        FastDirectoryBroker broker = new FastDirectoryBroker(config);
        broker.setOrderingService(new NoOpOrderingService());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread brokerThread = new Thread(() -> {
            try {
                broker.start();
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "test-broker-directory-reconnect");

        DirectoryService second = null;
        try {
            brokerThread.start();
            assertTrue(first.awaitActiveBrokerForTesting(
                    1, "127.0.0.1", clientPort, 5, TimeUnit.SECONDS));

            first.stop();
            second = new DirectoryService(Map.of());
            runningService = second;
            second.start(directoryBrokerPort, directoryClientPort);
            assertTrue(second.awaitBrokerListenerReady(1, TimeUnit.SECONDS));
            assertTrue(second.awaitClientListenerReady(1, TimeUnit.SECONDS));

            assertTrue(second.awaitActiveBrokerForTesting(
                    1, "127.0.0.1", clientPort, 5, TimeUnit.SECONDS),
                    "the broker must reconnect and publish its endpoint again");
        } finally {
            broker.stop();
            if (second != null) {
                second.stop();
            } else {
                first.stop();
            }
            brokerThread.join(2_000L);
        }

        assertFalse(brokerThread.isAlive());
        assertNull(failure.get());
    }

    private DirectoryService startDirectory() throws Exception {
        DirectoryService directory = new DirectoryService(Map.of(
                0, new RaftPeerEndpoint(0, "127.0.0.1", 7000, 50000)));
        runningService = directory;

        Thread brokerListener = startBrokerListener(directory, 0);
        Thread clientListener = new Thread(
                () -> directory.startClientsListener(0),
                "test-directory-client-listener");
        clientListener.start();
        assertTrue(directory.awaitBrokerListenerReady(1, TimeUnit.SECONDS));
        assertTrue(directory.awaitClientListenerReady(1, TimeUnit.SECONDS));
        assertTrue(brokerListener.isAlive());
        assertTrue(clientListener.isAlive());
        return directory;
    }

    private static Thread startBrokerListener(DirectoryService directory, int port) {
        Thread listener = new Thread(
                () -> directory.startBrokersListener(port),
                "test-directory-broker-listener");
        listener.start();
        return listener;
    }

    private static RegistrationConnection register(
            int directoryPort,
            int brokerId,
            String host,
            int clientPort) throws IOException {
        Socket socket = new Socket("127.0.0.1", directoryPort);
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(new DirectoryRegisterMessage(brokerId, host, clientPort));
        out.flush();
        return new RegistrationConnection(socket, out);
    }

    private record RegistrationConnection(Socket socket, ObjectOutputStream output)
            implements AutoCloseable {
        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static final class FastDirectoryBroker extends Broker {
        private FastDirectoryBroker(BrokerConfig config) {
            super(config);
        }

        @Override
        protected long directoryHeartbeatIntervalMs() {
            return 25L;
        }

        @Override
        protected long directoryReconnectDelayMs() {
            return 25L;
        }
    }

    private static final class NoOpOrderingService implements OrderingService {
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
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public int getLeaderId() {
            return 1;
        }
    }
}
