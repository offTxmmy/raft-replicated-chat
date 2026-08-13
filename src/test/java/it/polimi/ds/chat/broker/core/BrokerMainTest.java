package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.directory.GetClusterRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetClusterResponseMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerMainTest {

    @Test
    void raftCommandKeepsHybridAsDefault() {
        assertEquals(RaftTransportMode.HYBRID, BrokerMain.transportModeForCommand("raft"));
    }

    @Test
    void raftLocalCommandSelectsTcpOnlyTransport() {
        assertEquals(RaftTransportMode.LOCAL_TCP,
                BrokerMain.transportModeForCommand("raft-local"));
    }

    @Test
    void unknownCommandIsRejected() {
        assertNull(BrokerMain.transportModeForCommand("unknown"));
    }

    @Test
    void hybridDirectoryEndpointDefaultsRemainBackwardsCompatible() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft", "1", "7001"},
                RaftTransportMode.HYBRID);

        assertEquals("localhost", endpoint.host());
        assertEquals(60000, endpoint.port());
    }

    @Test
    void hybridDirectoryEndpointIsReadAfterTransportOptions() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{
                        "raft", "1", "7001", "51001", "7100", "demo", "1300",
                        "192.0.2.10", "62000"
                },
                RaftTransportMode.HYBRID);

        assertEquals("192.0.2.10", endpoint.host());
        assertEquals(62000, endpoint.port());
    }

    @Test
    void localTcpDirectoryEndpointDoesNotDependOnHybridOptions() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft-local", "2", "7002", "51002", "directory.lan", "62001"},
                RaftTransportMode.LOCAL_TCP);

        assertEquals("directory.lan", endpoint.host());
        assertEquals(62001, endpoint.port());
    }

    @Test
    void invalidDirectoryEndpointIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                BrokerMain.directoryEndpointForArgs(
                        new String[]{"raft-local", "2", "7002", "51002", "", "62001"},
                        RaftTransportMode.LOCAL_TCP));
        assertThrows(IllegalArgumentException.class, () ->
                BrokerMain.directoryEndpointForArgs(
                        new String[]{"raft-local", "2", "7002", "51002", "host", "0"},
                        RaftTransportMode.LOCAL_TCP));
    }

    @Test
    void omittedClientPortUsesDirectoryVoterEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7002, 52345));

        assertEquals(52345, BrokerMain.resolveClientPort(2, null, voters));
    }

    @Test
    void explicitClientPortOverridesDirectoryVoterEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7002, 52345));

        assertEquals(53333, BrokerMain.resolveClientPort(2, 53333, voters));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.resolveClientPort(2, 0, voters));
    }

    @Test
    void rpcPortMustMatchTheStaticVoterEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7002, 52345));

        BrokerMain.validateRpcPort(2, 7002, voters);
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.validateRpcPort(2, 7999, voters));
    }

    @Test
    void normalDirectoryBootstrapReturnsStaticVoters() throws Exception {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7_002, 50_002));
        try (OneShotClusterServer server =
                     OneShotClusterServer.respondingWith(voters)) {
            assertEquals(voters, BrokerMain.fetchVotersFromDirectory(
                    2, "127.0.0.1", server.port(), 250, 250));
            assertTrue(server.requestReceived.await(1L, TimeUnit.SECONDS));
            server.awaitSuccessfulCompletion();
        }
    }

    @Test
    void directoryConnectFailureIsBounded() throws Exception {
        int unusedPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            unusedPort = reservation.getLocalPort();
        }

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertThrows(IOException.class, () ->
                        BrokerMain.fetchVotersFromDirectory(
                                1, "127.0.0.1", unusedPort, 100, 100)));
    }

    @Test
    void directoryObjectStreamHandshakeIsBounded() throws Exception {
        try (OneShotClusterServer server =
                     OneShotClusterServer.stallingBeforeHandshake()) {
            IOException failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, () ->
                            BrokerMain.fetchVotersFromDirectory(
                                    1, "127.0.0.1", server.port(), 250, 100))
            );

            assertInstanceOf(SocketTimeoutException.class, failure);
            assertTrue(server.accepted.await(1L, TimeUnit.SECONDS));
        }
    }

    @Test
    void directoryResponseReadIsBounded() throws Exception {
        try (OneShotClusterServer server =
                     OneShotClusterServer.stallingAfterRequest()) {
            IOException failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, () ->
                            BrokerMain.fetchVotersFromDirectory(
                                    1, "127.0.0.1", server.port(), 250, 100))
            );

            assertInstanceOf(SocketTimeoutException.class, failure);
            assertTrue(server.requestReceived.await(1L, TimeUnit.SECONDS));
        }
    }

    private static final class OneShotClusterServer implements AutoCloseable {
        private final ServerSocket listener;
        private final Map<Integer, RaftPeerEndpoint> response;
        private final boolean stallBeforeHandshake;
        private final CountDownLatch accepted = new CountDownLatch(1);
        private final CountDownLatch requestReceived = new CountDownLatch(1);
        private final CountDownLatch releaseStall = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final Thread thread;

        private volatile Socket acceptedSocket;
        private volatile Throwable failure;
        private volatile boolean closing;

        private OneShotClusterServer(
                Map<Integer, RaftPeerEndpoint> response,
                boolean stallBeforeHandshake
        ) throws IOException {
            this.listener = new ServerSocket(0);
            this.response = response;
            this.stallBeforeHandshake = stallBeforeHandshake;
            this.thread = new Thread(this::serve, "one-shot-cluster-directory");
            thread.setDaemon(true);
            thread.start();
        }

        private static OneShotClusterServer respondingWith(
                Map<Integer, RaftPeerEndpoint> response
        ) throws IOException {
            return new OneShotClusterServer(response, false);
        }

        private static OneShotClusterServer stallingBeforeHandshake()
                throws IOException {
            return new OneShotClusterServer(null, true);
        }

        private static OneShotClusterServer stallingAfterRequest()
                throws IOException {
            return new OneShotClusterServer(null, false);
        }

        private int port() {
            return listener.getLocalPort();
        }

        private void serve() {
            try (Socket socket = listener.accept()) {
                acceptedSocket = socket;
                accepted.countDown();
                if (stallBeforeHandshake) {
                    releaseStall.await();
                    return;
                }

                ObjectOutputStream output =
                        new ObjectOutputStream(socket.getOutputStream());
                output.flush();
                ObjectInputStream input =
                        new ObjectInputStream(socket.getInputStream());
                try (output; input) {
                    Object request = input.readObject();
                    if (!(request instanceof GetClusterRequestMessage)) {
                        throw new IOException("Unexpected Directory request " + request);
                    }
                    requestReceived.countDown();

                    if (response == null) {
                        releaseStall.await();
                    } else {
                        output.writeObject(new GetClusterResponseMessage(
                                true, response));
                        output.flush();
                    }
                }
            } catch (SocketException e) {
                if (!closing) {
                    failure = e;
                }
            } catch (Throwable e) {
                failure = e;
            } finally {
                accepted.countDown();
                requestReceived.countDown();
                completed.countDown();
            }
        }

        private void awaitSuccessfulCompletion() throws Exception {
            assertTrue(completed.await(1L, TimeUnit.SECONDS));
            if (failure != null) {
                throw new AssertionError("Directory test server failed", failure);
            }
        }

        @Override
        public void close() throws Exception {
            closing = true;
            releaseStall.countDown();
            listener.close();
            Socket socket = acceptedSocket;
            if (socket != null) {
                socket.close();
            }
            thread.join(1_000L);
        }
    }
}
