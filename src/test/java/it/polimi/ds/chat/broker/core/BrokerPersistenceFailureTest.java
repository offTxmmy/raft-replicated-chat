package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.client.discovery.ClientDirectory;
import it.polimi.ds.chat.directory.DirectoryService;
import it.polimi.ds.chat.ordering.raft.*;
import it.polimi.ds.chat.ordering.raft.config.*;
import it.polimi.ds.chat.protocol.raft.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class BrokerPersistenceFailureTest {
    @Test
    void udpTermWriteFailureWithdrawsWholeBroker(@TempDir Path dir) throws Exception {
        exercise(dir, true);
    }

    @Test
    void tcpCommitWriteFailureWithdrawsWholeBroker(@TempDir Path dir) throws Exception {
        exercise(dir, false);
    }

    private static void exercise(Path dir, boolean udpFailure) throws Exception {
        int rpcPort = freePort(), clientPort = freePort();
        int directoryBrokerPort = freePort(), directoryClientPort = freePort();
        int udpPort;
        try (DatagramSocket socket = new DatagramSocket(0)) { udpPort = socket.getLocalPort(); }
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                1, new RaftPeerEndpoint(1, "127.0.0.1", rpcPort, clientPort),
                2, new RaftPeerEndpoint(2, "127.0.0.1", freePort(), freePort()));
        DirectoryService directory = new DirectoryService(voters);
        directory.start(directoryBrokerPort, directoryClientPort);
        CountDownLatch stopped = new CountDownLatch(1);
        Broker broker = new Broker(new BrokerConfig(1, "127.0.0.1", clientPort,
                new RaftConfig(20_000, 30_000, 100, rpcPort,
                        udpFailure ? RaftTransportMode.HYBRID : RaftTransportMode.LOCAL_TCP,
                        udpPort, RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES, "fatal-test", dir, voters),
                "127.0.0.1", directoryBrokerPort)) {
            @Override public void stop() { super.stop(); stopped.countDown(); }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> serving = executor.submit(() -> { try { broker.start(); } catch (IOException e) { throw new RuntimeException(e); } });
        try {
            assertTrue(broker.awaitClientListenerReady(3, TimeUnit.SECONDS));
            var registered = DirectoryService.class.getDeclaredMethod("awaitActiveBrokerForTesting",
                    int.class, String.class, int.class, long.class, TimeUnit.class);
            registered.setAccessible(true);
            assertTrue((boolean) registered.invoke(directory, 1, "127.0.0.1", clientPort, 3L, TimeUnit.SECONDS));
            try (Socket session = new Socket("127.0.0.1", clientPort)) {
                session.setSoTimeout(4000);
                ObjectOutputStream clientOut = new ObjectOutputStream(session.getOutputStream());
                clientOut.flush();
                new ObjectInputStream(session.getInputStream());
                // A directory at the tmp-file path deterministically rejects the write on every OS.
                Files.createDirectory(dir.resolve(udpFailure ? "state.bin.tmp" : "commit.bin.tmp"));
                if (udpFailure) {
                    RaftUdpEnvelope envelope = new RaftUdpEnvelope("fatal-test", "failure-1", 2, 1,
                            RaftUdpMessageType.REQUEST_VOTE_REQUEST, 1,
                            new RequestVoteRequestMessage(1, 2, 0, 0), 1);
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) { out.writeObject(envelope); }
                    try (DatagramSocket socket = new DatagramSocket()) {
                        byte[] data = bytes.toByteArray();
                        socket.send(new DatagramPacket(data, data.length, InetAddress.getLoopbackAddress(), udpPort));
                    }
                } else {
                    try (Socket rpc = new Socket("127.0.0.1", rpcPort)) {
                        ObjectOutputStream out = new ObjectOutputStream(rpc.getOutputStream());
                        out.writeObject(new AppendEntriesRequestMessage(1, 2, 0, 0,
                                List.of(new RaftLogEntry(1, 1, null)), 1));
                        out.flush();
                        rpc.setSoTimeout(4000);
                        ObjectInputStream in = new ObjectInputStream(rpc.getInputStream());
                        assertThrows(IOException.class, in::readObject, "failed commit must not return AppendEntries success");
                    }
                }
                assertTrue(stopped.await(5, TimeUnit.SECONDS), "fatal storage error left broker partially alive");
                assertEquals(-1, session.getInputStream().read(), "client session remained open");
            }
            serving.get(3, TimeUnit.SECONDS);
            assertFalse(broker.getOrderingService().isLeader());
            assertThrows(IOException.class, () -> { try (Socket ignored = new Socket("127.0.0.1", clientPort)) { } });
            // Directory's socket handler processes EOF asynchronously, so use a bounded condition wait.
            ClientDirectory lookup = new ClientDirectory("127.0.0.1", directoryClientPort);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (lookup.getBestBroker() != null && System.nanoTime() < deadline) Thread.sleep(10);
            assertNull(lookup.getBestBroker(), "failed broker remains advertised");
            try (FileRaftPersistence nextOwner = new FileRaftPersistence(dir)) {
                assertNotNull(nextOwner.loadTermAndVote(), "teardown did not release storage ownership");
            }
        } finally { broker.stop(); directory.stop(); executor.shutdownNow(); }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }
}
