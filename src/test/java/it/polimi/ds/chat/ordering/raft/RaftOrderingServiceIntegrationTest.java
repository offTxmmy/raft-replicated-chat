package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.OrderingMode;
import it.polimi.ds.chat.broker.RaftConfig;
import it.polimi.ds.chat.broker.RaftPeerEndpoint;
import it.polimi.ds.chat.messages.ChatDeliverMessage;
import it.polimi.ds.chat.messages.ChatReqMessage;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test for {@link RaftOrderingService}.
 *
 * <p>Three orchestrator instances are started on loopback, each with its own
 * RPC port and storage directory and an identical static voter map. The test
 * verifies that:
 * <ul>
 *   <li>a single leader is elected within a reasonable time window,
 *   <li>a proposal sent to the leader is committed and delivered to every node
 *       in the same total order (sequence 1, same payload).
 * </ul>
 *
 * <p>Timeouts are conservative to keep the test stable under CI load.
 */
class RaftOrderingServiceIntegrationTest {

    private static final long ELECTION_TIMEOUT_MIN_MS = 200;
    private static final long ELECTION_TIMEOUT_MAX_MS = 400;
    private static final long HEARTBEAT_INTERVAL_MS   = 40;

    private static final long LEADER_ELECTION_DEADLINE_MS = 8_000;
    private static final long DELIVERY_DEADLINE_MS        = 8_000;

    private RaftOrderingService[] nodes;

    @AfterEach
    void tearDown() {
        if (nodes == null) return;
        for (RaftOrderingService n : nodes) {
            try { if (n != null) n.stop(); } catch (Exception ignored) {}
        }
    }

    @Test
    void threeNodesElectALeaderAndReplicateOneMessage(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i]));
        }

        nodes = new RaftOrderingService[3];
        List<List<ChatDeliverMessage>> deliveries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes[i] = buildService(i, ports[i], voters, baseDir.resolve("n" + i));
            List<ChatDeliverMessage> d = new CopyOnWriteArrayList<>();
            deliveries.add(d);
            nodes[i].onDeliver(d::add);
        }
        for (RaftOrderingService n : nodes) {
            n.start();
        }

        int leaderIdx = waitForSingleLeader();
        assertTrue(leaderIdx >= 0, "no leader elected within "
                + LEADER_ELECTION_DEADLINE_MS + " ms");

        // Sanity: exactly one leader at this instant.
        long leaderCount = Arrays.stream(nodes).filter(RaftOrderingService::isLeader).count();
        assertEquals(1L, leaderCount, "expected exactly one leader, got " + leaderCount);

        // Propose a single message on the leader.
        ChatReqMessage req = new ChatReqMessage(
                "msg-1", leaderIdx, "alice", "hello", new VectorClock());
        nodes[leaderIdx].propose(req);

        // Wait until all three nodes have delivered the message.
        boolean delivered = waitFor(
                () -> deliveries.stream().allMatch(d -> d.size() >= 1),
                DELIVERY_DEADLINE_MS);
        assertTrue(delivered, "message not delivered on all nodes within "
                + DELIVERY_DEADLINE_MS + " ms; per-node sizes = "
                + deliveries.stream().map(List::size).toList());

        for (int i = 0; i < 3; i++) {
            List<ChatDeliverMessage> d = deliveries.get(i);
            assertEquals(1, d.size(), "node " + i + " unexpected delivery count");
            assertEquals(1L, d.get(0).getSeq(), "node " + i + " unexpected seq");
            assertEquals("hello", d.get(0).getText(), "node " + i + " unexpected text");
            assertEquals("alice", d.get(0).getUsername(), "node " + i + " unexpected username");
            assertEquals(leaderIdx, d.get(0).getBrokerId(), "node " + i + " unexpected brokerId");
        }
    }

    // --- helpers ----------------------------------------------------------

    private RaftOrderingService buildService(int nodeId,
                                             int rpcPort,
                                             Map<Integer, RaftPeerEndpoint> voters,
                                             Path storageDir) throws IOException {
        Files.createDirectories(storageDir);
        RaftConfig raft = new RaftConfig(
                ELECTION_TIMEOUT_MIN_MS,
                ELECTION_TIMEOUT_MAX_MS,
                HEARTBEAT_INTERVAL_MS,
                rpcPort,
                storageDir,
                voters);

        // Sequencer-side BrokerConfig fields are irrelevant when orderingMode == RAFT;
        // we pass dummy values to satisfy the constructor.
        BrokerConfig cfg = new BrokerConfig(
                nodeId,
                false,
                "127.0.0.1",
                50000 + nodeId,
                50000 + nodeId,
                "127.0.0.1",
                50001,
                50002,
                null,
                OrderingMode.RAFT,
                raft);

        return new RaftOrderingService(cfg);
    }

    private int waitForSingleLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + LEADER_ELECTION_DEADLINE_MS;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].isLeader()) {
                    return i;
                }
            }
            Thread.sleep(50);
        }
        return -1;
    }

    private static boolean waitFor(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return cond.getAsBoolean();
    }

    /**
     * Allocates {@code n} loopback TCP ports by opening sockets with port 0
     * (OS-assigned), reading the bound port, and closing them. Subject to a
     * small race window before the production server binds the same port;
     * acceptable for in-process tests.
     */
    private static int[] pickFreePorts(int n) throws IOException {
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) {
                    try { s.close(); } catch (IOException ignored) {}
                }
            }
        }
        return ports;
    }
}