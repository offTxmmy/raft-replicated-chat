package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import it.polimi.ds.chat.common.clock.VectorClock;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * End-to-end integration test for {@link RaftOrderingService}.
 *
 * <p>Three orchestrator instances are started in LOCAL_TCP mode on loopback,
 * each with its own RPC port and storage directory and an identical static
 * voter map. A separate test keeps exercising HYBRID. The tests verify that:
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
    private int defaultBroadcastPort;
    private String defaultClusterId;

    @AfterEach
    void tearDown() {
        if (nodes == null) return;
        for (RaftOrderingService n : nodes) {
            try { if (n != null) n.stop(); } catch (Exception ignored) {}
        }
    }

    @Test
    void threeLocalTcpNodesElectALeaderAndReplicateOneMessage(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
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
            assertEquals("hello", d.get(0).getText(), "node " + i + " unexpected text");
            assertEquals("alice", d.get(0).getUsername(), "node " + i + " unexpected username");
            assertEquals(leaderIdx, d.get(0).getBrokerId(), "node " + i + " unexpected brokerId");
        }
    }

    @Test
    void restartedNodesReloadPersistedLogAndContinueSequence(@TempDir Path baseDir) throws Exception {
        int[] firstPorts = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", firstPorts[i], 50000 + i));
        }

        Path[] storageDirs = new Path[3];
        nodes = new RaftOrderingService[3];
        List<List<ChatDeliverMessage>> firstRunDeliveries = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            storageDirs[i] = baseDir.resolve("n" + i);
            nodes[i] = buildService(i, firstPorts[i], voters, storageDirs[i]);

            List<ChatDeliverMessage> d = new CopyOnWriteArrayList<>();
            firstRunDeliveries.add(d);
            nodes[i].onDeliver(d::add);
        }

        for (RaftOrderingService n : nodes) {
            n.start();
        }

        int firstLeader = waitForSingleLeader();
        assertTrue(firstLeader >= 0, "no leader elected before restart");

        ChatReqMessage firstReq = new ChatReqMessage(
                "msg-before-restart", firstLeader, "alice", "before restart", new VectorClock());

        assertTrue(nodes[firstLeader].propose(firstReq), "first proposal should commit");

        assertTrue(waitFor(
                () -> firstRunDeliveries.stream().allMatch(d -> d.size() >= 1),
                DELIVERY_DEADLINE_MS
        ), "first message not delivered before restart");

        for (RaftOrderingService n : nodes) {
            n.stop();
        }

        int[] secondPorts = pickFreePorts(3);
        Map<Integer, RaftPeerEndpoint> restartedVoters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            restartedVoters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", secondPorts[i], 50000 + i));
        }

        nodes = new RaftOrderingService[3];
        List<List<ChatDeliverMessage>> secondRunDeliveries = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            nodes[i] = buildService(i, secondPorts[i], restartedVoters, storageDirs[i]);

            List<ChatDeliverMessage> d = new CopyOnWriteArrayList<>();
            secondRunDeliveries.add(d);
            nodes[i].onDeliver(d::add);
        }

        for (RaftOrderingService n : nodes) {
            n.start();
        }

        int secondLeader = waitForSingleLeader();
        assertTrue(secondLeader >= 0, "no leader elected after restart");

        ChatReqMessage secondReq = new ChatReqMessage(
                "msg-after-restart", secondLeader, "alice", "after restart", new VectorClock());

        assertTrue(nodes[secondLeader].propose(secondReq), "second proposal should commit");

        assertTrue(waitFor(
                () -> secondRunDeliveries.stream().allMatch(d ->
                        d.stream().anyMatch(msg -> msg.getText().equals("after restart"))),
                DELIVERY_DEADLINE_MS
        ), "second message not delivered after restart");

        for (int i = 0; i < 3; i++) {
            boolean hasAfterRestart = secondRunDeliveries.get(i).stream()
                    .anyMatch(msg -> msg.getText().equals("after restart"));
            assertTrue(hasAfterRestart, "node " + i + " did not deliver restarted message");
        }
    }

    @Test
    void duplicateClientRetryOnLeaderCommitsAndDeliversOnce(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
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

        long clientTimestamp = 12345L;
        ChatReqMessage firstAttempt = new ChatReqMessage(
                "retry-leader-1",
                leaderIdx,
                "alice",
                "same wire message",
                new VectorClock(),
                clientTimestamp
        );
        ChatReqMessage retryAttempt = new ChatReqMessage(
                "retry-leader-2",
                leaderIdx,
                "alice",
                "same wire message",
                new VectorClock(),
                clientTimestamp
        );

        assertTrue(nodes[leaderIdx].propose(firstAttempt), "first attempt should commit");
        assertTrue(waitFor(
                () -> deliveries.stream().allMatch(d -> d.size() >= 1),
                DELIVERY_DEADLINE_MS
        ), "first attempt was not delivered on all nodes");

        long logIndexAfterFirstAttempt = nodes[leaderIdx].getLastLogIndexForTesting();

        assertTrue(nodes[leaderIdx].propose(retryAttempt), "committed retry should ACK without appending");

        assertEquals(logIndexAfterFirstAttempt, nodes[leaderIdx].getLastLogIndexForTesting(),
                "duplicate retry must not append a second Raft entry");
        for (int i = 0; i < 3; i++) {
            List<ChatDeliverMessage> d = deliveries.get(i);
            assertEquals(1, d.size(), "node " + i + " should deliver the client message once");
            assertEquals("same wire message", d.get(0).getText(), "node " + i + " unexpected text");
            assertEquals("alice", d.get(0).getUsername(), "node " + i + " unexpected username");
        }
    }

    /**
     * Verifies that duplicate committed log entries for the same client proposal
     * cross the application boundary only once.
     *
     * <p>The persisted state intentionally contains two entries with the same
     * client identity and sequence. Both are committed but neither was applied
     * before restart.
     */
    @Test
    void duplicateCommittedProposalEntriesShouldBeAppliedOnlyOnce(
            @TempDir Path baseDir
    ) throws Exception {
        int[] ports = pickFreePorts(1);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(
                0,
                new RaftPeerEndpoint(0, "127.0.0.1", ports[0], 50000)
        );

        Path storageDir = baseDir.resolve("n0");
        Files.createDirectories(storageDir);

        FileRaftPersistence persistence =
                new FileRaftPersistence(storageDir);

        persistence.persistTermAndVote(1L, null);

        ChatCommand first = new ChatCommand(
                "duplicate-entry-1",
                0,
                "alice",
                "deduplicated payload",
                new VectorClock(),
                "client-dedup",
                1L
        );

        ChatCommand duplicate = new ChatCommand(
                "duplicate-entry-2",
                0,
                "alice",
                "deduplicated payload",
                new VectorClock(),
                "client-dedup",
                1L
        );

        persistence.appendLogEntry(
                new RaftLogEntry(1L, 1L, first)
        );
        persistence.appendLogEntry(
                new RaftLogEntry(2L, 1L, duplicate)
        );

        // Both entries are committed, but neither has crossed the application
        // boundary yet.
        persistence.persistCommitProgress(2L, 0L);

        nodes = new RaftOrderingService[1];

        List<ChatDeliverMessage> deliveries =
                new CopyOnWriteArrayList<>();

        nodes[0] = buildService(
                0,
                ports[0],
                voters,
                storageDir
        );
        nodes[0].onDeliver(deliveries::add);

        nodes[0].start();

        assertEquals(
                1,
                deliveries.size(),
                "duplicate committed proposal must be delivered only once"
        );

        assertEquals(
                "deduplicated payload",
                deliveries.get(0).getText()
        );
    }

    /**
     * Verifies that a leader does not append a second copy of a proposal that is
     * already present in its local Raft log but has not been applied yet.
     */
    @Test
    void uncommittedProposalAlreadyInLogShouldNotBeAppendedAgain(
            @TempDir Path baseDir
    ) throws Exception {
        int[] ports = pickFreePorts(1);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(
                0,
                new RaftPeerEndpoint(0, "127.0.0.1", ports[0], 50000)
        );

        nodes = new RaftOrderingService[1];
        List<ChatDeliverMessage> deliveries =
                new CopyOnWriteArrayList<>();

        nodes[0] = buildService(
                0,
                ports[0],
                voters,
                baseDir.resolve("n0")
        );
        nodes[0].onDeliver(deliveries::add);
        nodes[0].start();

        int leaderIdx = waitForSingleLeader();
        assertEquals(0, leaderIdx, "single-node cluster should elect node 0");

        String clientId = "client-existing-log";
        long clientSeq = 1L;

        ChatReqMessage request = new ChatReqMessage(
                "retry-existing-log",
                0,
                "alice",
                "already in log",
                new VectorClock(),
                clientId,
                clientSeq
        );

        ChatCommand command = new ChatCommand(
                "original-existing-log",
                0,
                "alice",
                "already in log",
                new VectorClock(),
                clientId,
                clientSeq
        );

        RaftLog log = raftLogOf(nodes[0]);
        RaftNode node = raftNodeOf(nodes[0]);

        // Insert an entry directly into the log without advancing commitIndex.
        log.append(node.getCurrentTerm(), command);

        long logIndexBeforeRetry = log.lastLogIndex();

        boolean accepted = nodes[0].propose(request);

        assertFalse(
                accepted,
                "an uncommitted existing log entry must not be ACKed as committed"
        );

        assertEquals(
                logIndexBeforeRetry,
                log.lastLogIndex(),
                "retry must not append a second Raft entry for the same proposal"
        );

        assertEquals(
                1L,
                countClientProposalEntries(log, clientId, clientSeq),
                "the log must contain exactly one copy of the proposal"
        );

        assertTrue(
                deliveries.isEmpty(),
                "uncommitted proposal must not be delivered"
        );
    }

    /**
     * Verifies that truncating an uncommitted proposal removes and fails its
     * pending future, allowing the same client proposal to be submitted again.
     */
    @Test
    void truncatedPendingProposalShouldFailAndBeRetryable(
            @TempDir Path baseDir
    ) throws Exception {
        int[] ports = pickFreePorts(1);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(
                0,
                new RaftPeerEndpoint(0, "127.0.0.1", ports[0], 50000)
        );

        nodes = new RaftOrderingService[1];

        List<ChatDeliverMessage> deliveries =
                new CopyOnWriteArrayList<>();

        nodes[0] = buildService(
                0,
                ports[0],
                voters,
                baseDir.resolve("n0")
        );
        nodes[0].onDeliver(deliveries::add);
        nodes[0].start();

        assertEquals(0, waitForSingleLeader());

        String clientId = "client-truncated";
        long clientSeq = 1L;
        String key = clientProposalKey(clientId, clientSeq);

        ChatReqMessage request = new ChatReqMessage(
                "retry-after-truncation",
                0,
                "alice",
                "survives retry",
                new VectorClock(),
                clientId,
                clientSeq
        );

        ChatCommand command = new ChatCommand(
                "original-before-truncation",
                0,
                "alice",
                "survives retry",
                new VectorClock(),
                clientId,
                clientSeq
        );

        RaftLog log = raftLogOf(nodes[0]);
        RaftNode node = raftNodeOf(nodes[0]);

        RaftLogEntry appended =
                log.append(node.getCurrentTerm(), command);

        CompletableFuture<Boolean> pending =
                new CompletableFuture<>();

        ConcurrentHashMap<String, CompletableFuture<Boolean>> pendingCommits =
                pendingCommitsOf(nodes[0]);

        pendingCommits.put(key, pending);

        log.truncateFrom(appended.getIndex());

        assertTrue(
                pending.isDone(),
                "truncation must complete the pending future"
        );

        assertFalse(
                pending.getNow(true),
                "truncated proposal must complete with false"
        );

        assertFalse(
                pendingCommits.containsKey(key),
                "truncated proposal must be removed from pendingCommits"
        );

        assertEquals(
                0L,
                countClientProposalEntries(log, clientId, clientSeq),
                "truncated proposal must no longer exist in the log"
        );

        // After truncation, the same proposal must be eligible for a fresh append.
        assertTrue(
                nodes[0].propose(request),
                "a truncated proposal must be retryable"
        );

        assertEquals(
                1L,
                countClientProposalEntries(log, clientId, clientSeq),
                "retry must append exactly one fresh entry"
        );

        assertEquals(
                1,
                deliveries.size(),
                "retried proposal must be delivered once"
        );
    }

    /**
     * Verifies that losing leadership fails all proposal futures owned by the old
     * leader instead of leaving callers blocked until their timeout expires.
     */
    @Test
    void leadershipLossShouldFailAllPendingCommits(
            @TempDir Path baseDir
    ) throws Exception {
        int[] ports = pickFreePorts(1);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(
                0,
                new RaftPeerEndpoint(0, "127.0.0.1", ports[0], 50000)
        );

        nodes = new RaftOrderingService[1];

        nodes[0] = buildService(
                0,
                ports[0],
                voters,
                baseDir.resolve("n0")
        );
        nodes[0].start();

        assertEquals(0, waitForSingleLeader());

        ConcurrentHashMap<String, CompletableFuture<Boolean>> pendingCommits =
                pendingCommitsOf(nodes[0]);

        CompletableFuture<Boolean> first =
                new CompletableFuture<>();
        CompletableFuture<Boolean> second =
                new CompletableFuture<>();

        pendingCommits.put("client:first:1", first);
        pendingCommits.put("client:second:1", second);

        RaftNode node = raftNodeOf(nodes[0]);
        RaftElectionManager electionManager =
                electionManagerOf(nodes[0]);

        electionManager.onHigherTermObserved(
                node.getCurrentTerm() + 1L
        );

        assertTrue(first.isDone());
        assertTrue(second.isDone());

        assertFalse(first.getNow(true));
        assertFalse(second.getNow(true));

        assertTrue(
                pendingCommits.isEmpty(),
                "leadership loss must clear all pending commit futures"
        );
    }

    /**
     * Verifies that if leadership disappears after a pending future has been
     * registered but before the leader append succeeds, that future is explicitly
     * completed with false.
     */
    @Test
    void failedLeaderAppendShouldCompleteRegisteredPendingFuture(
            @TempDir Path baseDir
    ) throws Exception {
        int[] ports = pickFreePorts(1);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(
                0,
                new RaftPeerEndpoint(0, "127.0.0.1", ports[0], 50000)
        );

        nodes = new RaftOrderingService[1];

        nodes[0] = buildService(
                0,
                ports[0],
                voters,
                baseDir.resolve("n0")
        );
        nodes[0].start();

        assertEquals(0, waitForSingleLeader());

        RaftReplicationManager originalReplicationManager =
                replicationManagerOf(nodes[0]);

        BlockingNullAppendReplicationManager blockingReplicationManager =
                new BlockingNullAppendReplicationManager(
                        0,
                        raftNodeOf(nodes[0]),
                        raftLogOf(nodes[0]),
                        commitManagerOf(nodes[0])
                );

        setPrivateField(
                nodes[0],
                "replicationManager",
                blockingReplicationManager
        );

        String clientId = "client-null-append";
        long clientSeq = 1L;
        String key = clientProposalKey(clientId, clientSeq);

        ChatReqMessage request = new ChatReqMessage(
                "null-append",
                0,
                "alice",
                "append loses leadership",
                new VectorClock(),
                clientId,
                clientSeq
        );

        CompletableFuture<Boolean> proposeResult =
                CompletableFuture.supplyAsync(
                        () -> nodes[0].propose(request)
                );

        assertTrue(
                blockingReplicationManager.awaitAppendEntered(),
                "proposal did not reach appendCommandAsLeader"
        );

        CompletableFuture<Boolean> registeredPending =
                pendingCommitsOf(nodes[0]).get(key);

        assertTrue(
                registeredPending != null,
                "proposal future should already be registered"
        );

        blockingReplicationManager.releaseAppend();

        assertFalse(
                proposeResult.get(1L, TimeUnit.SECONDS),
                "failed append must return false"
        );

        assertTrue(
                registeredPending.isDone(),
                "registered pending future must be completed"
        );

        assertFalse(
                registeredPending.getNow(true),
                "failed append must complete the future with false"
        );

        assertFalse(
                pendingCommitsOf(nodes[0]).containsKey(key),
                "failed append must remove its pending reservation"
        );

        // Restore the real manager so normal tearDown shuts down the real service state.
        setPrivateField(
                nodes[0],
                "replicationManager",
                originalReplicationManager
        );
    }

    /**
     * Verifies that a proposal already replicated to the future leader is not
     * appended again after an actual leader change.
     *
     * <p>The test deterministically reproduces the state in which the old leader
     * and one follower contain the proposal, but the follower has not yet learned
     * it as committed. After the old leader stops, the up-to-date follower becomes
     * leader and must preserve a single copy across the client retry.
     */
    @Test
    void retryAfterLeaderChangeShouldNotDuplicateExistingProposal(
            @TempDir Path baseDir
    ) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(
                    i,
                    new RaftPeerEndpoint(
                            i,
                            "127.0.0.1",
                            ports[i],
                            50000 + i
                    )
            );
        }

        nodes = new RaftOrderingService[3];

        List<List<ChatDeliverMessage>> deliveries =
                new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            nodes[i] = buildService(
                    i,
                    ports[i],
                    voters,
                    baseDir.resolve("n" + i)
            );

            List<ChatDeliverMessage> nodeDeliveries =
                    new CopyOnWriteArrayList<>();

            deliveries.add(nodeDeliveries);
            nodes[i].onDeliver(nodeDeliveries::add);
        }

        for (RaftOrderingService node : nodes) {
            node.start();
        }

        int oldLeader = waitForSingleLeader();
        assertTrue(oldLeader >= 0, "initial leader was not elected");

        int futureLeader = -1;
        int survivingFollower = -1;

        for (int i = 0; i < nodes.length; i++) {
            if (i == oldLeader) {
                continue;
            }

            if (futureLeader < 0) {
                futureLeader = i;
            } else {
                survivingFollower = i;
            }
        }

        assertTrue(futureLeader >= 0);
        assertTrue(survivingFollower >= 0);

        final int nextLeader = futureLeader;
        final int survivor = survivingFollower;

        long commonLogIndex =
                nodes[oldLeader].getLastLogIndexForTesting();

        assertTrue(
                waitFor(
                        () -> nodes[nextLeader].getLastLogIndexForTesting()
                                == commonLogIndex
                                && nodes[survivor].getLastLogIndexForTesting()
                                == commonLogIndex,
                        DELIVERY_DEADLINE_MS
                ),
                "followers did not catch up before failover setup"
        );

        String clientId = "client-failover";
        long clientSeq = 1L;

        ChatCommand command = new ChatCommand(
                "original-before-failover",
                oldLeader,
                "alice",
                "failover payload",
                new VectorClock(),
                clientId,
                clientSeq
        );

        long oldTerm =
                raftNodeOf(nodes[oldLeader]).getCurrentTerm();

        /*
         * Freeze old-leader replication before installing the deterministic
         * pre-crash state, so the third node cannot accidentally receive K.
         */
        replicationManagerOf(nodes[oldLeader]).stop();

        RaftLogEntry oldLeaderEntry =
                raftLogOf(nodes[oldLeader]).append(
                        oldTerm,
                        command
                );

        RaftLogEntry futureLeaderEntry =
                raftLogOf(nodes[nextLeader]).append(
                        oldTerm,
                        command
                );

        assertEquals(
                oldLeaderEntry.getIndex(),
                futureLeaderEntry.getIndex(),
                "old leader and future leader must contain K at the same log index"
        );

        assertEquals(
                1L,
                countClientProposalEntries(
                        raftLogOf(nodes[nextLeader]),
                        clientId,
                        clientSeq
                )
        );

        // Crash/stop the old leader.
        nodes[oldLeader].stop();

        assertTrue(
                waitFor(
                        () -> nodes[nextLeader].isLeader(),
                        LEADER_ELECTION_DEADLINE_MS
                ),
                "the follower carrying the newer log entry did not become leader"
        );

        ChatReqMessage retry = new ChatReqMessage(
                "retry-after-failover",
                nextLeader,
                "alice",
                "failover payload",
                new VectorClock(),
                clientId,
                clientSeq
        );

        /*
         * The retry may observe K either just before or just after it becomes
         * committed. Therefore true or false are both valid here.
         *
         * The safety property is that it must never append a second copy.
         */
        nodes[nextLeader].propose(retry);

        assertEquals(
                1L,
                countClientProposalEntries(
                        raftLogOf(nodes[nextLeader]),
                        clientId,
                        clientSeq
                ),
                "new leader must not append a duplicate client proposal"
        );

        assertTrue(
                waitFor(
                        () -> deliveries.get(nextLeader).stream()
                                .filter(msg ->
                                        "failover payload".equals(
                                                msg.getText()
                                        ))
                                .count() == 1L
                                && deliveries.get(survivor).stream()
                                .filter(msg ->
                                        "failover payload".equals(
                                                msg.getText()
                                        ))
                                .count() == 1L,
                        DELIVERY_DEADLINE_MS
                ),
                "surviving nodes did not apply the failover proposal exactly once"
        );

        assertEquals(
                1L,
                countClientProposalEntries(
                        raftLogOf(nodes[survivor]),
                        clientId,
                        clientSeq
                ),
                "surviving follower must contain one copy of the proposal"
        );
    }

    @Test
    void sameUsernameAndSequenceFromDifferentClientsAreNotDeduplicated(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
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

        ChatReqMessage fromClientA = new ChatReqMessage(
                "same-user-a",
                leaderIdx,
                "alice",
                "from client A",
                new VectorClock(),
                "client-a",
                1L
        );
        ChatReqMessage fromClientB = new ChatReqMessage(
                "same-user-b",
                leaderIdx,
                "alice",
                "from client B",
                new VectorClock(),
                "client-b",
                1L
        );

        assertTrue(nodes[leaderIdx].propose(fromClientA), "first client message should commit");
        assertTrue(nodes[leaderIdx].propose(fromClientB), "second client message should commit");

        assertTrue(waitFor(
                () -> deliveries.stream().allMatch(d -> d.size() >= 2),
                DELIVERY_DEADLINE_MS
        ), "same username/clientSeq messages from different clients were incorrectly deduplicated");

        for (int i = 0; i < 3; i++) {
            List<String> texts = deliveries.get(i).stream()
                    .map(ChatDeliverMessage::getText)
                    .toList();
            assertTrue(texts.contains("from client A"), "node " + i + " missing client A message");
            assertTrue(texts.contains("from client B"), "node " + i + " missing client B message");
        }
    }

    @Test
    void hybridTransportElectsLeaderAndReplicatesOneMessage(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);
        int broadcastPort = pickFreeUdpPort();
        String clusterId = "hybrid-it-" + System.nanoTime();

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
        }

        nodes = new RaftOrderingService[3];
        List<List<ChatDeliverMessage>> deliveries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes[i] = buildService(
                    i,
                    ports[i],
                    voters,
                    baseDir.resolve("hybrid-n" + i),
                    RaftTransportMode.HYBRID,
                    broadcastPort,
                    clusterId
            );
            List<ChatDeliverMessage> d = new CopyOnWriteArrayList<>();
            deliveries.add(d);
            nodes[i].onDeliver(d::add);
        }
        for (RaftOrderingService n : nodes) {
            n.start();
        }

        int leaderIdx = waitForSingleLeader();
        assertTrue(leaderIdx >= 0, "no HYBRID leader elected within "
                + LEADER_ELECTION_DEADLINE_MS + " ms");

        long leaderCount = Arrays.stream(nodes).filter(RaftOrderingService::isLeader).count();
        assertEquals(1L, leaderCount, "expected exactly one HYBRID leader, got " + leaderCount);

        assertTrue(waitFor(() -> {
            for (int i = 0; i < nodes.length; i++) {
                if (i != leaderIdx && nodes[i].getLeaderId() != leaderIdx) {
                    return false;
                }
            }
            return true;
        }, DELIVERY_DEADLINE_MS), "followers did not learn the HYBRID leader from heartbeats");

        ChatReqMessage req = new ChatReqMessage(
                "hybrid-msg-1", leaderIdx, "alice", "hello hybrid", new VectorClock());

        assertTrue(nodes[leaderIdx].propose(req), "HYBRID leader proposal should commit");

        boolean delivered = waitFor(
                () -> deliveries.stream().allMatch(d -> d.size() >= 1),
                DELIVERY_DEADLINE_MS);
        assertTrue(delivered, "HYBRID message not delivered on all nodes within "
                + DELIVERY_DEADLINE_MS + " ms; per-node sizes = "
                + deliveries.stream().map(List::size).toList());

        for (int i = 0; i < 3; i++) {
            List<ChatDeliverMessage> d = deliveries.get(i);
            assertEquals(1, d.size(), "node " + i + " unexpected HYBRID delivery count");
            assertEquals("hello hybrid", d.get(0).getText(), "node " + i + " unexpected HYBRID text");
            assertEquals("alice", d.get(0).getUsername(), "node " + i + " unexpected HYBRID username");
            assertEquals(leaderIdx, d.get(0).getBrokerId(), "node " + i + " unexpected HYBRID brokerId");
        }
    }

    @Test
    void duplicateClientRetryForwardedByFollowerCommitsAndDeliversOnce(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
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

        int followerIdx = -1;
        for (int i = 0; i < nodes.length; i++) {
            if (i != leaderIdx) {
                followerIdx = i;
                break;
            }
        }
        assertTrue(followerIdx >= 0, "no follower found");

        final int leader = leaderIdx;
        final int follower = followerIdx;
        assertTrue(waitFor(
                () -> nodes[follower].getLeaderId() == leader,
                DELIVERY_DEADLINE_MS
        ), "follower did not learn the current leader");

        long clientTimestamp = 67890L;
        ChatReqMessage firstAttempt = new ChatReqMessage(
                "retry-follower-1",
                followerIdx,
                "alice",
                "same forwarded wire message",
                new VectorClock(),
                clientTimestamp
        );
        ChatReqMessage retryAttempt = new ChatReqMessage(
                "retry-follower-2",
                followerIdx,
                "alice",
                "same forwarded wire message",
                new VectorClock(),
                clientTimestamp
        );

        assertTrue(nodes[followerIdx].propose(firstAttempt), "first follower-forwarded attempt should commit");
        assertTrue(waitFor(
                () -> deliveries.stream().allMatch(d -> d.size() >= 1),
                DELIVERY_DEADLINE_MS
        ), "first follower-forwarded attempt was not delivered on all nodes");

        long logIndexAfterFirstAttempt = nodes[leaderIdx].getLastLogIndexForTesting();

        assertTrue(nodes[followerIdx].propose(retryAttempt),
                "committed follower-forwarded retry should ACK without appending");

        assertEquals(logIndexAfterFirstAttempt, nodes[leaderIdx].getLastLogIndexForTesting(),
                "duplicate forwarded retry must not append a second Raft entry");
        for (int i = 0; i < 3; i++) {
            List<ChatDeliverMessage> d = deliveries.get(i);
            assertEquals(1, d.size(), "node " + i + " should deliver the forwarded client message once");
            assertEquals("same forwarded wire message", d.get(0).getText(), "node " + i + " unexpected text");
            assertEquals("alice", d.get(0).getUsername(), "node " + i + " unexpected username");
        }
    }

    @Test
    void followerForwardsClientProposalToLeader(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
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

        int followerIdx = -1;
        for (int i = 0; i < nodes.length; i++) {
            if (i != leaderIdx) {
                followerIdx = i;
                break;
            }
        }

        assertTrue(followerIdx >= 0, "no follower found");

        final int leader = leaderIdx;
        final int follower = followerIdx;
        assertTrue(waitFor(
                () -> nodes[follower].getLeaderId() == leader,
                DELIVERY_DEADLINE_MS
        ), "follower did not learn the current leader");

        ChatReqMessage forwardedReq = new ChatReqMessage(
                "msg-follower",
                followerIdx,
                "alice",
                "forwarded through follower",
                new VectorClock()
        );

        boolean accepted = nodes[followerIdx].propose(forwardedReq);

        assertTrue(accepted, "a follower should proxy client proposals to the leader");

        boolean delivered = waitFor(
                () -> deliveries.stream().allMatch(d -> d.size() >= 1),
                DELIVERY_DEADLINE_MS
        );

        assertTrue(delivered, "forwarded proposal was not delivered on all nodes");
        for (int i = 0; i < 3; i++) {
            List<ChatDeliverMessage> d = deliveries.get(i);
            assertEquals(1, d.size(), "node " + i + " unexpected delivery count");
            assertEquals("forwarded through follower", d.get(0).getText(), "node " + i + " unexpected text");
            assertEquals(followerIdx, d.get(0).getBrokerId(), "node " + i + " unexpected brokerId");
        }
    }

    @Test
    void followerWithoutKnownLeaderRejectsClientProposal(@TempDir Path baseDir) throws Exception {
        int[] ports = pickFreePorts(3);

        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            voters.put(i, new RaftPeerEndpoint(i, "127.0.0.1", ports[i], 50000 + i));
        }

        nodes = new RaftOrderingService[1];
        List<ChatDeliverMessage> deliveries = new CopyOnWriteArrayList<>();
        nodes[0] = buildService(0, ports[0], voters, baseDir.resolve("n0"));
        nodes[0].onDeliver(deliveries::add);
        nodes[0].start();

        ChatReqMessage request = new ChatReqMessage(
                "msg-no-leader",
                0,
                "alice",
                "no known leader",
                new VectorClock()
        );

        boolean accepted = nodes[0].propose(request);

        assertFalse(accepted, "proposal should be rejected when no leader is known");
        assertTrue(deliveries.isEmpty(), "proposal without a known leader must not be delivered");
    }

    // --- helpers ----------------------------------------------------------

    private RaftOrderingService buildService(int nodeId,
                                             int rpcPort,
                                             Map<Integer, RaftPeerEndpoint> voters,
                                             Path storageDir) throws IOException {
        return buildService(
                nodeId,
                rpcPort,
                voters,
                storageDir,
                RaftTransportMode.LOCAL_TCP,
                RaftConfig.DEFAULT_RAFT_BROADCAST_PORT,
                RaftConfig.DEFAULT_CLUSTER_ID
        );
    }

    private RaftOrderingService buildService(int nodeId,
                                             int rpcPort,
                                             Map<Integer, RaftPeerEndpoint> voters,
                                             Path storageDir,
                                             RaftTransportMode transportMode,
                                             int raftBroadcastPort,
                                             String clusterId) throws IOException {
        Files.createDirectories(storageDir);
        RaftConfig raft = new RaftConfig(
                ELECTION_TIMEOUT_MIN_MS,
                ELECTION_TIMEOUT_MAX_MS,
                HEARTBEAT_INTERVAL_MS,
                rpcPort,
                transportMode,
                raftBroadcastPort,
                RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES,
                clusterId,
                storageDir,
                voters);

        BrokerConfig cfg = new BrokerConfig(
                nodeId,
                "127.0.0.1",
                50000 + nodeId,
                50000 + nodeId,
                50002,
                raft);

        return new RaftOrderingService(cfg);
    }

    private static RaftLog raftLogOf(RaftOrderingService service) {
        return privateField(
                service,
                "raftLog",
                RaftLog.class
        );
    }

    private static RaftNode raftNodeOf(RaftOrderingService service) {
        return privateField(
                service,
                "raftNode",
                RaftNode.class
        );
    }

    private static RaftCommitManager commitManagerOf(
            RaftOrderingService service
    ) {
        return privateField(
                service,
                "commitManager",
                RaftCommitManager.class
        );
    }

    private static RaftElectionManager electionManagerOf(
            RaftOrderingService service
    ) {
        return privateField(
                service,
                "electionManager",
                RaftElectionManager.class
        );
    }

    private static RaftReplicationManager replicationManagerOf(
            RaftOrderingService service
    ) {
        return privateField(
                service,
                "replicationManager",
                RaftReplicationManager.class
        );
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, CompletableFuture<Boolean>>
    pendingCommitsOf(RaftOrderingService service) {
        return (ConcurrentHashMap<String, CompletableFuture<Boolean>>)
                privateField(
                        service,
                        "pendingCommits",
                        ConcurrentHashMap.class
                );
    }

    private static <T> T privateField(
            RaftOrderingService service,
            String fieldName,
            Class<T> fieldType
    ) {
        try {
            java.lang.reflect.Field field =
                    RaftOrderingService.class.getDeclaredField(fieldName);

            field.setAccessible(true);
            return fieldType.cast(field.get(service));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(
                    "Unable to access RaftOrderingService." + fieldName,
                    e
            );
        }
    }

    private static void setPrivateField(
            RaftOrderingService service,
            String fieldName,
            Object value
    ) {
        try {
            java.lang.reflect.Field field =
                    RaftOrderingService.class.getDeclaredField(fieldName);

            field.setAccessible(true);
            field.set(service, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(
                    "Unable to update RaftOrderingService." + fieldName,
                    e
            );
        }
    }

    private static String clientProposalKey(
            String clientId,
            long clientSeq
    ) {
        return "client:" + clientId + ":" + clientSeq;
    }

    private static long countClientProposalEntries(
            RaftLog log,
            String clientId,
            long clientSeq
    ) {
        long count = 0L;

        for (RaftLogEntry entry : log.getEntriesFrom(1L)) {
            ChatCommand command = entry.getCommand();

            if (command != null
                    && command.hasClientIdentity()
                    && clientId.equals(command.getClientId())
                    && command.getClientSeq() == clientSeq) {
                count++;
            }
        }

        return count;
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

    private static int pickFreeUdpPort() throws IOException {
        try (java.net.DatagramSocket socket = new java.net.DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private int defaultBroadcastPort() throws IOException {
        if (defaultBroadcastPort == 0) {
            defaultBroadcastPort = pickFreeUdpPort();
        }
        return defaultBroadcastPort;
    }

    private String defaultClusterId() {
        if (defaultClusterId == null) {
            defaultClusterId = "raft-it-" + System.nanoTime();
        }
        return defaultClusterId;
    }

    private static final class BlockingNullAppendReplicationManager
            extends RaftReplicationManager {

        private final CountDownLatch appendEntered =
                new CountDownLatch(1);

        private final CountDownLatch appendRelease =
                new CountDownLatch(1);

        private BlockingNullAppendReplicationManager(
                int localNodeId,
                RaftNode node,
                RaftLog log,
                RaftCommitManager commitManager
        ) {
            super(
                    localNodeId,
                    Set.of(localNodeId),
                    node,
                    log,
                    commitManager,
                    (peerId, request) -> {},
                    null,
                    null
            );
        }

        @Override
        public RaftLogEntry appendCommandAsLeader(ChatCommand command) {
            appendEntered.countDown();

            try {
                if (!appendRelease.await(2L, TimeUnit.SECONDS)) {
                    throw new AssertionError(
                            "Timed out waiting to release blocked append"
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(
                        "Interrupted while waiting to release blocked append",
                        e
                );
            }

            return null;
        }

        private boolean awaitAppendEntered()
                throws InterruptedException {
            return appendEntered.await(
                    2L,
                    TimeUnit.SECONDS
            );
        }

        private void releaseAppend() {
            appendRelease.countDown();
        }
    }
}
