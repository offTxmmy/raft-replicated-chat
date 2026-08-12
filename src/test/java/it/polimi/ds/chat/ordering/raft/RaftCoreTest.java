package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Minimal integration tests that exercise election and replication wiring together.
 */
class RaftCoreTest {

    /**
     * Verifies that a leader heartbeat tick drives AppendEntries sends to peers
     * through the replication manager.
     */
    @Test
    void leaderHeartbeatShouldTriggerReplicationToPeers() {
        FakeClock fakeClock = new FakeClock();
        RaftNode node = new RaftNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RecordingAppendEntriesSender sender = new RecordingAppendEntriesSender();

        RaftReplicationManager replicationManager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null,
                null
        );
        replicationManager.start();

        RaftElectionManager electionManager = new RaftElectionManager(
                1,
                Set.of(1, 2, 3),
                100L,
                200L,
                50L,
                node,
                log.snapshotMetadata(),
                new RecordingVoteRequestSender(),
                fakeClock,
                replicationManager
        );
        electionManager.start();

        fakeClock.lastOneShotTask.fire();
        electionManager.onRequestVoteResponse(new RequestVoteResponseMessage(node.getCurrentTerm(), true, 2));

        RaftLogEntry entry = replicationManager.appendCommandAsLeader(command("x"));
        assertNotNull(entry);

        electionManager.onHeartbeatTick();

        assertEquals(2, sender.totalRequests());
        AppendEntriesRequestMessage request = sender.lastRequest(2);
        assertNotNull(request);
        assertEquals(node.getCurrentTerm(), request.getTerm());
        assertEquals(1, request.getLeaderId());
        assertEquals(1, request.getEntries().size());
        assertEquals("x", request.getEntries().get(0).getCommand().getLocalMsgId());
    }

    /**
     * Verifies that accepted AppendEntries notifies the election manager and
     * resets the follower election timeout.
     */
    @Test
    void acceptedAppendEntriesShouldResetElectionTimeout() {
        FakeClock fakeClock = new FakeClock();
        RaftNode node = new RaftNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftElectionManager electionManager = new RaftElectionManager(
                1,
                Set.of(1, 2, 3),
                100L,
                200L,
                50L,
                node,
                log.snapshotMetadata(),
                new RecordingVoteRequestSender(),
                fakeClock,
                null
        );
        electionManager.start();
        int scheduledBefore = fakeClock.oneShotScheduleCount;

        RaftReplicationManager replicationManager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                new RecordingAppendEntriesSender(),
                electionManager::onValidLeaderActivityObserved,
                null
        );
        replicationManager.start();

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                1L,
                2,
                0L,
                0L,
                List.of(),
                0L
        );

        AppendEntriesResponseMessage response = replicationManager.handleAppendEntries(request);

        assertTrue(response.isSuccess());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(1L, node.getCurrentTerm());
        assertEquals(2, node.getLeaderId());
        assertEquals(scheduledBefore + 1, fakeClock.oneShotScheduleCount);
    }

    /**
     * Verifies that a higher-term AppendEntries response is propagated from the
     * replication layer to the election layer and performs a complete leader
     * step-down lifecycle.
     */
    @Test
    void higherTermAppendEntriesResponseShouldTriggerCompleteElectionStepDown() {
        FakeClock fakeClock = new FakeClock();
        RaftNode node = new RaftNode(1);
        RaftLog log = new RaftLog();

        RaftCommitManager commitManager =
                new RaftCommitManager(log, entry -> {});

        RecordingAppendEntriesSender sender =
                new RecordingAppendEntriesSender();

        RaftReplicationManager[] replicationRef =
                new RaftReplicationManager[1];

        RaftElectionManager electionManager =
                new RaftElectionManager(
                        1,
                        Set.of(1, 2, 3),
                        100L,
                        200L,
                        50L,
                        node,
                        log.snapshotMetadata(),
                        new RecordingVoteRequestSender(),
                        fakeClock,
                        new RaftElectionListener() {
                            @Override
                            public void onLeaderElected(int leaderId, long term) {
                                replicationRef[0].onLeaderElected(
                                        leaderId,
                                        term
                                );
                            }

                            @Override
                            public void onSteppedDown(
                                    long newTerm,
                                    int knownLeaderId
                            ) {
                                replicationRef[0].onSteppedDown(
                                        newTerm,
                                        knownLeaderId
                                );
                            }

                            @Override
                            public void onHeartbeatRoundDue(long term) {
                                replicationRef[0].onHeartbeatRoundDue(term);
                            }
                        }
                );

        RaftReplicationManager replicationManager =
                new RaftReplicationManager(
                        1,
                        Set.of(1, 2, 3),
                        node,
                        log,
                        commitManager,
                        sender,
                        electionManager::onValidLeaderActivityObserved,
                        electionManager::onHigherTermObserved
                );

        replicationRef[0] = replicationManager;

        replicationManager.start();
        electionManager.start();

        fakeClock.lastOneShotTask.fire(); // candidate term 1

        electionManager.onRequestVoteResponse(
                new RequestVoteResponseMessage(
                        1L,
                        true,
                        2
                )
        ); // leader term 1

        assertEquals(RaftRole.LEADER, node.getRole());

        FakeScheduledTask heartbeatTask =
                fakeClock.lastFixedRateTask;

        int timeoutCountBeforeStepDown =
                fakeClock.oneShotScheduleCount;

        replicationManager.handleAppendEntriesResponse(
                2,
                new AppendEntriesResponseMessage(
                        5L,
                        false,
                        2,
                        0L,
                        -1L,
                        0L
                )
        );

        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(5L, node.getCurrentTerm());
        assertEquals(RaftNode.NO_LEADER, node.getLeaderId());

        assertTrue(heartbeatTask.cancelled);

        assertEquals(
                timeoutCountBeforeStepDown + 1,
                fakeClock.oneShotScheduleCount
        );

        assertNotNull(fakeClock.lastOneShotTask);
        assertFalse(fakeClock.lastOneShotTask.cancelled);
    }

    /**
     * Exercises the complete incoming-request path that previously bypassed the
     * election lifecycle by updating RaftNode directly in the replication layer.
     */
    @Test
    void higherTermAppendEntriesRequestShouldCompleteLeaderStepDownAndStillApply() {
        FakeClock fakeClock = new FakeClock();
        RaftNode node = new RaftNode(1);
        RaftLog log = new RaftLog();
        List<Long> appliedIndexes = new ArrayList<>();
        RaftCommitManager commitManager =
                new RaftCommitManager(log, entry -> appliedIndexes.add(entry.getIndex()));
        RecordingAppendEntriesSender sender = new RecordingAppendEntriesSender();

        ConcurrentHashMap<String, CompletableFuture<Boolean>> pendingCommits =
                new ConcurrentHashMap<>();
        CompletableFuture<Boolean> pending = new CompletableFuture<>();
        pendingCommits.put("client:pending:1", pending);

        AtomicInteger steppedDownCallbacks = new AtomicInteger();
        AtomicInteger leaderObservedCallbacks = new AtomicInteger();
        AtomicBoolean stepDownCallbackHeldReplicationLock = new AtomicBoolean();
        RaftReplicationManager[] replicationRef = new RaftReplicationManager[1];

        RaftElectionManager electionManager = new RaftElectionManager(
                1,
                Set.of(1, 2, 3),
                100L,
                200L,
                50L,
                node,
                log.snapshotMetadata(),
                new RecordingVoteRequestSender(),
                fakeClock,
                new RaftElectionListener() {
                    @Override
                    public void onLeaderElected(int leaderId, long term) {
                        replicationRef[0].onLeaderElected(leaderId, term);
                    }

                    @Override
                    public void onSteppedDown(long newTerm, int knownLeaderId) {
                        stepDownCallbackHeldReplicationLock.set(
                                Thread.holdsLock(replicationRef[0])
                        );
                        steppedDownCallbacks.incrementAndGet();
                        replicationRef[0].onSteppedDown(newTerm, knownLeaderId);

                        List<CompletableFuture<Boolean>> futures =
                                List.copyOf(pendingCommits.values());
                        pendingCommits.clear();
                        futures.forEach(future -> future.complete(false));
                    }

                    @Override
                    public void onLeaderObserved(int leaderId, long term) {
                        leaderObservedCallbacks.incrementAndGet();
                    }

                    @Override
                    public void onHeartbeatRoundDue(long term) {
                        replicationRef[0].onHeartbeatRoundDue(term);
                    }
                }
        );

        RaftReplicationManager replicationManager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                electionManager::onValidLeaderActivityObserved,
                electionManager::onHigherTermObserved
        );
        replicationRef[0] = replicationManager;

        replicationManager.start();
        electionManager.start();
        fakeClock.lastOneShotTask.fire();
        electionManager.onRequestVoteResponse(
                new RequestVoteResponseMessage(1L, true, 2)
        );

        assertEquals(RaftRole.LEADER, node.getRole());
        FakeScheduledTask oldHeartbeatTask = fakeClock.lastFixedRateTask;
        int timeoutSchedulesBeforeRequest = fakeClock.oneShotScheduleCount;

        AppendEntriesResponseMessage response = replicationManager.handleAppendEntries(
                new AppendEntriesRequestMessage(
                        5L,
                        2,
                        0L,
                        0L,
                        List.of(new RaftLogEntry(1L, 5L, command("new-leader"))),
                        1L
                )
        );

        assertTrue(response.isSuccess());
        assertEquals(5L, response.getTerm());
        assertEquals(1L, response.getMatchIndex());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(5L, node.getCurrentTerm());
        assertEquals(2, node.getLeaderId());

        assertTrue(oldHeartbeatTask.cancelled);
        assertEquals(timeoutSchedulesBeforeRequest + 2, fakeClock.oneShotScheduleCount);
        assertNotNull(fakeClock.lastOneShotTask);
        assertFalse(fakeClock.lastOneShotTask.cancelled);

        assertTrue(pending.isDone());
        assertFalse(pending.getNow(true));
        assertTrue(pendingCommits.isEmpty());
        assertEquals(1, steppedDownCallbacks.get());
        assertEquals(1, leaderObservedCallbacks.get());
        assertFalse(stepDownCallbackHeldReplicationLock.get());

        assertEquals(1L, log.lastLogIndex());
        assertEquals(5L, log.lastLogTerm());
        assertEquals(1L, commitManager.getCommitIndex());
        assertEquals(List.of(1L), appliedIndexes);
    }

    private ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }

    private static final class RecordingVoteRequestSender implements RaftVoteRequestSender {
        @Override
        public void sendRequestVote(int peerId, it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage request) {
        }
    }

    private static final class RecordingAppendEntriesSender implements RaftAppendEntriesSender {
        private final Map<Integer, List<AppendEntriesRequestMessage>> sent = new HashMap<>();

        @Override
        public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
            sent.computeIfAbsent(peerId, key -> new ArrayList<>()).add(request);
        }

        AppendEntriesRequestMessage lastRequest(int peerId) {
            List<AppendEntriesRequestMessage> list = sent.get(peerId);
            if (list == null || list.isEmpty()) {
                return null;
            }
            return list.get(list.size() - 1);
        }

        int totalRequests() {
            int total = 0;
            for (List<AppendEntriesRequestMessage> list : sent.values()) {
                total += list.size();
            }
            return total;
        }
    }

    private static final class FakeClock implements RaftClock {
        private int oneShotScheduleCount;
        private int fixedRateScheduleCount;
        private FakeScheduledTask lastOneShotTask;
        private FakeScheduledTask lastFixedRateTask;

        @Override
        public RaftScheduledTask scheduleOnce(long delayMs, Runnable task) {
            oneShotScheduleCount++;
            if (lastOneShotTask != null) {
                lastOneShotTask.cancel();
            }
            lastOneShotTask = new FakeScheduledTask(task);
            return lastOneShotTask;
        }

        @Override
        public RaftScheduledTask scheduleAtFixedRate(long initialDelayMs, long periodMs, Runnable task) {
            fixedRateScheduleCount++;
            if (lastFixedRateTask != null) {
                lastFixedRateTask.cancel();
            }
            lastFixedRateTask = new FakeScheduledTask(task);
            return lastFixedRateTask;
        }
    }

    private static final class FakeScheduledTask implements RaftScheduledTask {
        private final Runnable task;
        private boolean cancelled;

        private FakeScheduledTask(Runnable task) {
            this.task = task;
            this.cancelled = false;
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        void fire() {
            if (!cancelled) {
                task.run();
            }
        }
    }
}
