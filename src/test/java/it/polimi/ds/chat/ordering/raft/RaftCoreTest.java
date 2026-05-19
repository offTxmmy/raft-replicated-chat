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
                electionManager::onValidLeaderActivityObserved
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
