package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.messages.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.messages.raft.ChatCommand;
import it.polimi.ds.chat.messages.raft.RaftLogEntry;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RaftReplicationManagerTest {

    @Test
    // Rejects construction when the voting set is empty.
    void constructorShouldRejectEmptyVotingSet() {
        RaftNode node = followerNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RecordingSender sender = new RecordingSender();

        assertThrows(IllegalArgumentException.class, () -> new RaftReplicationManager(
                1,
                Set.of(),
                node,
                log,
                commitManager,
                sender,
                null
        ));
    }

    @Test
    // Rejects construction when the local node is not in the voting set.
    void constructorShouldRejectWhenLocalNotInVotingSet() {
        RaftNode node = followerNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RecordingSender sender = new RecordingSender();

        assertThrows(IllegalArgumentException.class, () -> new RaftReplicationManager(
                1,
                Set.of(2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        ));
    }

    @Test
    // Stops replication and prevents heartbeat sends until restarted.
    void stopShouldPreventHeartbeatSends() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        log.append(node.getCurrentTerm(), command("a"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, node.getCurrentTerm());

        manager.onHeartbeatRoundDue(node.getCurrentTerm());
        int sentBeforeStop = sender.totalRequests();

        manager.stop();
        manager.onHeartbeatRoundDue(node.getCurrentTerm());
        assertEquals(sentBeforeStop, sender.totalRequests());

        manager.start();
        manager.onHeartbeatRoundDue(node.getCurrentTerm());
        assertEquals(sentBeforeStop, sender.totalRequests());
    }

    @Test
    // Ensures non-leaders cannot append new commands.
    void appendCommandAsLeaderShouldReturnNullWhenNotLeader() {
        RaftReplicationManager manager = newManager(followerNode(1));

        assertNull(manager.appendCommandAsLeader(command("x")));
    }

    @Test
    // Appends a new entry when the node is the leader.
    void appendCommandAsLeaderShouldAppendWhenLeader() {
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = newManager(node, log, commitManager, new RecordingObserver());

        RaftLogEntry entry = manager.appendCommandAsLeader(command("x"));

        assertNotNull(entry);
        assertEquals(1L, entry.getIndex());
        assertEquals(node.getCurrentTerm(), entry.getTerm());
        assertEquals(1L, log.lastLogIndex());
    }

    @Test
    // Rejects AppendEntries with a stale term and avoids leader activity notification.
    void handleAppendEntriesShouldRejectStaleTerm() {
        RaftNode node = followerNodeWithTerm(1, 1L);
        RecordingObserver observer = new RecordingObserver();
        RaftReplicationManager manager = newManager(node, observer);

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                0L,
                2,
                0L,
                0L,
                List.of(),
                0L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(1L, response.getTerm());
        assertEquals(0, observer.calls);
    }

    @Test
    // Steps down a leader when it receives a valid AppendEntries for its term.
    void handleAppendEntriesShouldStepDownWhenLeaderReceivesAppendEntries() {
        RaftNode node = leaderNode(1);
        RaftReplicationManager manager = newManager(node, new RecordingObserver());

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                node.getCurrentTerm(),
                2,
                0L,
                0L,
                List.of(),
                0L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertTrue(response.isSuccess());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(2, node.getLeaderId());
    }

    @Test
    // Accepts AppendEntries, appends entries, advances commit, and notifies leader activity.
    void handleAppendEntriesShouldAppendCommitAndNotify() {
        RaftNode node = followerNode(1);
        RecordingObserver observer = new RecordingObserver();
        List<Long> applied = new ArrayList<>();

        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));
        RaftReplicationManager manager = newManager(node, log, commitManager, observer);

        List<RaftLogEntry> entries = List.of(
                new RaftLogEntry(1L, 2L, command("a"))
        );

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                2L,
                5,
                0L,
                0L,
                entries,
                1L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertTrue(response.isSuccess());
        assertEquals(2L, node.getCurrentTerm());
        assertEquals(1L, log.lastLogIndex());
        assertEquals(1L, commitManager.getCommitIndex());
        assertEquals(List.of(1L), applied);
        assertEquals(1, observer.calls);
        assertEquals(2L, observer.lastTerm);
        assertEquals(5, observer.lastLeaderId);
    }

    @Test
    // Advances commit index from a heartbeat even when no new entries are sent.
    void handleAppendEntriesShouldApplyLeaderCommitOnHeartbeat() {
        RaftNode node = followerNodeWithTerm(1, 1L);
        List<Long> applied = new ArrayList<>();

        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));
        RaftReplicationManager manager = newManager(node, log, commitManager, new RecordingObserver());

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                1L,
                2,
                1L,
                1L,
                List.of(),
                1L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertTrue(response.isSuccess());
        assertEquals(1L, commitManager.getCommitIndex());
        assertEquals(List.of(1L), applied);
    }

    @Test
    // Rejects AppendEntries when prevLogIndex/prevLogTerm do not match.
    void handleAppendEntriesShouldRejectOnPrevMismatch() {
        RaftNode node = followerNodeWithTerm(1, 1L);
        RecordingObserver observer = new RecordingObserver();

        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = newManager(node, log, commitManager, observer);

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                1L,
                2,
                1L,
                2L,
                List.of(new RaftLogEntry(2L, 1L, command("b"))),
                1L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(1L, log.lastLogIndex());
        assertEquals(0, observer.calls);
    }

    @Test
    // Ignores heartbeat rounds when the term does not match the leader's term.
    void heartbeatRoundShouldIgnoreTermMismatch() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, node.getCurrentTerm());

        manager.onHeartbeatRoundDue(node.getCurrentTerm() - 1L);

        assertEquals(0, sender.totalRequests());
    }

    @Test
    // Sends entries starting from the follower nextIndex on heartbeat rounds.
    void heartbeatRoundShouldSendEntriesFromNextIndex() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.appendCommandAsLeader(command("b"));
        manager.onHeartbeatRoundDue(1L);

        assertEquals(2, sender.totalRequests());
        AppendEntriesRequestMessage request = sender.lastRequest(2);
        assertNotNull(request);
        assertEquals(1L, request.getPrevLogIndex());
        assertEquals(1L, request.getPrevLogTerm());
        assertEquals(1, request.getEntries().size());
        assertEquals("b", request.getEntries().get(0).getCommand().getLocalMsgId());
    }

    @Test
    // Backtracks nextIndex after a failed AppendEntries response.
    void failedAppendEntriesResponseShouldBacktrackNextIndex() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(1L, command("c"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.onHeartbeatRoundDue(1L);
        AppendEntriesRequestMessage first = sender.lastRequest(2);
        assertEquals(3L, first.getPrevLogIndex());

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(1L, false, 2, 0L));
        manager.onHeartbeatRoundDue(1L);

        AppendEntriesRequestMessage second = sender.lastRequest(2);
        assertEquals(2L, second.getPrevLogIndex());
    }

    @Test
    // Ignores stale AppendEntries responses without changing nextIndex.
    void staleAppendEntriesResponseShouldBeIgnored() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(1L, command("c"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.onHeartbeatRoundDue(1L);
        AppendEntriesRequestMessage first = sender.lastRequest(2);
        assertEquals(3L, first.getPrevLogIndex());

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(0L, false, 2, 0L));
        manager.onHeartbeatRoundDue(1L);

        AppendEntriesRequestMessage second = sender.lastRequest(2);
        assertEquals(3L, second.getPrevLogIndex());
    }

    @Test
    // Steps down and stops sending when a higher-term response is received.
    void higherTermAppendEntriesResponseShouldStepDownLeader() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(5L, false, 2, 0L));

        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(5L, node.getCurrentTerm());

        manager.onHeartbeatRoundDue(5L);
        assertEquals(0, sender.totalRequests());
    }

    @Test
    // Ignores AppendEntries responses when the manager is stopped.
    void stoppedManagerShouldIgnoreAppendEntriesResponse() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);
        manager.stop();

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(5L, false, 2, 0L));

        assertEquals(RaftRole.LEADER, node.getRole());
        assertEquals(1L, node.getCurrentTerm());
    }

    @Test
    // Advances commit index when a majority match is observed.
    void successfulAppendEntriesResponseShouldAdvanceCommitOnMajority() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(1L, command("c"));

        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(1L, true, 2, 3L));

        assertEquals(3L, commitManager.getCommitIndex());
        assertEquals(List.of(1L, 2L, 3L), applied);
    }

    private RaftReplicationManager newManager(RaftNode node) {
        return newManager(node, new RecordingObserver());
    }

    private RaftReplicationManager newManager(RaftNode node, RecordingObserver observer) {
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        return newManager(node, log, commitManager, observer);
    }

    private RaftReplicationManager newManager(RaftNode node, RaftLog log, RaftCommitManager commitManager, RecordingObserver observer) {
        RaftReplicationManager manager = new RaftReplicationManager(
                node.getNodeId(),
                Set.of(node.getNodeId(), 2, 3),
                node,
                log,
                commitManager,
                new RecordingSender(),
                observer
        );
        manager.start();
        return manager;
    }

    private RaftNode leaderNode(int nodeId) {
        RaftNode node = new RaftNode(nodeId);
        node.startElection();
        node.becomeLeader();
        return node;
    }

    private RaftNode followerNode(int nodeId) {
        return new RaftNode(nodeId);
    }

    private RaftNode followerNodeWithTerm(int nodeId, long term) {
        RaftNode node = new RaftNode(nodeId);
        if (term > 0L) {
            node.startElection();
            node.becomeFollower(term, RaftNode.NO_LEADER);
        }
        return node;
    }

    private ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }

    private static class RecordingSender implements RaftAppendEntriesSender {
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

    private static class RecordingObserver implements RaftLeaderActivityObserver {
        int calls;
        long lastTerm;
        int lastLeaderId;

        @Override
        public void onValidLeaderActivityObserved(long term, int leaderId) {
            calls++;
            lastTerm = term;
            lastLeaderId = leaderId;
        }
    }
}
