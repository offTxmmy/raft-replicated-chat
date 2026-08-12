package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
                null,
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
                null,
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
                null,
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
    void stoppedLeaderCannotAppendOrApplyACommand() {
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        List<RaftLogEntry> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, applied::add);
        RaftReplicationManager manager = newManager(
                node,
                log,
                commitManager,
                new RecordingObserver()
        );

        manager.stop();

        assertNull(manager.appendCommandAsLeader(command("post-stop")));
        assertEquals(0L, log.lastLogIndex());
        assertEquals(0L, commitManager.getCommitIndex());
        assertTrue(applied.isEmpty());
    }

    @Test
    // Serializes leader-only append with term/role transitions on the RaftNode.
    void appendCommandAsLeaderShouldSerializeAppendAgainstStepDown() throws Exception {
        RaftNode node = leaderNode(1);
        long leaderTerm = node.getCurrentTerm();

        BlockingAppendRaftLog log = new BlockingAppendRaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                new RecordingSender(),
                null,
                null
        );

        manager.start();
        manager.onLeaderElected(1, leaderTerm);

        AtomicReference<RaftLogEntry> appendedEntry = new AtomicReference<>();

        Thread appendThread = new Thread(
                () -> appendedEntry.set(manager.appendCommandAsLeader(command("x"))),
                "test-append"
        );

        appendThread.start();

        assertTrue(
                log.awaitAppendEntered(),
                "Append did not reach the controlled blocking point"
        );

        CountDownLatch stepDownCompleted = new CountDownLatch(1);

        Thread stepDownThread = new Thread(() -> {
            try {
                node.stepDownIfHigherTerm(leaderTerm + 1L);
            } finally {
                stepDownCompleted.countDown();
            }
        }, "test-step-down");

        stepDownThread.start();

        boolean stepDownCompletedBeforeAppend =
                waitUntilBlockedOrCompleted(stepDownThread, stepDownCompleted);

        log.releaseAppend();

        joinOrFail(appendThread);
        joinOrFail(stepDownThread);

        assertFalse(
                stepDownCompletedBeforeAppend,
                "Step-down must not complete while a leader-only log append is in progress"
        );

        RaftLogEntry entry = appendedEntry.get();
        assertNotNull(entry);
        assertEquals(leaderTerm, entry.getTerm());

        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(leaderTerm + 1L, node.getCurrentTerm());
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
        RaftLeaderActivityObserver electionOwner =
                (term, leaderId) -> node.becomeFollower(term, leaderId);
        RaftReplicationManager manager = newManager(node, electionOwner);

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
    void stoppedManagerShouldRejectAppendEntriesRequestWithoutMutation() {
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager =
                new RaftCommitManager(log, entry -> {});
        RecordingObserver leaderObserver = new RecordingObserver();
        RecordingHigherTermObserver higherTermObserver =
                new RecordingHigherTermObserver();

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                new RecordingSender(),
                leaderObserver,
                higherTermObserver
        );
        manager.start();
        manager.onLeaderElected(1, node.getCurrentTerm());
        manager.stop();

        AppendEntriesResponseMessage response = manager.handleAppendEntries(
                new AppendEntriesRequestMessage(
                        5L,
                        2,
                        0L,
                        0L,
                        List.of(new RaftLogEntry(1L, 5L, command("stopped"))),
                        1L
                )
        );

        assertFalse(response.isSuccess());
        assertEquals(1L, response.getTerm());
        assertEquals(RaftRole.LEADER, node.getRole());
        assertEquals(1L, node.getCurrentTerm());
        assertEquals(1, node.getLeaderId());
        assertEquals(0L, log.lastLogIndex());
        assertEquals(0, leaderObserver.calls);
        assertEquals(0, higherTermObserver.calls);
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
    // Ensures leader activity is reported only after releasing the replication manager lock.
    void handleAppendEntriesShouldNotifyOutsideReplicationManagerLock() {
        RaftNode node = followerNode(1);
        RaftReplicationManager[] managerRef = new RaftReplicationManager[1];
        boolean[] observerHeldLock = new boolean[1];

        RaftLeaderActivityObserver observer = (term, leaderId) ->
                observerHeldLock[0] = Thread.holdsLock(managerRef[0]);

        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        managerRef[0] = newManager(node, log, commitManager, observer);

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                2L,
                5,
                0L,
                0L,
                List.of(new RaftLogEntry(1L, 2L, command("a"))),
                1L
        );

        AppendEntriesResponseMessage response = managerRef[0].handleAppendEntries(request);

        assertTrue(response.isSuccess());
        assertFalse(observerHeldLock[0]);
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
        assertEquals(1, observer.calls);
        assertEquals(1L, observer.lastTerm);
        assertEquals(2, observer.lastLeaderId);
        assertEquals(1L, response.getConflictTerm());
        assertEquals(1L, response.getConflictIndex());
    }

    @Test
    // Rejects AppendEntries when the follower log is too short and returns a nextIndex hint.
    void handleAppendEntriesShouldProvideConflictIndexWhenLogTooShort() {
        RaftNode node = followerNodeWithTerm(1, 1L);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = newManager(node, log, commitManager, new RecordingObserver());

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                1L,
                2,
                5L,
                1L,
                List.of(),
                1L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertFalse(response.isSuccess());
        assertEquals(-1L, response.getConflictTerm());
        assertEquals(2L, response.getConflictIndex());
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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, node.getCurrentTerm());

        manager.onHeartbeatRoundDue(node.getCurrentTerm() - 1L);

        assertEquals(0, sender.totalRequests());
    }

    @Test
    // Prevents a heartbeat validated in one term from being rebuilt using a newer term after step-down.
    void heartbeatRoundShouldSerializeRequestBuildAgainstStepDown() throws Exception {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        long leaderTerm = node.getCurrentTerm();

        BlockingSendPlanRaftLog log = new BlockingSendPlanRaftLog();
        log.append(leaderTerm, command("a"));

        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null,
                null
        );

        manager.start();
        manager.onLeaderElected(1, leaderTerm);

        Thread heartbeatThread = new Thread(
                () -> manager.onHeartbeatRoundDue(leaderTerm),
                "test-heartbeat"
        );

        heartbeatThread.start();

        assertTrue(
                log.awaitSendPlanBuildEntered(),
                "Heartbeat did not reach the controlled send-plan blocking point"
        );

        CountDownLatch stepDownCompleted = new CountDownLatch(1);

        Thread stepDownThread = new Thread(() -> {
            try {
                node.stepDownIfHigherTerm(leaderTerm + 1L);
            } finally {
                stepDownCompleted.countDown();
            }
        }, "test-step-down");

        stepDownThread.start();

        boolean stepDownCompletedBeforePlanBuild =
                waitUntilBlockedOrCompleted(stepDownThread, stepDownCompleted);

        log.releaseSendPlanBuild();

        joinOrFail(heartbeatThread);
        joinOrFail(stepDownThread);

        assertFalse(
                stepDownCompletedBeforePlanBuild,
                "Step-down must not complete while AppendEntries requests are being built"
        );

        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(leaderTerm + 1L, node.getCurrentTerm());

        assertEquals(2, sender.totalRequests());

        AppendEntriesRequestMessage requestTo2 = sender.lastRequest(2);
        AppendEntriesRequestMessage requestTo3 = sender.lastRequest(3);

        assertNotNull(requestTo2);
        assertNotNull(requestTo3);

        assertEquals(leaderTerm, requestTo2.getTerm());
        assertEquals(leaderTerm, requestTo3.getTerm());
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
                null,
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
    // Groups identical empty AppendEntries heartbeats into one broadcast send.
    void heartbeatRoundShouldBroadcastIdenticalEmptyHeartbeats() {
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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.onHeartbeatRoundDue(1L);

        assertEquals(1, sender.broadcasts.size());
        BroadcastAppendEntries broadcast = sender.broadcasts.get(0);
        assertEquals(Set.of(2, 3), broadcast.peerIds());
        assertTrue(broadcast.request().getEntries().isEmpty());
        assertEquals(1L, broadcast.request().getPrevLogIndex());
        assertEquals(1L, broadcast.request().getPrevLogTerm());
        assertEquals(2, sender.totalRequests());
    }

    @Test
    // Keeps empty heartbeats unicast when only some followers can receive that request.
    void heartbeatRoundShouldNotBroadcastPartialEmptyHeartbeatGroup() {
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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.appendCommandAsLeader(command("b"));
        manager.onHeartbeatRoundDue(1L);
        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(1L, true, 2, 2L, -1L, 0L));

        sender.clear();
        manager.onHeartbeatRoundDue(1L);

        assertEquals(0, sender.broadcasts.size());
        assertTrue(sender.lastRequest(2).getEntries().isEmpty());
        assertEquals(1, sender.lastRequest(3).getEntries().size());
    }

    @Test
    // Backtracks nextIndex using the conflictIndex hint when the follower log is too short.
    void failedAppendEntriesResponseShouldUseConflictIndexHint() {
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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.onHeartbeatRoundDue(1L);
        AppendEntriesRequestMessage first = sender.lastRequest(2);
        assertEquals(3L, first.getPrevLogIndex());

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(1L, false, 2, 0L, -1L, 2L));
        manager.onHeartbeatRoundDue(1L);

        AppendEntriesRequestMessage second = sender.lastRequest(2);
        assertEquals(1L, second.getPrevLogIndex());
    }

    @Test
    // Uses conflictTerm to jump to the last index of that term when present in the leader log.
    void failedAppendEntriesResponseShouldUseConflictTermHint() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(2L, command("c"));
        log.append(2L, command("d"));
        log.append(3L, command("e"));
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.onHeartbeatRoundDue(1L);
        AppendEntriesRequestMessage first = sender.lastRequest(2);
        assertEquals(5L, first.getPrevLogIndex());

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(1L, false, 2, 0L, 2L, 3L));
        manager.onHeartbeatRoundDue(1L);

        AppendEntriesRequestMessage second = sender.lastRequest(2);
        assertEquals(4L, second.getPrevLogIndex());
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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.onHeartbeatRoundDue(1L);
        AppendEntriesRequestMessage first = sender.lastRequest(2);
        assertEquals(3L, first.getPrevLogIndex());

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(0L, false, 2, 0L, -1L, 0L));
        manager.onHeartbeatRoundDue(1L);

        AppendEntriesRequestMessage second = sender.lastRequest(2);
        assertEquals(3L, second.getPrevLogIndex());
    }

    @Test
    void obsoleteResponsesShouldNotRegressConfirmedReplicationProgress() {
        RecordingSender sender = new RecordingSender();
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        for (int index = 1; index <= 10; index++) {
            log.append(1L, command("entry-" + index));
        }

        RaftCommitManager commitManager =
                new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        // A current response proves that the follower contains entries through 10.
        manager.handleAppendEntriesResponse(
                2,
                new AppendEntriesResponseMessage(1L, true, 2, 10L, -1L, 0L)
        );

        // These two responses belong to older in-flight requests. Neither may
        // undo already confirmed match/next progress.
        manager.handleAppendEntriesResponse(
                2,
                new AppendEntriesResponseMessage(1L, true, 2, 5L, -1L, 0L)
        );
        manager.handleAppendEntriesResponse(
                2,
                new AppendEntriesResponseMessage(1L, false, 2, 0L, -1L, 2L)
        );

        manager.onHeartbeatRoundDue(1L);

        AppendEntriesRequestMessage nextRequest = sender.lastRequest(2);
        assertNotNull(nextRequest);
        assertEquals(
                10L,
                nextRequest.getPrevLogIndex(),
                "nextIndex must remain matchIndex + 1 after obsolete responses"
        );
        assertTrue(nextRequest.getEntries().isEmpty());
    }

    @Test
    // Delegates higher-term AppendEntries responses to the election layer.
    void higherTermAppendEntriesResponseShouldNotifyHigherTermObserver() {
        RecordingSender sender = new RecordingSender();
        RecordingHigherTermObserver higherTermObserver =
                new RecordingHigherTermObserver();

        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager =
                new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                sender,
                null,
                higherTermObserver
        );

        manager.start();
        manager.onLeaderElected(1, node.getCurrentTerm());

        manager.handleAppendEntriesResponse(
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

        assertEquals(1, higherTermObserver.calls);
        assertEquals(5L, higherTermObserver.lastTerm);
    }

    @Test
// Higher-term information must not be discarded only because the node is no longer leader.
    void higherTermAppendEntriesResponseShouldBeObservedEvenWhenNodeIsNotLeader() {
        RecordingHigherTermObserver higherTermObserver =
                new RecordingHigherTermObserver();

        RaftNode node = followerNodeWithTerm(1, 1L);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager =
                new RaftCommitManager(log, entry -> {});

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1, 2, 3),
                node,
                log,
                commitManager,
                new RecordingSender(),
                null,
                higherTermObserver
        );

        manager.start();

        manager.handleAppendEntriesResponse(
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

        assertEquals(1, higherTermObserver.calls);
        assertEquals(5L, higherTermObserver.lastTerm);
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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);
        manager.stop();

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(5L, false, 2, 0L, -1L, 0L));

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
                null,
                null
        );
        manager.start();
        manager.onLeaderElected(1, 1L);

        manager.handleAppendEntriesResponse(2, new AppendEntriesResponseMessage(1L, true, 2, 3L, -1L, 0L));

        assertEquals(3L, commitManager.getCommitIndex());
        assertEquals(List.of(1L, 2L, 3L), applied);
    }

    private RaftReplicationManager newManager(RaftNode node) {
        return newManager(node, new RecordingObserver());
    }

    private RaftReplicationManager newManager(RaftNode node, RaftLeaderActivityObserver observer) {
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        return newManager(node, log, commitManager, observer);
    }

    private RaftReplicationManager newManager(RaftNode node, RaftLog log, RaftCommitManager commitManager, RaftLeaderActivityObserver observer) {
        RaftReplicationManager manager = new RaftReplicationManager(
                node.getNodeId(),
                Set.of(node.getNodeId(), 2, 3),
                node,
                log,
                commitManager,
                new RecordingSender(),
                observer,
                node::stepDownIfHigherTerm
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

    private static boolean waitUntilBlockedOrCompleted(
            Thread thread,
            CountDownLatch completed
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);

        while (completed.getCount() > 0L
                && thread.getState() != Thread.State.BLOCKED) {

            if (System.nanoTime() >= deadline) {
                fail("Thread did not become blocked or complete within the timeout");
            }

            Thread.onSpinWait();
        }

        return completed.getCount() == 0L;
    }

    private static void joinOrFail(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(2));

        assertFalse(
                thread.isAlive(),
                "Thread did not terminate: " + thread.getName()
        );
    }

    private static class BlockingAppendRaftLog extends RaftLog {

        private final CountDownLatch appendEntered = new CountDownLatch(1);
        private final CountDownLatch appendRelease = new CountDownLatch(1);

        @Override
        public synchronized RaftLogEntry append(long term, ChatCommand command) {
            appendEntered.countDown();
            awaitRelease(appendRelease);
            return super.append(term, command);
        }

        boolean awaitAppendEntered() throws InterruptedException {
            return appendEntered.await(2, TimeUnit.SECONDS);
        }

        void releaseAppend() {
            appendRelease.countDown();
        }
    }

    private static class BlockingSendPlanRaftLog extends RaftLog {

        private final CountDownLatch sendPlanBuildEntered = new CountDownLatch(1);
        private final CountDownLatch sendPlanBuildRelease = new CountDownLatch(1);

        @Override
        public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex) {
            sendPlanBuildEntered.countDown();
            awaitRelease(sendPlanBuildRelease);
            return super.getEntriesFrom(startIndex);
        }

        boolean awaitSendPlanBuildEntered() throws InterruptedException {
            return sendPlanBuildEntered.await(2, TimeUnit.SECONDS);
        }

        void releaseSendPlanBuild() {
            sendPlanBuildRelease.countDown();
        }
    }

    private static void awaitRelease(CountDownLatch latch) {
        try {
            if (!latch.await(2, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for test release");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for test release", e);
        }
    }

    private static class RecordingSender implements RaftAppendEntriesSender {
        private final Map<Integer, List<AppendEntriesRequestMessage>> sent = new HashMap<>();
        private final List<BroadcastAppendEntries> broadcasts = new ArrayList<>();

        @Override
        public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
            sent.computeIfAbsent(peerId, key -> new ArrayList<>()).add(request);
        }

        @Override
        public void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
            broadcasts.add(new BroadcastAppendEntries(request, Set.copyOf(peerIds)));
            RaftAppendEntriesSender.super.broadcastAppendEntries(request, peerIds);
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

        void clear() {
            sent.clear();
            broadcasts.clear();
        }
    }

    private record BroadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
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

    private static class RecordingHigherTermObserver
            implements RaftHigherTermObserver {

        int calls;
        long lastTerm;

        @Override
        public void onHigherTermObserved(long term) {
            calls++;
            lastTerm = term;
        }
    }
}
