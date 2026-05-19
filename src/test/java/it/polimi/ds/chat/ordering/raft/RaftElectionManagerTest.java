package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RaftElectionManagerTest {

    /**
     * Verifies that the manager derives the peer voting set and majority threshold
     * from the configured static voting membership.
     */
    @Test
    void shouldComputePeerVotingSetAndMajorityFromStaticMembership() {
        FakeClock fakeClock = new FakeClock();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, new RecordingVoteRequestSender());

        assertEquals(2, manager.getLocalNodeId());
        assertEquals(setOf(1, 2, 3, 4, 5), manager.getAllVotingNodeIds());
        assertEquals(setOf(1, 3, 4, 5), manager.getPeerVotingNodeIds());
        assertEquals(3, manager.getMajority());
    }

    /**
     * Verifies that construction fails if the local node does not belong to the
     * configured static voting set.
     */
    @Test
    void shouldRejectConstructionIfLocalNodeIsNotInStaticVotingSet() {
        FakeClock fakeClock = new FakeClock();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> newManager(9, setOf(1, 2, 3), fakeClock, new RecordingVoteRequestSender()));

        assertTrue(error.getMessage().contains("local node"));
    }

    /**
     * Verifies that starting the manager schedules exactly one election timeout
     * and does not start heartbeat scheduling.
     */
    @Test
    void startShouldScheduleExactlyOneElectionTimeout() {
        FakeClock fakeClock = new FakeClock();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, new RecordingVoteRequestSender());

        manager.start();

        assertTrue(manager.isRunning());
        assertEquals(1, fakeClock.oneShotScheduleCount);
        assertEquals(0, fakeClock.fixedRateScheduleCount);
        assertNotNull(fakeClock.lastOneShotTask);
        assertTrue(fakeClock.lastOneShotDelayMs >= manager.getElectionTimeoutMinMs());
        assertTrue(fakeClock.lastOneShotDelayMs <= manager.getElectionTimeoutMaxMs());
        assertFalse(fakeClock.lastOneShotTask.cancelled);
    }

    /**
     * Verifies that calling start multiple times is idempotent and does not create
     * additional election timeouts.
     */
    @Test
    void repeatedStartShouldBeIdempotent() {
        FakeClock fakeClock = new FakeClock();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, new RecordingVoteRequestSender());

        manager.start();
        manager.start();

        assertEquals(1, fakeClock.oneShotScheduleCount);
    }

    /**
     * Verifies that stopping the manager cancels the active election timeout and
     * clears any election tracking state.
     */
    @Test
    void stopShouldCancelElectionTimeoutAndClearElectionTracking() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // starts election

        FakeScheduledTask activeTask = fakeClock.lastOneShotTask;
        assertNotNull(manager.getCurrentElectionTerm());
        assertFalse(manager.getGrantedVotersSnapshot().isEmpty());

        manager.stop();

        assertFalse(manager.isRunning());
        assertTrue(activeTask.cancelled);
        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());
    }

    /**
     * Verifies that resetting the election timeout cancels the previous timeout
     * and schedules a fresh one.
     */
    @Test
    void resetElectionTimeoutShouldCancelPreviousTaskAndScheduleANewOne() {
        FakeClock fakeClock = new FakeClock();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, new RecordingVoteRequestSender());
        manager.start();

        FakeScheduledTask firstTask = fakeClock.lastOneShotTask;

        manager.resetElectionTimeout();

        assertEquals(2, fakeClock.oneShotScheduleCount);
        assertTrue(firstTask.cancelled);
        assertNotSame(firstTask, fakeClock.lastOneShotTask);
        assertFalse(fakeClock.lastOneShotTask.cancelled);
    }

    /**
     * Verifies that heartbeat scheduling can be started and stopped correctly.
     */
    @Test
    void shouldStartAndStopHeartbeatSchedule() {
        FakeClock fakeClock = new FakeClock();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, new RecordingVoteRequestSender());

        manager.startHeartbeatSchedule();

        assertEquals(1, fakeClock.fixedRateScheduleCount);
        assertEquals(manager.getHeartbeatIntervalMs(), fakeClock.lastFixedRateInitialDelayMs);
        assertEquals(manager.getHeartbeatIntervalMs(), fakeClock.lastFixedRatePeriodMs);
        assertNotNull(fakeClock.lastFixedRateTask);
        assertFalse(fakeClock.lastFixedRateTask.cancelled);

        manager.stopHeartbeatSchedule();

        assertTrue(fakeClock.lastFixedRateTask.cancelled);
    }

    /**
     * Verifies that an election timeout starts a new election, records the self-vote,
     * and sends a RequestVote message to all configured peer voters.
     */
    @Test
    void electionTimeoutShouldStartElectionTrackSelfVoteAndSendRequestVoteToAllPeers() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire();

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertEquals(Integer.valueOf(2), managerNode(manager).getVotedFor());

        assertEquals(Long.valueOf(1L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());

        assertEquals(3, sender.sentRequests.size());
        assertEquals(setOf(1, 3, 4), sender.destinationPeerIds());

        for (SentVoteRequest sent : sender.sentRequests) {
            assertEquals(1L, sent.request().getTerm());
            assertEquals(2, sent.request().getCandidateId());
            assertEquals(0L, sent.request().getLastLogIndex());
            assertEquals(0L, sent.request().getLastLogTerm());
        }
    }

    /**
     * Verifies that RequestVote messages are built using the current local log metadata.
     */
    @Test
    void electionTimeoutShouldUseCurrentLogMetadataWhenBuildingRequestVote() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftNode raftNode = new RaftNode(7);

        RaftElectionManager manager = new RaftElectionManager(
                7,
                setOf(7, 8, 9),
                150L,
                300L,
                50L,
                raftNode,
                new FakeLogMetadata(12L, 5L),
                sender,
                fakeClock,
                null
        );

        manager.start();
        fakeClock.lastOneShotTask.fire();

        assertEquals(2, sender.sentRequests.size());
        for (SentVoteRequest sent : sender.sentRequests) {
            assertEquals(12L, sent.request().getLastLogIndex());
            assertEquals(5L, sent.request().getLastLogTerm());
        }
    }

    /**
     * Verifies that RequestVote messages are built using the current local log metadata,
     * even if it changes between the moment the election is started and the timeout fires.
     */
    @Test
    void electionTimeoutShouldReadLiveRaftLogMetadataAtElectionTime() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftNode raftNode = new RaftNode(7);
        RaftLog raftLog = new RaftLog();

        RaftElectionManager manager = new RaftElectionManager(
                7,
                setOf(7, 8, 9),
                150L,
                300L,
                50L,
                raftNode,
                raftLog,
                sender,
                fakeClock,
                null
        );

        manager.start();

        // The manager was already created when the log was empty.
        // These appends happen later, before the election actually starts.
        raftLog.append(2L, new ChatCommand("m1", 7, "alice", "one", new VectorClock()));
        raftLog.append(2L, new ChatCommand("m2", 7, "alice", "two", new VectorClock()));
        raftLog.append(3L, new ChatCommand("m3", 7, "alice", "three", new VectorClock()));

        fakeClock.lastOneShotTask.fire();

        assertEquals(2, sender.sentRequests.size());

        for (SentVoteRequest sent : sender.sentRequests) {
            assertEquals(1L, sent.request().getTerm());
            assertEquals(7, sent.request().getCandidateId());

            // Critical assertion:
            // RequestVote must contain the current log tip, not the empty startup snapshot.
            assertEquals(3L, sent.request().getLastLogIndex());
            assertEquals(3L, sent.request().getLastLogTerm());
        }
    }

    /**
     * Verifies that after starting an election, a new election timeout is armed so
     * the election can be retried if it does not complete.
     */
    @Test
    void electionTimeoutShouldRearmAnotherElectionTimeoutForRetry() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        FakeScheduledTask firstTimeout = fakeClock.lastOneShotTask;

        firstTimeout.fire();

        assertEquals(2, fakeClock.oneShotScheduleCount);
        assertTrue(firstTimeout.cancelled);
        assertNotSame(firstTimeout, fakeClock.lastOneShotTask);
        assertFalse(fakeClock.lastOneShotTask.cancelled);
    }

    /**
     * Verifies that an election timeout has no effect when the manager is not running.
     */
    @Test
    void electionTimeoutShouldDoNothingIfManagerIsStopped() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.onElectionTimeoutFired();

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(0L, managerNode(manager).getCurrentTerm());
        assertTrue(sender.sentRequests.isEmpty());
        assertNull(manager.getCurrentElectionTerm());
    }

    /**
     * Verifies that an election timeout is ignored when the local node is already leader.
     */
    @Test
    void electionTimeoutShouldDoNothingIfNodeIsAlreadyLeader() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        managerNode(manager).startElection();
        managerNode(manager).becomeLeader();

        manager.start();
        fakeClock.lastOneShotTask.fire();

        assertEquals(RaftRole.LEADER, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertTrue(sender.sentRequests.isEmpty());
        assertNull(manager.getCurrentElectionTerm());
    }

    /**
     * Verifies that a granted vote response is counted and that reaching majority
     * promotes the local node to leader.
     */
    @Test
    void grantedVoteResponseShouldBeCountedAndMajorityShouldPromoteLeader() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // start election in term 1

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1));

        assertEquals(RaftRole.LEADER, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertEquals(2, managerNode(manager).getLeaderId());
        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());

        assertEquals(1, listener.leaderElectedCount);
        assertEquals(2, listener.lastLeaderId);
        assertEquals(1L, listener.lastLeaderTerm);

        assertEquals(1, fakeClock.fixedRateScheduleCount);
        assertNotNull(fakeClock.lastFixedRateTask);
        assertFalse(fakeClock.lastFixedRateTask.cancelled);
    }

    /**
     * Verifies that duplicate granted votes from the same voter are counted only once.
     */
    @Test
    void duplicateGrantedVoteShouldNotBeCountedTwice() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // term 1, self-vote already counted

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1));
        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1));

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(setOf(2, 1), manager.getGrantedVotersSnapshot());
    }

    /**
     * Verifies that stale vote responses from lower terms are ignored.
     */
    @Test
    void staleVoteResponseShouldBeIgnored() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // local term = 1

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(0L, true, 1));

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(Long.valueOf(1L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());
    }

    /**
     * Verifies that a denied vote response in the current term does not cause step-down
     * and does not change the tracked election.
     */
    @Test
    void deniedVoteResponseInSameTermShouldNotStepDown() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // local term = 1

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, false, 1));

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(Long.valueOf(1L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());
    }

    /**
     * Verifies that a higher-term vote response forces the node to step down,
     * clears election tracking, and resets the follower timeout.
     */
    @Test
    void higherTermVoteResponseShouldForceStepDownAndResetElectionTimeout() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // term 1 candidate
        FakeScheduledTask retryTask = fakeClock.lastOneShotTask;

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(5L, false, 1));

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(5L, managerNode(manager).getCurrentTerm());
        assertNull(managerNode(manager).getVotedFor());
        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());

        assertTrue(retryTask.cancelled);
        assertEquals(3, fakeClock.oneShotScheduleCount); // start + retry after election + reset after step-down

        assertEquals(1, listener.steppedDownCount);
        assertEquals(5L, listener.lastSteppedDownTerm);
        assertEquals(RaftNode.NO_LEADER, listener.lastKnownLeaderId);
    }

    /**
     * Verifies that vote responses are ignored if the local node is no longer a candidate.
     */
    @Test
    void responseShouldBeIgnoredIfNodeIsNoLongerCandidate() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire();
        managerNode(manager).becomeFollower(1L, 1);

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1));

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());
    }

    /**
     * Verifies that a single-node cluster becomes leader immediately after its self-vote,
     * without sending any RequestVote messages.
     */
    @Test
    void singleNodeClusterShouldBecomeLeaderImmediatelyAfterSelfVote() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(2), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire();

        assertEquals(RaftRole.LEADER, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertTrue(sender.sentRequests.isEmpty());
        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());

        assertEquals(1, listener.leaderElectedCount);
        assertEquals(2, listener.lastLeaderId);
        assertEquals(1L, listener.lastLeaderTerm);
    }

    /**
     * Verifies that granting an incoming RequestVote request resets the election timeout.
     */
    @Test
    void grantedIncomingVoteRequestShouldResetElectionTimeout() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        FakeScheduledTask firstTimeout = fakeClock.lastOneShotTask;

        RequestVoteResponseMessage response = manager.onRequestVoteRequest(
                new RequestVoteRequestMessage(1L, 1, 0L, 0L)
        );

        assertTrue(response.isVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals(2, response.getVoterId());

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertEquals(Integer.valueOf(1), managerNode(manager).getVotedFor());

        assertTrue(firstTimeout.cancelled);
        assertEquals(2, fakeClock.oneShotScheduleCount);
    }

    /**
     * Verifies that denying an incoming RequestVote request in the same term does not
     * reset the election timeout.
     */
    @Test
    void deniedIncomingVoteRequestInSameTermShouldNotResetElectionTimeout() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // become candidate in term 1
        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;

        RequestVoteResponseMessage response = manager.onRequestVoteRequest(
                new RequestVoteRequestMessage(1L, 1, 0L, 0L)
        );

        assertFalse(response.isVoteGranted());
        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(Long.valueOf(1L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());

        assertSame(retryTimeout, fakeClock.lastOneShotTask);
        assertFalse(retryTimeout.cancelled);
        assertEquals(2, fakeClock.oneShotScheduleCount);
    }

    /**
     * Verifies that a higher-term incoming RequestVote request forces step-down,
     * clears election tracking, and resets the election timeout.
     */
    @Test
    void higherTermIncomingVoteRequestShouldStepDownClearElectionTrackingAndResetTimeout() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // candidate in term 1
        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;

        RequestVoteResponseMessage response = manager.onRequestVoteRequest(
                new RequestVoteRequestMessage(5L, 1, 0L, 0L)
        );

        assertTrue(response.isVoteGranted());
        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(5L, managerNode(manager).getCurrentTerm());
        assertEquals(Integer.valueOf(1), managerNode(manager).getVotedFor());

        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());

        assertTrue(retryTimeout.cancelled);
        assertEquals(3, fakeClock.oneShotScheduleCount);

        assertEquals(1, listener.steppedDownCount);
        assertEquals(5L, listener.lastSteppedDownTerm);
    }

    /**
     * Verifies that valid leader activity observed in the same term causes a candidate
     * to step down to follower and reset its timeout.
     */
    @Test
    void validLeaderActivityInSameTermShouldStepDownCandidateAndResetTimeout() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // candidate in term 1
        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;

        manager.onValidLeaderActivityObserved(1L, 1);

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertEquals(1, managerNode(manager).getLeaderId());

        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());

        assertTrue(retryTimeout.cancelled);
        assertEquals(3, fakeClock.oneShotScheduleCount);

        assertEquals(1, listener.steppedDownCount);
        assertEquals(1L, listener.lastSteppedDownTerm);
        assertEquals(1, listener.lastKnownLeaderId);
    }

    /**
     * Verifies that higher-term leader activity forces a local leader to step down,
     * stop heartbeat scheduling, and arm a follower election timeout.
     */
    @Test
    void higherTermLeaderActivityShouldStepDownLeaderStopHeartbeatAndArmFollowerTimeout() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // candidate in term 1
        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1)); // become leader

        FakeScheduledTask heartbeatTask = fakeClock.lastFixedRateTask;

        manager.onValidLeaderActivityObserved(5L, 1);

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(5L, managerNode(manager).getCurrentTerm());
        assertEquals(1, managerNode(manager).getLeaderId());

        assertTrue(heartbeatTask.cancelled);
        assertEquals(3, fakeClock.oneShotScheduleCount);

        assertEquals(1, listener.steppedDownCount);
        assertEquals(5L, listener.lastSteppedDownTerm);
        assertEquals(1, listener.lastKnownLeaderId);
    }

    /**
     * Verifies that a heartbeat tick emits a heartbeat-round event when the local node is leader.
     */
    @Test
    void heartbeatTickShouldEmitEventWhenNodeIsLeader() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // become candidate in term 1
        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1)); // become leader

        manager.onHeartbeatTick();

        assertEquals(1, listener.heartbeatRoundDueCount);
        assertEquals(1L, listener.lastHeartbeatTerm);
    }

    /**
     * Verifies that a heartbeat tick does nothing when the local node is not leader.
     */
    @Test
    void heartbeatTickShouldDoNothingIfNodeIsNotLeader() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        manager.onHeartbeatTick(); // still follower

        assertEquals(0, listener.heartbeatRoundDueCount);

        fakeClock.lastOneShotTask.fire(); // become candidate
        manager.onHeartbeatTick();

        assertEquals(0, listener.heartbeatRoundDueCount);
    }

    /**
     * Verifies that a heartbeat tick does nothing when the manager has been stopped.
     */
    @Test
    void heartbeatTickShouldDoNothingIfManagerIsStopped() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // become candidate
        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1)); // become leader
        manager.stop();

        manager.onHeartbeatTick();

        assertEquals(0, listener.heartbeatRoundDueCount);
    }

    /**
     * Verifies that a split vote does not incorrectly promote the local node to leader
     * without reaching majority.
     */
    @Test
    void splitVoteShouldNotPromoteLeaderWithoutMajority() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // term 1, self-vote = {2}

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1));
        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, false, 3));

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(1L, managerNode(manager).getCurrentTerm());
        assertEquals(Long.valueOf(1L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2, 1), manager.getGrantedVotersSnapshot());

        assertEquals(0, fakeClock.fixedRateScheduleCount);
        assertEquals(0, listener.leaderElectedCount);
    }

    /**
     * Verifies that if an election remains unresolved, the next timeout starts a new
     * election with a higher term.
     */
    @Test
    void retryTimeoutAfterUnresolvedElectionShouldStartNewElectionWithHigherTerm() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // first election -> term 1

        assertEquals(4, sender.sentRequests.size());
        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;

        retryTimeout.fire(); // second election -> term 2

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(2L, managerNode(manager).getCurrentTerm());
        assertEquals(Integer.valueOf(2), managerNode(manager).getVotedFor());
        assertEquals(Long.valueOf(2L), manager.getCurrentElectionTerm());

        assertEquals(8, sender.sentRequests.size());
        for (int i = 4; i < 8; i++) {
            assertEquals(2L, sender.sentRequests.get(i).request().getTerm());
            assertEquals(2, sender.sentRequests.get(i).request().getCandidateId());
        }
    }

    /**
     * Verifies that when a new election starts after a retry, previously granted votes
     * are cleared and only the self-vote is retained.
     */
    @Test
    void newElectionRetryShouldClearOldGrantedVotesAndRestartFromSelfVoteOnly() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // first election -> term 1

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1));
        assertEquals(setOf(2, 1), manager.getGrantedVotersSnapshot());

        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;
        retryTimeout.fire(); // second election -> term 2

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(2L, managerNode(manager).getCurrentTerm());
        assertEquals(Long.valueOf(2L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());
    }

    /**
     * Verifies that a granted vote response from a previous election term is ignored
     * after a retry has already started a newer election.
     */
    @Test
    void oldGrantedVoteResponseArrivingAfterRetryShouldBeIgnored() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, sender);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // first election -> term 1

        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;
        retryTimeout.fire(); // second election -> term 2

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(1L, true, 1)); // stale response from old election

        assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
        assertEquals(2L, managerNode(manager).getCurrentTerm());
        assertEquals(Long.valueOf(2L), manager.getCurrentElectionTerm());
        assertEquals(setOf(2), manager.getGrantedVotersSnapshot());
    }

    /**
     * Verifies that granted votes received for the retried election are counted only
     * against the new term and can still lead to leadership.
     */
    @Test
    void grantedVotesForRetriedElectionShouldBeCountedAgainstNewTermOnly() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // first election -> term 1

        FakeScheduledTask retryTimeout = fakeClock.lastOneShotTask;
        retryTimeout.fire(); // second election -> term 2

        manager.onRequestVoteResponse(new RequestVoteResponseMessage(2L, true, 1));

        assertEquals(RaftRole.LEADER, managerNode(manager).getRole());
        assertEquals(2L, managerNode(manager).getCurrentTerm());
        assertEquals(2, managerNode(manager).getLeaderId());

        assertNull(manager.getCurrentElectionTerm());
        assertTrue(manager.getGrantedVotersSnapshot().isEmpty());

        assertEquals(1, listener.leaderElectedCount);
        assertEquals(2, listener.lastLeaderId);
        assertEquals(2L, listener.lastLeaderTerm);
    }

    /**
     * §1.2 — onLeaderObserved fires when a follower learns a new leader id from
     * NO_LEADER. This is the case the previous listener API did not signal.
     */
    @Test
    void leaderObservedShouldFireWhenFollowerLearnsLeaderForTheFirstTime() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start(); // FOLLOWER, leaderId = NO_LEADER, term = 0

        manager.onValidLeaderActivityObserved(1L, 1);

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(1, managerNode(manager).getLeaderId());
        assertEquals(1L, managerNode(manager).getCurrentTerm());

        assertEquals(1, listener.leaderObservedCount);
        assertEquals(1, listener.lastObservedLeaderId);
        assertEquals(1L, listener.lastObservedLeaderTerm);
    }

    /**
     * §1.2 — onLeaderObserved is NOT fired when subsequent heartbeats from the
     * same leader in the same term are observed. Only changes of leader id matter.
     */
    @Test
    void leaderObservedShouldNotFireOnRepeatedHeartbeatsFromSameLeader() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();

        manager.onValidLeaderActivityObserved(1L, 1);
        manager.onValidLeaderActivityObserved(1L, 1);
        manager.onValidLeaderActivityObserved(1L, 1);

        assertEquals(1, listener.leaderObservedCount);
        assertEquals(1, listener.lastObservedLeaderId);
    }

    /**
     * §1.2 — onLeaderObserved fires again when the locally known leader id
     * changes within the same term (e.g. cluster re-routed activity to a
     * different leader after a transient anomaly).
     */
    @Test
    void leaderObservedShouldFireAgainWhenLeaderIdChangesWithinSameTerm() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4), fakeClock, sender, listener);

        manager.start();

        manager.onValidLeaderActivityObserved(3L, 1);
        manager.onValidLeaderActivityObserved(3L, 4);

        assertEquals(2, listener.leaderObservedCount);
        assertEquals(4, listener.lastObservedLeaderId);
        assertEquals(3L, listener.lastObservedLeaderTerm);
    }

    /**
     * §1.2 — A higher-term leader activity observed by a CANDIDATE causes both
     * onSteppedDown and onLeaderObserved (the locally known leader id changes
     * from NO_LEADER to the new real leader). Upper layers will deduplicate.
     */
    @Test
    void leaderObservedShouldFireAlsoOnHigherTermStepDownFromCandidate() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3), fakeClock, sender, listener);

        manager.start();
        fakeClock.lastOneShotTask.fire(); // becomes CANDIDATE in term 1, leaderId = NO_LEADER

        manager.onValidLeaderActivityObserved(5L, 3);

        assertEquals(RaftRole.FOLLOWER, managerNode(manager).getRole());
        assertEquals(5L, managerNode(manager).getCurrentTerm());
        assertEquals(3, managerNode(manager).getLeaderId());

        assertEquals(1, listener.steppedDownCount);
        assertEquals(1, listener.leaderObservedCount);
        assertEquals(3, listener.lastObservedLeaderId);
        assertEquals(5L, listener.lastObservedLeaderTerm);
    }

    /**
     * §1.5 — Tolerance to lost vote requests/responses.
     * With a sender that drops every RequestVote (no response ever returns), the
     * candidate must keep starting fresh elections at strictly increasing terms,
     * and its internal state must stay consistent (CANDIDATE, self-vote only,
     * tracked election term equal to currentTerm). This is the liveness guarantee
     * under UDP packet loss.
     */
    @Test
    void lossyVoteRequestSenderShouldKeepStartingElectionsAtIncreasingTerms() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RaftElectionManager manager = newManager(2, setOf(1, 2, 3, 4, 5), fakeClock, sender);

        manager.start();

        final int rounds = 5;
        for (long expectedTerm = 1L; expectedTerm <= rounds; expectedTerm++) {
            fakeClock.lastOneShotTask.fire(); // election timeout, no responses ever arrive

            assertEquals(RaftRole.CANDIDATE, managerNode(manager).getRole());
            assertEquals(expectedTerm, managerNode(manager).getCurrentTerm());
            assertEquals(Integer.valueOf(2), managerNode(manager).getVotedFor());
            assertEquals(Long.valueOf(expectedTerm), manager.getCurrentElectionTerm());
            assertEquals(setOf(2), manager.getGrantedVotersSnapshot());
        }

        // Each election round broadcasts to all 4 peers; with 5 rounds we expect 20 sends.
        assertEquals(rounds * 4, sender.sentRequests.size());
        for (int i = 0; i < sender.sentRequests.size(); i++) {
            long expectedTerm = (i / 4) + 1L;
            assertEquals(expectedTerm, sender.sentRequests.get(i).request().getTerm());
            assertEquals(2, sender.sentRequests.get(i).request().getCandidateId());
        }
    }

    /**
     * §1.6 — Crash-and-rejoin: a node restored from persisted state must come
     * up as FOLLOWER and accept the first valid heartbeat from the current
     * leader without triggering a spurious election. This test simulates a node
     * that crashed at term=7 and is restarted: an early heartbeat arrives
     * before the election timeout fires, and no election must start.
     */
    @Test
    void restartFromPersistedStateWithEarlyHeartbeatShouldNotTriggerElection() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RecordingElectionListener listener = new RecordingElectionListener();

        // Restored RaftNode: term=7, no vote (e.g. previous term ended cleanly).
        RaftNode restoredNode = new RaftNode(2, RaftPersistence.NO_OP, 7L, null);
        RaftElectionManager manager = new RaftElectionManager(
                2,
                setOf(1, 2, 3),
                150L,
                300L,
                50L,
                restoredNode,
                new FakeLogMetadata(0L, 0L),
                sender,
                fakeClock,
                listener
        );

        manager.start(); // arms a follower election timeout
        FakeScheduledTask initialTimeout = fakeClock.lastOneShotTask;

        // Heartbeat from the legitimate leader for the persisted term arrives early.
        manager.onValidLeaderActivityObserved(7L, 1);

        assertEquals(RaftRole.FOLLOWER, restoredNode.getRole());
        assertEquals(7L, restoredNode.getCurrentTerm());
        assertEquals(1, restoredNode.getLeaderId());

        // No election started: no RequestVote was sent, the manager has no
        // current election term, and the originally-armed timeout was reset.
        assertTrue(sender.sentRequests.isEmpty());
        assertNull(manager.getCurrentElectionTerm());
        assertEquals(0, listener.leaderElectedCount);
        assertTrue(initialTimeout.cancelled);
    }

    /**
     * §1.6 — Crash-and-rejoin: a node restored from persisted state with no
     * heartbeat arriving must eventually start a fresh election with
     * term = persistedTerm + 1, preserving Raft's monotonic term invariant.
     */
    @Test
    void restartFromPersistedStateWithoutHeartbeatShouldStartElectionAtPersistedTermPlusOne() {
        FakeClock fakeClock = new FakeClock();
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();

        RaftNode restoredNode = new RaftNode(2, RaftPersistence.NO_OP, 7L, null);
        RaftElectionManager manager = new RaftElectionManager(
                2,
                setOf(1, 2, 3),
                150L,
                300L,
                50L,
                restoredNode,
                new FakeLogMetadata(0L, 0L),
                sender,
                fakeClock,
                null
        );

        manager.start();
        fakeClock.lastOneShotTask.fire(); // election timeout, no heartbeat ever arrived

        assertEquals(RaftRole.CANDIDATE, restoredNode.getRole());
        assertEquals(8L, restoredNode.getCurrentTerm());
        assertEquals(Integer.valueOf(2), restoredNode.getVotedFor());
        assertEquals(Long.valueOf(8L), manager.getCurrentElectionTerm());

        assertEquals(2, sender.sentRequests.size());
        for (SentVoteRequest sent : sender.sentRequests) {
            assertEquals(8L, sent.request().getTerm());
            assertEquals(2, sent.request().getCandidateId());
        }
    }

    private static RaftElectionManager newManager(
            int localNodeId,
            Set<Integer> votingSet,
            FakeClock fakeClock,
            RecordingVoteRequestSender sender
    ) {
        return newManager(localNodeId, votingSet, fakeClock, sender, null);
    }

    private static RaftElectionManager newManager(
            int localNodeId,
            Set<Integer> votingSet,
            FakeClock fakeClock,
            RecordingVoteRequestSender sender,
            RecordingElectionListener listener
    ) {
        return new RaftElectionManager(
                localNodeId,
                votingSet,
                150L,
                300L,
                50L,
                new RaftNode(localNodeId),
                new FakeLogMetadata(0L, 0L),
                sender,
                fakeClock,
                listener
        );
    }

    private static RaftNode managerNode(RaftElectionManager manager) {
        try {
            java.lang.reflect.Field field = RaftElectionManager.class.getDeclaredField("raftNode");
            field.setAccessible(true);
            return (RaftNode) field.get(manager);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to access raftNode for test", e);
        }
    }

    private static Set<Integer> setOf(Integer... values) {
        LinkedHashSet<Integer> set = new LinkedHashSet<Integer>();
        for (Integer value : values) {
            set.add(value);
        }
        return set;
    }

    private record FakeLogMetadata(long lastLogIndex, long lastLogTerm) implements RaftLogMetadata {
    }

    private record SentVoteRequest(int peerId, RequestVoteRequestMessage request) {
    }

    private static final class RecordingVoteRequestSender implements RaftVoteRequestSender {
        private final List<SentVoteRequest> sentRequests = new ArrayList<SentVoteRequest>();

        @Override
        public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
            sentRequests.add(new SentVoteRequest(peerId, request));
        }

        private Set<Integer> destinationPeerIds() {
            LinkedHashSet<Integer> ids = new LinkedHashSet<Integer>();
            for (SentVoteRequest sent : sentRequests) {
                ids.add(sent.peerId());
            }
            return ids;
        }
    }

    private static final class RecordingElectionListener implements RaftElectionListener {
        private int leaderElectedCount;
        private int steppedDownCount;
        private int lastLeaderId;
        private long lastLeaderTerm;
        private long lastSteppedDownTerm;
        private int lastKnownLeaderId;
        private int heartbeatRoundDueCount;
        private long lastHeartbeatTerm;
        private int leaderObservedCount;
        private int lastObservedLeaderId;
        private long lastObservedLeaderTerm;

        @Override
        public void onLeaderElected(int leaderId, long term) {
            leaderElectedCount++;
            lastLeaderId = leaderId;
            lastLeaderTerm = term;
        }

        @Override
        public void onSteppedDown(long newTerm, int knownLeaderId) {
            steppedDownCount++;
            lastSteppedDownTerm = newTerm;
            lastKnownLeaderId = knownLeaderId;
        }

        @Override
        public void onLeaderObserved(int leaderId, long term) {
            leaderObservedCount++;
            lastObservedLeaderId = leaderId;
            lastObservedLeaderTerm = term;
        }

        @Override
        public void onHeartbeatRoundDue(long term) {
            heartbeatRoundDueCount++;
            lastHeartbeatTerm = term;
        }
    }

    private static final class FakeClock implements RaftClock {
        private int oneShotScheduleCount;
        private int fixedRateScheduleCount;
        private long lastOneShotDelayMs;
        private long lastFixedRateInitialDelayMs;
        private long lastFixedRatePeriodMs;
        private FakeScheduledTask lastOneShotTask;
        private FakeScheduledTask lastFixedRateTask;

        @Override
        public RaftScheduledTask scheduleOnce(long delayMs, Runnable task) {
            oneShotScheduleCount++;
            if (lastOneShotTask != null) {
                lastOneShotTask.cancel();
            }
            lastOneShotDelayMs = delayMs;
            lastOneShotTask = new FakeScheduledTask(task);
            return lastOneShotTask;
        }

        @Override
        public RaftScheduledTask scheduleAtFixedRate(long initialDelayMs, long periodMs, Runnable task) {
            fixedRateScheduleCount++;
            if (lastFixedRateTask != null) {
                lastFixedRateTask.cancel();
            }
            lastFixedRateInitialDelayMs = initialDelayMs;
            lastFixedRatePeriodMs = periodMs;
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

        public void fire() {
            if (!cancelled) {
                task.run();
            }
        }
    }
}