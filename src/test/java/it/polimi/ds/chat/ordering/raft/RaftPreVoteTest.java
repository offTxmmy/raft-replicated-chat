package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic election tests: timers and elapsed monotonic time are driven explicitly. */
class RaftPreVoteTest {
    @Test
    void isolatedVoterDoesNotInflateDurableTermAcrossOneHundredTimeouts(@TempDir Path dir) {
        FileRaftPersistence storage = new FileRaftPersistence(dir);
        storage.persistTermAndVote(7, 1);
        Fixture f = new Fixture(new RaftNode(0, storage, 7, 1), new RaftLog(), Set.of(0, 1, 2));
        for (int i = 0; i < 100; i++) {
            f.clock.expire();
            assertEquals(7, f.node.getCurrentTerm());
            assertEquals(1, f.node.getVotedFor());
            assertEquals(RaftRole.FOLLOWER, f.node.getRole());
            assertNull(f.manager.getCurrentElectionTerm());
        }
        assertEquals(new RaftPersistence.PersistedState(7, 1), storage.loadTermAndVote());
        storage.close();
        assertEquals(200, f.sender.preVotes.size());
        assertTrue(f.sender.votes.isEmpty());
    }

    @Test
    void majorityStartsExactlyOneRealElectionAndDuplicateRepliesCannotRestartIt() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage round = f.sender.lastPreVote();
        assertEquals(0, f.node.getCurrentTerm());
        f.grant(round, 1);
        f.grant(round, 1);
        f.grant(round, 2);
        assertEquals(1, f.node.getCurrentTerm());
        assertEquals(RaftRole.CANDIDATE, f.node.getRole());
        assertEquals(2, f.sender.votes.size());
        assertEquals(Set.of(0), f.manager.getGrantedVotersSnapshot());
        f.manager.onRequestVoteResponse(new RequestVoteResponseMessage(1, true, 1));
        assertTrue(f.node.isLeader());
        assertEquals(1, f.leaderEvents);
    }

    @Test
    void delayedReplyFromPreviousRoundAtSameProspectiveTermCannotWinNewRound() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage old = f.sender.lastPreVote();
        f.clock.expire();
        PreVoteRequestMessage current = f.sender.lastPreVote();
        assertEquals(old.getTerm(), current.getTerm());
        assertNotEquals(old.getRoundId(), current.getRoundId());
        f.grant(old, 1);
        assertEquals(0, f.node.getCurrentTerm());
        f.grant(current, 1);
        assertEquals(1, f.node.getCurrentTerm());
    }

    @Test
    void heartbeatInvalidatesPreVotesAndAlreadyDequeuedTimeout() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage old = f.sender.lastPreVote();
        Task staleTimeout = f.clock.timeout;
        f.manager.onValidLeaderActivityObserved(0, 1);
        Task freshTimeout = f.clock.timeout;
        staleTimeout.fireEvenIfCancelled();
        f.grant(old, 1);
        assertEquals(0, f.node.getCurrentTerm());
        assertEquals(1, f.node.getLeaderId());
        assertSame(freshTimeout, f.clock.timeout);
        assertTrue(f.sender.votes.isEmpty());
    }

    @Test
    void higherNormalTermClearsBothRoundsAndOldTimeouts() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage old = f.sender.lastPreVote();
        Task oldTimeout = f.clock.timeout;
        f.manager.onHigherTermObserved(5);
        f.grant(old, 1);
        oldTimeout.fireEvenIfCancelled();
        assertEquals(5, f.node.getCurrentTerm());
        assertNull(f.manager.getCurrentElectionTerm());
        f.clock.expire();
        f.grant(f.sender.lastPreVote(), 1);
        assertEquals(6, f.node.getCurrentTerm());
        f.manager.onRequestVoteResponse(new RequestVoteResponseMessage(9, false, 2));
        assertEquals(RaftRole.FOLLOWER, f.node.getRole());
        assertEquals(9, f.node.getCurrentTerm());
        assertNull(f.manager.getCurrentElectionTerm());
        assertTrue(f.manager.getGrantedVotersSnapshot().isEmpty());
    }

    @Test
    void preliminaryRequestNeverMutatesTermVoteRoleTimerOrPersistence() {
        List<Long> persisted = new ArrayList<>();
        RaftNode node = new RaftNode(0, (term, vote) -> persisted.add(term), 3, 1);
        Fixture f = new Fixture(node, new RaftLog(), Set.of(0, 1, 2));
        Task timer = f.clock.timeout;
        assertTrue(f.manager.onPreVoteRequest(new PreVoteRequestMessage(99, 2, 0, 0, "round")).isVoteGranted());
        assertEquals(3, node.getCurrentTerm());
        assertEquals(1, node.getVotedFor());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertSame(timer, f.clock.timeout);
        assertTrue(persisted.isEmpty());
    }

    @Test
    void recentLeaderGuardExpiresAtBaselineTimeoutWithoutBeingExtendedByPreVote() {
        Fixture f = new Fixture();
        f.manager.onValidLeaderActivityObserved(3, 1);
        PreVoteRequestMessage request = new PreVoteRequestMessage(50, 2, 0, 0, "r");
        f.clock.advance(99);
        assertFalse(f.manager.onPreVoteRequest(request).isVoteGranted());
        f.clock.advance(1);
        assertTrue(f.manager.onPreVoteRequest(request).isVoteGranted());
        assertEquals(3, f.node.getCurrentTerm());
        assertEquals(1, f.node.getLeaderId());
    }

    @Test
    void leaderRejectsPreVoteButStillStepsDownForHigherNormalRequestVote() {
        Fixture f = new Fixture();
        f.elect();
        f.clock.advance(10000);
        assertFalse(f.manager.onPreVoteRequest(new PreVoteRequestMessage(50, 2, 0, 0, "r")).isVoteGranted());
        assertTrue(f.node.isLeader());
        Task oldHeartbeat = f.clock.heartbeat;
        assertTrue(f.manager.onRequestVoteRequest(new RequestVoteRequestMessage(50, 2, 0, 0)).isVoteGranted());
        assertEquals(50, f.node.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, f.node.getRole());
        assertTrue(oldHeartbeat.cancelled);
    }

    @Test
    void staleLogStaleTermAndNonMemberAreRejected() {
        RaftLog log = new RaftLog();
        log.append(4, null);
        Fixture f = new Fixture(new RaftNode(0, RaftPersistence.NO_OP, 4, null), log, Set.of(0, 1, 2));
        assertFalse(f.manager.onPreVoteRequest(new PreVoteRequestMessage(5, 1, 100, 3, "old-term")).isVoteGranted());
        assertFalse(f.manager.onPreVoteRequest(new PreVoteRequestMessage(5, 1, 0, 4, "old-index")).isVoteGranted());
        assertFalse(f.manager.onPreVoteRequest(new PreVoteRequestMessage(4, 1, 1, 4, "stale")).isVoteGranted());
        assertFalse(f.manager.onPreVoteRequest(new PreVoteRequestMessage(5, 99, 1, 4, "outsider")).isVoteGranted());
        assertTrue(f.manager.onPreVoteRequest(new PreVoteRequestMessage(5, 1, 1, 4, "fresh")).isVoteGranted());
        f.clock.expire();
        f.grant(f.sender.lastPreVote(), 99);
        assertEquals(4, f.node.getCurrentTerm());
    }

    @Test
    void higherActualResponderTermIsAdoptedButProspectiveTermIsNeverAdopted() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage request = f.sender.lastPreVote();
        f.manager.onPreVoteResponse(new PreVoteResponseMessage(8, request.getTerm(), request.getRoundId(), false, 1));
        assertEquals(8, f.node.getCurrentTerm());
        f.clock.expire();
        f.grant(f.sender.lastPreVote(), 1);
        assertEquals(9, f.node.getCurrentTerm());
    }

    @Test
    void stopStartInvalidatesOldRoundEvenWhenProspectiveTermIsUnchanged() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage old = f.sender.lastPreVote();
        f.manager.stop();
        f.manager.start();
        f.clock.expire();
        f.grant(old, 1);
        assertEquals(0, f.node.getCurrentTerm());
        f.grant(f.sender.lastPreVote(), 1);
        assertEquals(1, f.node.getCurrentTerm());
    }

    @Test
    void singleVoterElectsImmediatelyWithoutNetworkPreVotes() {
        Fixture f = new Fixture(new RaftNode(0), new RaftLog(), Set.of(0));
        f.clock.expire();
        assertTrue(f.node.isLeader());
        assertEquals(1, f.node.getCurrentTerm());
        assertEquals(1, f.leaderEvents);
        assertTrue(f.sender.preVotes.isEmpty());
        assertTrue(f.sender.votes.isEmpty());
    }

    @Test
    void dequeuedHeartbeatFromPreviousLeadershipCannotRunInNewLeadership() {
        Fixture f = new Fixture();
        f.elect();
        Task oldHeartbeat = f.clock.heartbeat;
        f.manager.onHigherTermObserved(3);
        f.elect();
        oldHeartbeat.fireEvenIfCancelled();
        assertEquals(0, f.heartbeatEvents);
        f.clock.heartbeat.fireEvenIfCancelled();
        assertEquals(1, f.heartbeatEvents);
    }

    @Test
    void oneThrowingHeartbeatCallbackDoesNotSuppressLaterRounds() {
        Fixture f = new Fixture();
        f.elect();
        f.failHeartbeat = true;
        assertDoesNotThrow(() -> f.clock.heartbeat.fireEvenIfCancelled());
        f.clock.heartbeat.fireEvenIfCancelled();
        assertEquals(2, f.heartbeatEvents);
    }

    @Test
    void duplicateAndNonMemberPreVotesCannotFormAFiveVoterMajority() {
        Fixture f = new Fixture(new RaftNode(0), new RaftLog(), Set.of(0, 1, 2, 3, 4));
        f.clock.expire();
        PreVoteRequestMessage request = f.sender.lastPreVote();
        f.grant(request, 1);
        f.grant(request, 1);
        f.grant(request, 99);
        f.grant(request, 0);
        assertEquals(0, f.node.getCurrentTerm());
        f.grant(request, 2);
        assertEquals(1, f.node.getCurrentTerm());
        f.manager.onRequestVoteResponse(new RequestVoteResponseMessage(1, true, 99));
        f.manager.onRequestVoteResponse(new RequestVoteResponseMessage(1, true, 1));
        f.manager.onRequestVoteResponse(new RequestVoteResponseMessage(1, true, 1));
        assertFalse(f.node.isLeader());
        assertEquals(Set.of(0, 1), f.manager.getGrantedVotersSnapshot());
        f.manager.onRequestVoteResponse(new RequestVoteResponseMessage(1, true, 2));
        assertTrue(f.node.isLeader());
    }

    @Test
    void staleProspectiveTermCannotBeAcceptedEvenWithCurrentRoundId() {
        Fixture f = new Fixture();
        f.clock.expire();
        PreVoteRequestMessage request = f.sender.lastPreVote();
        f.manager.onPreVoteResponse(new PreVoteResponseMessage(0, 20, request.getRoundId(), true, 1));
        assertEquals(0, f.node.getCurrentTerm());
        f.grant(request, 1);
        assertEquals(1, f.node.getCurrentTerm());
    }

    @Test
    void sendFailuresRetainAnElectionRetry() {
        Clock clock = new Clock();
        RaftNode node = new RaftNode(0);
        List<PreVoteRequestMessage> attempts = new ArrayList<>();
        RaftPreVoteRequestSender preliminarySender = (peer, request) -> {
            attempts.add(request);
            throw new IllegalStateException("injected preliminary send failure");
        };
        RaftElectionManager manager = new RaftElectionManager(0, Set.of(0, 1, 2), 100, 100, 20,
                node, new RaftLog(), (peer, request) -> {
                    throw new IllegalStateException("injected vote send failure");
                }, preliminarySender, clock, null);
        manager.start();
        assertDoesNotThrow(clock::expire);
        assertDoesNotThrow(clock::expire);
        assertEquals(2, attempts.size());
        assertEquals(0, node.getCurrentTerm());
        PreVoteRequestMessage request = attempts.get(1);
        assertDoesNotThrow(() -> manager.onPreVoteResponse(
                new PreVoteResponseMessage(0, request.getTerm(), request.getRoundId(), true, 1)));
        assertEquals(1, node.getCurrentTerm());
        assertDoesNotThrow(clock::expire);
        assertEquals(1, node.getCurrentTerm());
        assertEquals(3, attempts.size());
    }

    private static class Fixture {
        final RaftNode node;
        final Clock clock = new Clock();
        final Sender sender = new Sender();
        final RaftElectionManager manager;
        int leaderEvents;
        int heartbeatEvents;
        boolean failHeartbeat;

        Fixture() { this(new RaftNode(0), new RaftLog(), Set.of(0, 1, 2)); }

        Fixture(RaftNode node, RaftLog log, Set<Integer> voters) {
            this.node = node;
            manager = new RaftElectionManager(0, voters, 100, 100, 20,
                    node, log, sender, sender, clock, new RaftElectionListener() {
                @Override public void onLeaderElected(int id, long term) { leaderEvents++; }
                @Override public void onHeartbeatRoundDue(long term) {
                    heartbeatEvents++;
                    if (failHeartbeat) {
                        failHeartbeat = false;
                        throw new IllegalStateException("injected heartbeat failure");
                    }
                }
            });
            manager.start();
        }

        void grant(PreVoteRequestMessage request, int voter) {
            manager.onPreVoteResponse(new PreVoteResponseMessage(
                    node.getCurrentTerm(), request.getTerm(), request.getRoundId(), true, voter));
        }

        void elect() {
            clock.expire();
            grant(sender.lastPreVote(), 1);
            manager.onRequestVoteResponse(new RequestVoteResponseMessage(node.getCurrentTerm(), true, 1));
            assertTrue(node.isLeader());
        }
    }

    private static class Sender implements RaftVoteRequestSender, RaftPreVoteRequestSender {
        final List<PreVoteRequestMessage> preVotes = new ArrayList<>();
        final List<RequestVoteRequestMessage> votes = new ArrayList<>();
        @Override public void sendPreVote(int peer, PreVoteRequestMessage request) { preVotes.add(request); }
        @Override public void sendRequestVote(int peer, RequestVoteRequestMessage request) { votes.add(request); }
        PreVoteRequestMessage lastPreVote() { return preVotes.get(preVotes.size() - 1); }
    }

    static class Clock implements RaftClock {
        long nanos;
        Task timeout;
        Task heartbeat;
        @Override public long nanoTime() { return nanos; }
        @Override public RaftScheduledTask scheduleOnce(long delay, Runnable task) { return timeout = new Task(task); }
        @Override public RaftScheduledTask scheduleAtFixedRate(long delay, long period, Runnable task) { return heartbeat = new Task(task); }
        void advance(long millis) { nanos += TimeUnit.MILLISECONDS.toNanos(millis); }
        void expire() { advance(100); assertFalse(timeout.cancelled); timeout.fireEvenIfCancelled(); }
    }

    static class Task implements RaftScheduledTask {
        final Runnable runnable;
        boolean cancelled;
        Task(Runnable runnable) { this.runnable = runnable; }
        @Override public void cancel() { cancelled = true; }
        void fireEvenIfCancelled() { runnable.run(); }
    }
}
