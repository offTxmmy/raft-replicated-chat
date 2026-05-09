package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.messages.raft.RequestVoteResponseMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * §1.1 — Persistence hook for currentTerm and votedFor.
 *
 * <p>Verifies that every mutation of (currentTerm, votedFor) inside RaftNode is
 * pushed to the {@link RaftPersistence} hook synchronously and exactly once,
 * with the new values, before the mutation becomes observable. Also verifies
 * crash-and-rejoin scenarios: a node restored from persisted state must respect
 * the safety rules (no double vote in same term, no stale-term acceptance).
 */
class RaftPersistenceTest {

    @Test
    void startElectionShouldPersistIncrementedTermAndSelfVote() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(7, p);

        node.startElection();

        assertEquals(1, p.writes.size());
        PersistedSnapshot last = p.writes.get(0);
        assertEquals(1L, last.term);
        assertEquals(Integer.valueOf(7), last.votedFor);
    }

    @Test
    void becomeFollowerWithHigherTermShouldPersistResetVote() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(9, p);
        node.startElection(); // write #1: (1, 9)

        node.becomeFollower(5L, 3);

        assertEquals(2, p.writes.size());
        PersistedSnapshot last = p.writes.get(1);
        assertEquals(5L, last.term);
        assertNull(last.votedFor);
    }

    @Test
    void becomeFollowerWithSameTermShouldNotPersist() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(9, p);
        node.startElection(); // write #1

        node.becomeFollower(1L, 4);

        // Same-term step-down keeps votedFor: no new mutation, no persist.
        assertEquals(1, p.writes.size());
    }

    @Test
    void stepDownIfHigherTermShouldPersistNewTermAndClearedVote() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(2, p);
        node.startElection(); // (1, 2)

        node.stepDownIfHigherTerm(8L);

        assertEquals(2, p.writes.size());
        PersistedSnapshot last = p.writes.get(1);
        assertEquals(8L, last.term);
        assertNull(last.votedFor);
    }

    @Test
    void stepDownIfNotHigherShouldNotPersist() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(2, p);
        node.startElection();

        node.stepDownIfHigherTerm(1L); // not higher

        assertEquals(1, p.writes.size());
    }

    @Test
    void grantingVoteShouldPersistBeforeReturningResponse() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(1, p);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(2L, 5, 10L, 4L);
        FakeLog log = new FakeLog(8L, 4L);

        RequestVoteResponseMessage response = node.handleRequestVote(request, log);

        assertTrue(response.isVoteGranted());
        // Two persistence calls: one for the term advance via becomeFollower(term, NO_LEADER),
        // one for the granted vote. Both must reflect the new values and the second
        // one must be the (term=2, votedFor=5) state.
        assertFalse(p.writes.isEmpty());
        PersistedSnapshot last = p.writes.get(p.writes.size() - 1);
        assertEquals(2L, last.term);
        assertEquals(Integer.valueOf(5), last.votedFor);
    }

    @Test
    void duplicateGrantedVoteForSameCandidateShouldPersistOnlyOnce() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(1, p);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(2L, 5, 10L, 4L);
        FakeLog log = new FakeLog(8L, 4L);

        node.handleRequestVote(request, log);
        int writesAfterFirst = p.writes.size();
        node.handleRequestVote(request, log); // duplicate broadcast

        // The second handle must not write again: votedFor and currentTerm unchanged.
        assertEquals(writesAfterFirst, p.writes.size());
    }

    @Test
    void rejectingVoteShouldNotPersist() {
        InMemoryPersistence p = new InMemoryPersistence();
        RaftNode node = new RaftNode(1, p);
        node.startElection(); // (1, 1)

        // Different candidate, same term -> rejected by canVoteFor.
        RequestVoteRequestMessage request = new RequestVoteRequestMessage(1L, 5, 10L, 4L);
        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLog(0L, 0L));

        assertFalse(response.isVoteGranted());
        assertEquals(1, p.writes.size()); // only the startElection write
    }

    // --- Crash-and-rejoin scenarios -----------------------------------------

    @Test
    void afterRestartFromPersistedVoteSecondCandidateInSameTermMustBeRejected() {
        // Phase 1: node A votes for candidate 5 in term 2 and persists.
        InMemoryPersistence disk = new InMemoryPersistence();
        RaftNode original = new RaftNode(1, disk);
        RequestVoteRequestMessage firstReq = new RequestVoteRequestMessage(2L, 5, 10L, 4L);
        original.handleRequestVote(firstReq, new FakeLog(8L, 4L));
        PersistedSnapshot persisted = disk.writes.get(disk.writes.size() - 1);
        assertEquals(2L, persisted.term);
        assertEquals(Integer.valueOf(5), persisted.votedFor);

        // Phase 2: simulate crash + restart. Build a fresh RaftNode from persisted state.
        RaftNode restored = new RaftNode(1, disk, persisted.term, persisted.votedFor);

        // Phase 3: candidate 7 sends RequestVote for the SAME term -> must be rejected.
        RequestVoteRequestMessage secondReq = new RequestVoteRequestMessage(2L, 7, 10L, 4L);
        RequestVoteResponseMessage response = restored.handleRequestVote(secondReq, new FakeLog(8L, 4L));

        assertFalse(response.isVoteGranted());
        assertEquals(2L, restored.getCurrentTerm());
        assertEquals(Integer.valueOf(5), restored.getVotedFor());
    }

    @Test
    void afterRestartFromPersistedHigherTermStaleRequestVoteMustBeRejected() {
        // Phase 1: node observes a higher term and persists term=7, votedFor=null.
        InMemoryPersistence disk = new InMemoryPersistence();
        RaftNode original = new RaftNode(1, disk);
        original.stepDownIfHigherTerm(7L);
        PersistedSnapshot persisted = disk.writes.get(disk.writes.size() - 1);
        assertEquals(7L, persisted.term);
        assertNull(persisted.votedFor);

        // Phase 2: crash + restart from persisted state.
        RaftNode restored = new RaftNode(1, disk, persisted.term, persisted.votedFor);

        // Phase 3: a candidate from the OLD term 5 must be rejected.
        RequestVoteRequestMessage staleReq = new RequestVoteRequestMessage(5L, 9, 100L, 5L);
        RequestVoteResponseMessage response = restored.handleRequestVote(staleReq, new FakeLog(0L, 0L));

        assertFalse(response.isVoteGranted());
        assertEquals(7L, restored.getCurrentTerm());
        assertNull(restored.getVotedFor());
    }

    // --- Test doubles -------------------------------------------------------

    private record PersistedSnapshot(long term, Integer votedFor) {}

    private static final class InMemoryPersistence implements RaftPersistence {
        private final List<PersistedSnapshot> writes = new ArrayList<>();

        @Override
        public void persistTermAndVote(long currentTerm, Integer votedFor) {
            writes.add(new PersistedSnapshot(currentTerm, votedFor));
        }
    }

    private record FakeLog(long lastLogIndex, long lastLogTerm) implements RaftLogMetadata {}
}
