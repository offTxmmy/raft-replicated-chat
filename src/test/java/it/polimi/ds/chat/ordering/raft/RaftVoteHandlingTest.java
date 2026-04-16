package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.messages.raft.RequestVoteResponseMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaftVoteHandlingTest {

    @Test
    void shouldRejectVoteIfCandidateTermIsStale() {
        RaftNode node = new RaftNode(1);
        node.startElection(); // currentTerm = 1, votedFor = self

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                0L,  // stale term
                2,
                5L,
                3L
        );

        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(5L, 3L));

        assertFalse(response.isVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals(RaftRole.CANDIDATE, node.getRole());
        assertEquals(Integer.valueOf(1), node.getVotedFor());
    }

    @Test
    void shouldGrantVoteIfNewerTermAndCandidateLogIsUpToDate() {
        RaftNode node = new RaftNode(1);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                2L,
                2,
                10L,
                4L
        );

        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(8L, 4L));

        assertTrue(response.isVoteGranted());
        assertEquals(2L, response.getTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(Integer.valueOf(2), node.getVotedFor());
        assertEquals(RaftNode.NO_LEADER, node.getLeaderId());
    }

    @Test
    void shouldRejectVoteIfAlreadyVotedForDifferentCandidateInSameTerm() {
        RaftNode node = new RaftNode(1);
        node.startElection(); // term = 1, votedFor = 1

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                1L,
                2,
                10L,
                5L
        );

        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(10L, 5L));

        assertFalse(response.isVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals(Integer.valueOf(1), node.getVotedFor());
    }

    @Test
    void shouldAllowVoteForSameCandidateAgainInSameTerm() {
        RaftNode node = new RaftNode(1);
        node.recordVoteFor(2);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                0L,
                2,
                3L,
                1L
        );

        // Make node term = 0 and log equal to candidate log.
        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(3L, 1L));

        assertTrue(response.isVoteGranted());
        assertEquals(0L, response.getTerm());
        assertEquals(Integer.valueOf(2), node.getVotedFor());
    }

    @Test
    void shouldRejectVoteIfCandidateLogTermIsOlder() {
        RaftNode node = new RaftNode(1);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                1L,
                2,
                100L,
                2L
        );

        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(5L, 3L));

        assertFalse(response.isVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertNull(node.getVotedFor());
    }

    @Test
    void shouldRejectVoteIfLogTermsEqualButCandidateIndexIsSmaller() {
        RaftNode node = new RaftNode(1);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                1L,
                2,
                4L,
                3L
        );

        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(5L, 3L));

        assertFalse(response.isVoteGranted());
        assertEquals(1L, response.getTerm());
    }

    @Test
    void shouldUpdateTermEvenIfVoteIsRejectedBecauseLogIsStale() {
        RaftNode node = new RaftNode(1);
        node.startElection(); // currentTerm = 1, role = CANDIDATE

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                2L,   // newer term
                2,
                1L,   // stale log
                1L
        );

        RequestVoteResponseMessage response = node.handleRequestVote(request, new FakeLogMetadata(10L, 5L));

        assertFalse(response.isVoteGranted());
        assertEquals(2L, response.getTerm());

        // Even though vote is denied, the node must still adopt the newer term.
        assertEquals(2L, node.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertNull(node.getVotedFor());
    }

    /**
         * Test-only fake implementation.
         * In production, Person B's RaftLog should implement RaftLogMetadata.
         */
        private record FakeLogMetadata(long lastLogIndex, long lastLogTerm) implements RaftLogMetadata {

    }
}