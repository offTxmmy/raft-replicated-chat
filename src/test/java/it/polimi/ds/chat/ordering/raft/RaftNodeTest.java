package it.polimi.ds.chat.ordering.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {

    @Test
    void shouldInitializeAsFollowerInTermZero() {
        RaftNode node = new RaftNode(3);

        assertEquals(3, node.getNodeId());
        assertEquals(0L, node.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertNull(node.getVotedFor());
        assertEquals(RaftNode.NO_LEADER, node.getLeaderId());
        assertFalse(node.isLeader());
    }

    @Test
    void startElectionShouldIncrementTermAndVoteForSelf() {
        RaftNode node = new RaftNode(5);

        long newTerm = node.startElection();

        assertEquals(1L, newTerm);
        assertEquals(1L, node.getCurrentTerm());
        assertEquals(RaftRole.CANDIDATE, node.getRole());
        assertEquals(Integer.valueOf(5), node.getVotedFor());
        assertEquals(RaftNode.NO_LEADER, node.getLeaderId());
    }

    @Test
    void candidateShouldBecomeLeader() {
        RaftNode node = new RaftNode(2);

        node.startElection();
        node.becomeLeader();

        assertEquals(RaftRole.LEADER, node.getRole());
        assertTrue(node.isLeader());
        assertEquals(2, node.getLeaderId());
        assertEquals(1L, node.getCurrentTerm());
    }

    @Test
    void becomeLeaderShouldFailIfNodeIsNotCandidate() {
        RaftNode node = new RaftNode(2);

        assertThrows(IllegalStateException.class, node::becomeLeader);
    }

    @Test
    void higherTermShouldForceStepDownToFollower() {
        RaftNode node = new RaftNode(7);

        node.startElection();
        node.becomeLeader();

        boolean changed = node.stepDownIfHigherTerm(4L);

        assertTrue(changed);
        assertEquals(4L, node.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertNull(node.getVotedFor());
        assertEquals(RaftNode.NO_LEADER, node.getLeaderId());
        assertFalse(node.isLeader());
    }

    @Test
    void staleHigherTermCheckShouldDoNothing() {
        RaftNode node = new RaftNode(1);

        node.startElection(); // term = 1

        boolean changed = node.stepDownIfHigherTerm(1L);

        assertFalse(changed);
        assertEquals(1L, node.getCurrentTerm());
        assertEquals(RaftRole.CANDIDATE, node.getRole());
        assertEquals(Integer.valueOf(1), node.getVotedFor());
    }

    @Test
    void becomeFollowerWithHigherTermShouldResetVote() {
        RaftNode node = new RaftNode(9);

        node.startElection(); // term = 1, votedFor = 9
        boolean accepted = node.becomeFollower(2L, 4);

        assertTrue(accepted);
        assertEquals(2L, node.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertNull(node.getVotedFor());
        assertEquals(4, node.getLeaderId());
    }

    @Test
    void becomeFollowerWithSameTermShouldKeepVote() {
        RaftNode node = new RaftNode(9);

        node.startElection(); // term = 1, votedFor = 9
        boolean accepted = node.becomeFollower(1L, 4);

        assertTrue(accepted);
        assertEquals(1L, node.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, node.getRole());
        assertEquals(Integer.valueOf(9), node.getVotedFor());
        assertEquals(4, node.getLeaderId());
    }

    @Test
    void becomeFollowerWithStaleTermShouldBeRejected() {
        RaftNode node = new RaftNode(9);

        node.startElection(); // term = 1
        boolean accepted = node.becomeFollower(0L, 4);

        assertFalse(accepted);
        assertEquals(1L, node.getCurrentTerm());
        assertEquals(RaftRole.CANDIDATE, node.getRole());
        assertEquals(Integer.valueOf(9), node.getVotedFor());
        assertEquals(RaftNode.NO_LEADER, node.getLeaderId());
    }

    @Test
    void recordVoteForShouldAllowSameCandidateTwice() {
        RaftNode node = new RaftNode(3);

        node.recordVoteFor(8);
        node.recordVoteFor(8);

        assertEquals(Integer.valueOf(8), node.getVotedFor());
    }

    @Test
    void recordVoteForShouldRejectDifferentCandidateInSameTerm() {
        RaftNode node = new RaftNode(3);

        node.recordVoteFor(8);

        assertThrows(IllegalStateException.class, () -> node.recordVoteFor(9));
    }
}