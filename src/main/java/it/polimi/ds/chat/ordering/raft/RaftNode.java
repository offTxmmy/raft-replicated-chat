package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.messages.raft.RequestVoteResponseMessage;

/**
 * Core Raft node state.
 * This class owns the minimum state needed for leader election:
 * - currentTerm
 * - votedFor
 * - role
 * - known leader
 * It does NOT handle:
 * - timers
 * - RPC networking
 * - log replication
 * - commit logic
 * Thread-safety:
 * all state transitions are synchronized because this object will later be
 * accessed by timer threads, RPC handlers, and response handlers.
 */
public class RaftNode {

    public static final int NO_LEADER = -1;

    private final int nodeId;
    private final RaftPersistence persistence;

    private long currentTerm;
    private Integer votedFor;
    private int leaderId;
    private RaftRole role;

    /**
     * Creates a new Raft node with no persistence (test convenience).
     * Equivalent to {@code new RaftNode(nodeId, RaftPersistence.NO_OP)}.
     *
     * @param nodeId local broker/node id
     */
    public RaftNode(int nodeId) {
        this(nodeId, RaftPersistence.NO_OP);
    }

    /**
     * Creates a new Raft node from a clean state (term 0, no vote, no leader).
     *
     * @param nodeId local broker/node id
     * @param persistence durable storage hook for {@code (currentTerm, votedFor)}
     */
    public RaftNode(int nodeId, RaftPersistence persistence) {
        this(nodeId, persistence, 0L, null);
    }

    /**
     * Creates a Raft node restored from previously persisted state. Used at
     * broker startup after a crash. The role is always {@link RaftRole#FOLLOWER}
     * on restart (Raft does not persist role). The known leader is unknown until
     * a heartbeat is observed.
     *
     * @param nodeId local broker/node id
     * @param persistence durable storage hook
     * @param persistedTerm term loaded from stable storage
     * @param persistedVotedFor vote loaded from stable storage, or {@code null}
     */
    public RaftNode(int nodeId, RaftPersistence persistence, long persistedTerm, Integer persistedVotedFor) {
        this.nodeId = nodeId;
        this.persistence = persistence;
        this.currentTerm = persistedTerm;
        this.votedFor = persistedVotedFor;
        this.leaderId = NO_LEADER;
        this.role = RaftRole.FOLLOWER;
    }

    public int getNodeId() {
        return nodeId;
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized Integer getVotedFor() {
        return votedFor;
    }

    public synchronized int getLeaderId() {
        return leaderId;
    }

    public synchronized RaftRole getRole() {
        return role;
    }

    public synchronized boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    /**
     * Starts a new election.
     * Effects:
     * - increments the current term
     * - becomes CANDIDATE
     * - votes for itself
     * - forgets any known leader
     *
     * @return the new current term
     */
    public synchronized long startElection() {
        currentTerm++;
        role = RaftRole.CANDIDATE;
        votedFor = nodeId;
        leaderId = NO_LEADER;
        persistence.persistTermAndVote(currentTerm, votedFor);
        return currentTerm;
    }


    /**
     * Becomes leader in the current term.
     * This should happen only after winning a majority in the current term.
     *
     * @throws IllegalStateException if the node is not currently a candidate
     */
    public synchronized void becomeLeader() {
        if (role != RaftRole.CANDIDATE) {
            throw new IllegalStateException("Only a candidate can become leader");
        }

        role = RaftRole.LEADER;
        leaderId = nodeId;
    }

    /**
     * Transitions to FOLLOWER if the observed term is not stale.
     * Rules:
     * - if observedTerm < currentTerm: ignore
     * - if observedTerm == currentTerm: step down to FOLLOWER, keep votedFor
     * - if observedTerm > currentTerm: update term, clear votedFor, become FOLLOWER
     * Keeping votedFor on same-term step-down is important:
     * a node must not vote twice in the same term.
     *
     * @param observedTerm observed term from another node/RPC
     * @param knownLeaderId known leader id for that term, or NO_LEADER if unknown
     * @return true if the node accepted the transition, false if the term was stale
     */
    public synchronized boolean becomeFollower(long observedTerm, int knownLeaderId) {
        if (observedTerm < currentTerm) {
            return false;
        }

        boolean termChanged = false;
        if (observedTerm > currentTerm) {
            currentTerm = observedTerm;
            votedFor = null;
            termChanged = true;
        }

        role = RaftRole.FOLLOWER;
        leaderId = knownLeaderId;

        if (termChanged) {
            persistence.persistTermAndVote(currentTerm, votedFor);
        }
        return true;
    }

    /**
     * Called when this node observes a higher term somewhere in the system.
     * The node must immediately step down.
     *
     * @param observedHigherTerm term seen from another node
     * @return true if the node stepped down, false if the observed term was not higher
     */
    public synchronized boolean stepDownIfHigherTerm(long observedHigherTerm) {
        if (observedHigherTerm <= currentTerm){
            return false;
        }

        currentTerm = observedHigherTerm;
        votedFor = null;
        role = RaftRole.FOLLOWER;
        leaderId = NO_LEADER;
        persistence.persistTermAndVote(currentTerm, votedFor);
        return true;
    }

    /**
     * Records a vote in the current term.
     * This method is intentionally simple for now.
     * Later, the full vote-granting logic will also check:
     * - candidate term
     * - whether already voted
     * - log freshness
     *
     * @param candidateId candidate voted for
     * @throws IllegalStateException if a different vote already exists in this term
     */
    public synchronized void recordVoteFor(int candidateId) {
        if (votedFor != null && votedFor != candidateId) {
            throw new IllegalStateException("Node has already voted for " + votedFor + " in term " + currentTerm);
        }

        votedFor = candidateId;
    }

    /**
     * Clears leader knowledge without changing term or vote.
     * Useful when the leader is suspected dead but no higher term has been observed yet.
     */
    public synchronized void clearKnownLeader() {
        leaderId = NO_LEADER;
    }

    /**
     * Handles an incoming RequestVote RPC according to Raft voting rules.
     * Decision flow:
     * 1. Reject stale terms.
     * 2. If the candidate term is newer, update local term and step down.
     * 3. Reject if this node has already voted for another candidate in this term.
     * 4. Reject if candidate log is not up-to-date.
     * 5. Otherwise, grant vote.
     *
     * @param request incoming vote request
     * @param logMetadata read-only access to local log metadata
     * @return vote response containing the local current term and the decision
     */
    public synchronized RequestVoteResponseMessage handleRequestVote(RequestVoteRequestMessage request, RaftLogMetadata logMetadata) {
        if (request.getTerm() < currentTerm) {
            return new RequestVoteResponseMessage(currentTerm, false, nodeId);
        }

        if (request.getTerm() > currentTerm) {
            // Newer term discovered: this node must step down and forget previous vote
            becomeFollower(request.getTerm(), NO_LEADER);
        }

        if (!canVoteFor(request.getCandidateId())) {
            return new RequestVoteResponseMessage(currentTerm, false, nodeId);
        }

        if (!isCandidateLogUpToDate(request, logMetadata)) {
            return new RequestVoteResponseMessage(currentTerm, false, nodeId);
        }

        // Grant vote
        boolean voteChanged = votedFor == null || votedFor != request.getCandidateId();
        votedFor = request.getCandidateId();
        role = RaftRole.FOLLOWER;
        leaderId = NO_LEADER;

        if (voteChanged) {
            // Persist the vote BEFORE returning the response: a granted vote must
            // be durable before it becomes observable to the candidate.
            persistence.persistTermAndVote(currentTerm, votedFor);
        }

        return new RequestVoteResponseMessage(currentTerm, true, nodeId);
    }

    /**
     * A node can vote if:
     * - it has not voted yet in this term, or
     * - it has already voted for the same candidate.
     */
    private boolean canVoteFor(int candidateId) {
        return votedFor == null || votedFor == candidateId;
    }

    /**
     * Raft log freshness rule:
     * - candidate log is more up-to-date if its lastLogTerm is higher
     * - if terms are equal, candidate log is more up-to-date if its lastLogIndex is >=
     */
    private boolean isCandidateLogUpToDate(RequestVoteRequestMessage request, RaftLogMetadata logMetadata) {
        // PERSON B INTEGRATION:
        // Here we are using the two methods that must come from Person B's log implementation:
        // - lastLogTerm()
        // - lastLogIndex()
        long localLastLogTerm = logMetadata.lastLogTerm();
        long localLastLogIndex = logMetadata.lastLogIndex();

        if (request.getLastLogTerm() > localLastLogTerm) {
            return true;
        }

        if (request.getLastLogTerm() < localLastLogTerm) {
            return false;
        }

        return request.getLastLogIndex() >= localLastLogIndex;
    }
}
