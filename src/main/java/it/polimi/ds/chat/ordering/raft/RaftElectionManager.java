package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Orchestration layer for Raft leader election.
 *
 * <p>This class manages the time-based and event-driven parts of the Raft
 * election process, while delegating authoritative Raft state transitions
 * to {@link RaftNode}.
 *
 * <p>Its responsibilities include:
 * <ul>
 *   <li>managing election timeout lifecycle</li>
 *   <li>managing heartbeat scheduling while leader</li>
 *   <li>starting new elections when timeouts expire</li>
 *   <li>building and sending {@link RequestVoteRequestMessage} messages</li>
 *   <li>tracking the currently active election attempt</li>
 *   <li>collecting and counting {@link RequestVoteResponseMessage} messages</li>
 *   <li>promoting the local node to leader on majority</li>
 *   <li>stepping down on higher-term messages or valid leader activity</li>
 *   <li>notifying external listeners about leadership-related events</li>
 * </ul>
 *
 * <p>This class does not own persistent Raft state, log replication,
 * AppendEntries conflict handling, commit/apply logic, or transport details.
 * Those concerns belong to other components.
 *
 * <p>The local {@link RaftNode} remains the single source of truth for term,
 * role, leader id, and vote state.
 */
public class RaftElectionManager {

    private final int localNodeId;
    private final Set<Integer> allVotingNodeIds;
    private final Set<Integer> peerVotingNodeIds;
    private final int majority;
    private final long electionTimeoutMinMs;
    private final long electionTimeoutMaxMs;
    private final long heartbeatIntervalMs;

    private final RaftNode raftNode;
    private final RaftLogMetadata logMetadata;
    private final RaftVoteRequestSender voteRequestSender;
    private final RaftClock raftClock;
    private final RaftElectionListener electionListener;

    private volatile boolean running;
    private RaftScheduledTask electionTimeoutTask;
    private RaftScheduledTask heartbeatTask;

    /**
     * Election currently being tracked by this manager.
     * Null means no election is currently active/tracked.
     */
    private Long currentElectionTerm;

    private final Set<Integer> grantedVoters = new LinkedHashSet<>();

    public RaftElectionManager(
            int localNodeId,
            Set<Integer> allVotingNodeIds,
            long electionTimeoutMinMs,
            long electionTimeoutMaxMs,
            long heartbeatIntervalMs,
            RaftNode raftNode,
            RaftLogMetadata logMetadata,
            RaftVoteRequestSender voteRequestSender,
            RaftClock raftClock,
            RaftElectionListener electionListener
    ) {
        this.localNodeId = localNodeId;
        this.allVotingNodeIds = immutableVotingSet(allVotingNodeIds);
        validateVotingSet(localNodeId, this.allVotingNodeIds);
        validateTimeouts(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs);

        this.peerVotingNodeIds = buildPeerVotingSet(localNodeId, this.allVotingNodeIds);
        this.majority = (this.allVotingNodeIds.size() / 2) + 1;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.raftNode = Objects.requireNonNull(raftNode, "raftNode");
        this.logMetadata = Objects.requireNonNull(logMetadata, "logMetadata");
        this.voteRequestSender = Objects.requireNonNull(voteRequestSender, "voteRequestSender");
        this.raftClock = Objects.requireNonNull(raftClock, "raftClock");
        this.electionListener = electionListener != null ? electionListener : new RaftElectionListener() {};
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        clearElectionTracking();
        stopHeartbeatSchedule();
        scheduleRandomElectionTimeout();
    }

    public synchronized void stop() {
        running = false;
        cancelElectionTimeout();
        stopHeartbeatSchedule();
        clearElectionTracking();
    }

    public int getLocalNodeId() {
        return localNodeId;
    }

    public Set<Integer> getAllVotingNodeIds() {
        return allVotingNodeIds;
    }

    public Set<Integer> getPeerVotingNodeIds() {
        return peerVotingNodeIds;
    }

    public int getMajority() {
        return majority;
    }

    public long getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public long getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Schedules a new randomized election timeout.
     *
     * <p>If the manager is not running, nothing happens.
     * Any previously scheduled election timeout is canceled before creating
     * the new one, so only one election timeout can be active at a time.
     *
     * <p>When the timeout expires, {@link #onElectionTimeoutFired()} is executed.
     */
    synchronized void scheduleRandomElectionTimeout() {
        if (!running) {
            return;
        }

        cancelElectionTimeout();
        long randomizedDelayMs = randomElectionTimeoutMs();
        electionTimeoutTask = raftClock.scheduleOnce(randomizedDelayMs, this::onElectionTimeoutFired);
    }

    /**
     * Resets the election timeout by cancelling any existing one and scheduling
     * a new randomized timeout.
     *
     * <p>If the manager is not running, nothing happens.
     */
    synchronized void resetElectionTimeout() {
        scheduleRandomElectionTimeout();
    }

    /**
     * Cancels the currently scheduled election timeout, if present.
     *
     * <p>After this method returns, no election timeout is considered active.
     */
    synchronized void cancelElectionTimeout() {
        if (electionTimeoutTask != null) {
            electionTimeoutTask.cancel();
            electionTimeoutTask = null;
        }
    }

    /**
     * Starts the periodic heartbeat schedule.
     *
     * <p>Any previously active heartbeat schedule is canceled before starting
     * a new one, so only one heartbeat schedule can be active at a time.
     *
     * <p>Each heartbeat tick triggers {@link #onHeartbeatTick()}.
     */
    synchronized void startHeartbeatSchedule() {
        stopHeartbeatSchedule();
        heartbeatTask = raftClock.scheduleAtFixedRate(
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                this::onHeartbeatTick
        );
    }

    /**
     * Stops the currently active heartbeat schedule, if present.
     *
     * <p>After this method returns, no heartbeat schedule is considered active.
     */
    synchronized void stopHeartbeatSchedule() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel();
            heartbeatTask = null;
        }
    }

    /**
     * Starts a new election if this manager is running and the local node is not already leader.
     * <p>
     * Election-start behavior:
     * - local node becomes candidate through RaftNode.startElection()
     * - current election term is tracked
     * - self-vote is recorded in grantedVoters
     * - RequestVote is built from local term + local log metadata
     * - request is sent to all static peer voters
     * - a fresh election timeout is armed for retry if needed
     * <p>
     * Special case:
     * - if the static cluster majority is 1, the node wins immediately after self-vote
     */
    void onElectionTimeoutFired() {
        Long leaderElectedTerm = null;

        synchronized (this) {
            if (!running) {
                return;
            }

            if (raftNode.isLeader()) {
                return;
            }

            long newElectionTerm = raftNode.startElection();
            beginElectionTracking(newElectionTerm);

            if (hasMajority(grantedVoters.size())) {
                leaderElectedTerm = becomeLeaderForCurrentElection();
            } else {
                RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                        newElectionTerm,
                        localNodeId,
                        logMetadata.lastLogIndex(),
                        logMetadata.lastLogTerm()
                );

                voteRequestSender.broadcastRequestVote(request, peerVotingNodeIds);

                scheduleRandomElectionTimeout();
            }
        }

        if (leaderElectedTerm != null) {
            electionListener.onLeaderElected(localNodeId, leaderElectedTerm);
        }
    }

    /**
     * Handles an incoming {@link RequestVoteRequestMessage}.
     *
     * <p>The actual vote decision is delegated to {@link RaftNode}. If processing
     * the request causes this node to step down from candidate or leader to follower,
     * the current election tracking is cleared, heartbeat scheduling is stopped,
     * and the step-down is reported to the listener.
     *
     * <p>The election timeout is reset if the vote is granted, or if handling the
     * request causes a step-down due to a higher term.
     *
     * @param request the incoming vote request
     * @return the vote response produced by the underlying {@link RaftNode}
     * @throws NullPointerException if {@code request} is {@code null}
     */
    RequestVoteResponseMessage onRequestVoteRequest(RequestVoteRequestMessage request) {
        Objects.requireNonNull(request, "request");

        RequestVoteResponseMessage response;
        Long steppedDownTerm = null;
        int steppedDownLeaderId = RaftNode.NO_LEADER;

        synchronized (this) {
            long termBefore = raftNode.getCurrentTerm();
            RaftRole roleBefore = raftNode.getRole();

            response = raftNode.handleRequestVote(request, logMetadata);

            long termAfter = raftNode.getCurrentTerm();
            RaftRole roleAfter = raftNode.getRole();

            boolean steppedDownFromActiveRole =
                    (roleBefore == RaftRole.CANDIDATE || roleBefore == RaftRole.LEADER) && roleAfter == RaftRole.FOLLOWER;

            if (steppedDownFromActiveRole) {
                clearElectionTracking();
                stopHeartbeatSchedule();
                steppedDownTerm = termAfter;
                steppedDownLeaderId = raftNode.getLeaderId();
            }

            if (response.isVoteGranted() || (termAfter > termBefore && steppedDownFromActiveRole)) {
                resetElectionTimeout();
            }
        }

        if (steppedDownTerm != null) {
            electionListener.onSteppedDown(steppedDownTerm, steppedDownLeaderId);
        }

        return response;
    }

    /**
     * Handles valid leader activity observed from another node, such as a heartbeat
     * or accepted AppendEntries from the current leader.
     *
     * <p>If the observed term is lower than the local term, nothing happens.
     * If the observed term is higher, this node steps down to follower, clears
     * election tracking, stops heartbeat scheduling, resets the election timeout,
     * and notifies the listener.
     *
     * <p>If the observed term matches the local term and this node is currently a
     * candidate, it steps down to follower and resets its follower timeout.
     * If this node is already a follower in the same term, it simply refreshes
     * the known leader and resets the election timeout.
     *
     * <p>If this node is already leader in the same term, the event is ignored.
     *
     * @param term the term associated with the observed leader activity
     * @param leaderId the id of the leader that generated the activity
     */
    void onValidLeaderActivityObserved(long term, int leaderId) {
        boolean notifySteppedDown = false;
        boolean notifyLeaderObserved = false;

        synchronized (this) {
            if (!running) {
                return;
            }

            long localCurrentTerm = raftNode.getCurrentTerm();
            RaftRole localRole = raftNode.getRole();
            int previousLeaderId = raftNode.getLeaderId();

            if (term < localCurrentTerm) {
                return;
            }

            if (term > localCurrentTerm) {
                raftNode.becomeFollower(term, leaderId);
                clearElectionTracking();
                stopHeartbeatSchedule();
                resetElectionTimeout();
                notifySteppedDown = true;
                notifyLeaderObserved = previousLeaderId != leaderId;
            } else if (localRole == RaftRole.CANDIDATE) {
                raftNode.becomeFollower(term, leaderId);
                clearElectionTracking();
                stopHeartbeatSchedule();
                resetElectionTimeout();
                notifySteppedDown = true;
                notifyLeaderObserved = previousLeaderId != leaderId;
            } else if (localRole == RaftRole.FOLLOWER) {
                raftNode.becomeFollower(term, leaderId);
                resetElectionTimeout();
                notifyLeaderObserved = previousLeaderId != leaderId;
            }

            // A same-term leader observing another same-term leader is not a
            // valid Raft transition; keep local state unchanged.
        }

        if (notifySteppedDown) {
            electionListener.onSteppedDown(term, leaderId);
        }
        if (notifyLeaderObserved) {
            electionListener.onLeaderObserved(leaderId, term);
        }
    }

    /**
     * Handles vote responses for the currently active election.
     * <p>
     * Rules:
     * - ignore if manager is stopped
     * - ignore if local node is no longer candidate
     * - if a higher term is observed, step down immediately
     * - ignore stale responses
     * - ignore responses for old elections
     * - count only granted votes
     * - count each voter at most once
     * - become leader on majority
     */
    void onRequestVoteResponse(RequestVoteResponseMessage response) {
        Objects.requireNonNull(response, "response");

        Long steppedDownTerm = null;
        Long leaderElectedTerm = null;

        synchronized (this) {
            if (!running) {
                return;
            }

            if (raftNode.getRole() != RaftRole.CANDIDATE) {
                return;
            }

            long localCurrentTerm = raftNode.getCurrentTerm();

            if (response.getTerm() > localCurrentTerm) {
                raftNode.stepDownIfHigherTerm(response.getTerm());
                clearElectionTracking();
                stopHeartbeatSchedule();
                resetElectionTimeout();
                steppedDownTerm = response.getTerm();
            } else if (response.getTerm() < localCurrentTerm) {
                return;
            } else if (currentElectionTerm == null) {
                return;
            } else if (response.getTerm() != currentElectionTerm) {
                return;
            } else if (!response.isVoteGranted()) {
                return;
            } else {
                boolean newVote = grantedVoters.add(response.getVoterId());
                if (!newVote) {
                    return;
                }

                if (hasMajority(grantedVoters.size())) {
                    leaderElectedTerm = becomeLeaderForCurrentElection();
                }
            }
        }

        if (steppedDownTerm != null) {
            electionListener.onSteppedDown(steppedDownTerm, RaftNode.NO_LEADER);
        }
        if (leaderElectedTerm != null) {
            electionListener.onLeaderElected(localNodeId, leaderElectedTerm);
        }
    }

    /**
     * Handles a scheduled heartbeat tick.
     *
     * <p>If the manager is not running, or if this node is not currently the
     * leader, nothing happens.
     *
     * <p>When invoked while leader, this method notifies the listener that a new
     * heartbeat round is due for the current term.
     */
    void onHeartbeatTick() {
        Long heartbeatTerm = null;

        synchronized (this) {
            if (!running) {
                return;
            }

            if (raftNode.getRole() != RaftRole.LEADER) {
                return;
            }

            heartbeatTerm = raftNode.getCurrentTerm();
        }

        electionListener.onHeartbeatRoundDue(heartbeatTerm);
    }

    /**
     * Returns the term of the election currently being tracked.
     *
     * <p>If no election is currently being tracked, this method returns {@code null}.
     *
     * @return the current tracked election term, or {@code null} if none exists
     */
    synchronized Long getCurrentElectionTerm() {
        return currentElectionTerm;
    }

    /**
     * Returns an immutable snapshot of the voters that have granted a vote in the
     * currently tracked election.
     *
     * <p>The returned set is a defensive copy and cannot be used to modify the
     * internal election state.
     *
     * @return an immutable snapshot of granted voters
     */
    synchronized Set<Integer> getGrantedVotersSnapshot() {
        return Collections.unmodifiableSet(new LinkedHashSet<>(grantedVoters));
    }

    /**
     * Starts tracking a new election for the given term.
     *
     * <p>The granted-voter set is reset and initialized with the local node id,
     * since a candidate counts its self-vote immediately.
     *
     * @param electionTerm the term of the election to track
     */
    private synchronized void beginElectionTracking(long electionTerm) {
        currentElectionTerm = electionTerm;
        grantedVoters.clear();
        grantedVoters.add(localNodeId);
    }

    /**
     * Clears the state associated with the currently tracked election.
     *
     * <p>After this method returns, no election term is tracked and no granted
     * voters are recorded.
     */
    private synchronized void clearElectionTracking() {
        currentElectionTerm = null;
        grantedVoters.clear();
    }

    /**
     * Promotes the local node to leader for the currently tracked election.
     *
     * <p>This method updates the node role, clears election tracking, cancels the
     * election timeout, starts heartbeat scheduling, and notifies the listener
     * that leadership has been acquired.
     */
    private long becomeLeaderForCurrentElection() {
        raftNode.becomeLeader();
        clearElectionTracking();
        cancelElectionTimeout();
        startHeartbeatSchedule();
        return raftNode.getCurrentTerm();
    }

    /**
     * Returns whether the given vote count is enough to reach majority in the
     * statically configured voting set.
     *
     * @param voteCount the number of granted votes
     * @return {@code true} if the vote count reaches majority, {@code false} otherwise
     */
    private boolean hasMajority(int voteCount) {
        return voteCount >= majority;
    }

    /**
     * Returns a randomized election timeout within the configured timeout range.
     *
     * <p>If the configured minimum and maximum are equal, that exact value is returned.
     *
     * @return a randomized election timeout in milliseconds
     */
    private long randomElectionTimeoutMs() {
        if (electionTimeoutMinMs == electionTimeoutMaxMs) {
            return electionTimeoutMinMs;
        }

        return ThreadLocalRandom.current().nextLong(electionTimeoutMinMs, electionTimeoutMaxMs + 1L);
    }

    /**
     * Returns an immutable copy of the provided static voting set.
     *
     * @param allVotingNodeIds the configured set of voting node ids
     * @return an immutable copy of the voting set
     * @throws NullPointerException if {@code allVotingNodeIds} is {@code null}
     */
    private static Set<Integer> immutableVotingSet(Set<Integer> allVotingNodeIds) {
        Objects.requireNonNull(allVotingNodeIds, "allVotingNodeIds");
        return Collections.unmodifiableSet(new LinkedHashSet<>(allVotingNodeIds));
    }

    /**
     * Validates the configured static voting set.
     *
     * <p>The voting set must not be empty and must contain the local node id.
     *
     * @param localNodeId the local node id
     * @param allVotingNodeIds the configured set of voting node ids
     * @throws IllegalArgumentException if the voting set is empty or does not contain the local node
     */
    private static void validateVotingSet(int localNodeId, Set<Integer> allVotingNodeIds) {
        if (allVotingNodeIds.isEmpty()) {
            throw new IllegalArgumentException("The static voting set must not be empty");
        }

        if (!allVotingNodeIds.contains(localNodeId)) {
            throw new IllegalArgumentException("The local node must belong to the static voting set");
        }
    }

    /**
     * Validates the configured election and heartbeat timeouts.
     *
     * <p>The election timeout minimum must be positive, the maximum must be greater
     * than or equal to the minimum, and the heartbeat interval must be positive.
     *
     * @param electionTimeoutMinMs the minimum election timeout in milliseconds
     * @param electionTimeoutMaxMs the maximum election timeout in milliseconds
     * @param heartbeatIntervalMs the heartbeat interval in milliseconds
     * @throws IllegalArgumentException if any timeout value is invalid
     */
    private static void validateTimeouts(long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs) {
        if (electionTimeoutMinMs <= 0L) {
            throw new IllegalArgumentException("Election timeout min must be > 0");
        }

        if (electionTimeoutMaxMs < electionTimeoutMinMs) {
            throw new IllegalArgumentException("Election timeout max must be >= min");
        }

        if (heartbeatIntervalMs <= 0L) {
            throw new IllegalArgumentException("Heartbeat interval must be > 0");
        }
    }

    /**
     * Builds the immutable set of peer voters by removing the local node id from
     * the full static voting set.
     *
     * @param localNodeId the local node id
     * @param allVotingNodeIds the full configured voting set
     * @return an immutable set containing all voting peers except the local node
     */
    private static Set<Integer> buildPeerVotingSet(int localNodeId, Set<Integer> allVotingNodeIds) {
        LinkedHashSet<Integer> peers = new LinkedHashSet<>(allVotingNodeIds);
        peers.remove(localNodeId);
        return Collections.unmodifiableSet(peers);
    }
}
