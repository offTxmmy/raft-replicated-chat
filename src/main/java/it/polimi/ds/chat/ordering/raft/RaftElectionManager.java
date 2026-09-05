package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final Logger LOGGER = Logger.getLogger(RaftElectionManager.class.getName());

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
    private final RaftPreVoteRequestSender preVoteRequestSender;
    private final RaftClock raftClock;
    private final RaftElectionListener electionListener;

    private volatile boolean running;
    private RaftScheduledTask electionTimeoutTask;
    private RaftScheduledTask heartbeatTask;
    private long electionTimeoutGeneration;
    private long heartbeatGeneration;
    private Long lastLeaderContactNanos;
    private PreVoteRequestMessage currentPreVote;
    private final Set<Integer> preVoters = new LinkedHashSet<>();

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
        this(localNodeId, allVotingNodeIds, electionTimeoutMinMs, electionTimeoutMaxMs,
                heartbeatIntervalMs, raftNode, logMetadata, voteRequestSender, null, raftClock, electionListener);
    }

    /**
     * Enables PreVote when a preliminary-round sender is supplied. The original
     * constructor retains basic Raft for standalone components; the ordering
     * service always supplies both senders and enables PreVote.
     */
    public RaftElectionManager(
            int localNodeId, Set<Integer> allVotingNodeIds,
            long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs,
            RaftNode raftNode, RaftLogMetadata logMetadata, RaftVoteRequestSender voteRequestSender,
            RaftPreVoteRequestSender preVoteRequestSender, RaftClock raftClock,
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
        this.preVoteRequestSender = preVoteRequestSender;
        this.raftClock = Objects.requireNonNull(raftClock, "raftClock");
        this.electionListener = electionListener != null ? electionListener : new RaftElectionListener() {};
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        lastLeaderContactNanos = null;
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
        long scheduledGeneration = electionTimeoutGeneration;
        electionTimeoutTask = raftClock.scheduleOnce(
                randomizedDelayMs,
                () -> onElectionTimeoutFired(scheduledGeneration)
        );
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
        // Invalidate even a callback that was already dequeued by the scheduler
        // and can therefore run despite cancellation of its task handle.
        electionTimeoutGeneration++;

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
        long scheduledGeneration = heartbeatGeneration;
        heartbeatTask = raftClock.scheduleAtFixedRate(
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                () -> onHeartbeatTick(scheduledGeneration)
        );
    }

    /**
     * Stops the currently active heartbeat schedule, if present.
     *
     * <p>After this method returns, no heartbeat schedule is considered active.
     */
    synchronized void stopHeartbeatSchedule() {
        heartbeatGeneration++;
        if (heartbeatTask != null) {
            heartbeatTask.cancel();
            heartbeatTask = null;
        }
    }

    /**
     * Starts a preliminary round when PreVote is enabled, otherwise a basic Raft
     * election. The preliminary round keeps durable term/vote unchanged and has
     * its own unique id, since retries may use the same prospective term.
     * A majority starts the real election; only real votes can elect a leader.
     * A single voter satisfies both majorities with its self-vote.
     */
    void onElectionTimeoutFired() {
        long currentGeneration;

        synchronized (this) {
            currentGeneration = electionTimeoutGeneration;
        }

        onElectionTimeoutFired(currentGeneration);
    }

    private void onElectionTimeoutFired(long scheduledGeneration) {
        Long leaderElectedTerm = null;

        synchronized (this) {
            if (!running || scheduledGeneration != electionTimeoutGeneration) {
                return;
            }

            electionTimeoutTask = null;

            if (raftNode.isLeader()) {
                return;
            }

            diagnostic("election timeout term=" + raftNode.getCurrentTerm() + " role=" + raftNode.getRole());
            if (preVoteRequestSender != null) {
                // Keep the durable term/vote unchanged while testing reachability.
                raftNode.becomeFollower(raftNode.getCurrentTerm(), RaftNode.NO_LEADER);
                clearElectionTracking();
                stopHeartbeatSchedule();
                currentPreVote = new PreVoteRequestMessage(
                        Math.addExact(raftNode.getCurrentTerm(), 1L), localNodeId,
                        logMetadata.lastLogIndex(), logMetadata.lastLogTerm(), UUID.randomUUID().toString());
                preVoters.add(localNodeId);
                if (hasMajority(preVoters.size())) {
                    leaderElectedTerm = startRealElection();
                } else {
                    scheduleRandomElectionTimeout();
                    diagnostic("TX PreVote prospectiveTerm=" + currentPreVote.getTerm()
                            + " round=" + currentPreVote.getRoundId());
                    try {
                        preVoteRequestSender.broadcastPreVote(currentPreVote, peerVotingNodeIds);
                    } catch (FileRaftPersistence.RaftPersistenceException failure) {
                        throw failure;
                    } catch (RuntimeException failure) {
                        LOGGER.log(Level.WARNING, "node=" + localNodeId + " PreVote send failed; retry timer armed", failure);
                    }
                }
            } else {
                leaderElectedTerm = startRealElection();
            }
        }

        if (leaderElectedTerm != null) {
            electionListener.onLeaderElected(localNodeId, leaderElectedTerm);
        }
    }

    /** Called under the election monitor, only after a preliminary majority when enabled. */
    private Long startRealElection() {
        clearElectionTracking();
        long term = raftNode.startElection();
        beginElectionTracking(term);
        diagnostic("start election term=" + term + " selfVote=" + localNodeId);
        if (hasMajority(grantedVoters.size())) {
            return becomeLeaderForCurrentElection();
        }
        // Arm before sending: synchronous test transports can deliver the winning
        // response from inside broadcast, and send failures must retain a retry.
        scheduleRandomElectionTimeout();
        RequestVoteRequestMessage request = new RequestVoteRequestMessage(
                term, localNodeId, logMetadata.lastLogIndex(), logMetadata.lastLogTerm());
        diagnostic("TX RequestVote term=" + term);
        try {
            voteRequestSender.broadcastRequestVote(request, peerVotingNodeIds);
        } catch (FileRaftPersistence.RaftPersistenceException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            LOGGER.log(Level.WARNING, "node=" + localNodeId + " RequestVote send failed; retry timer armed", failure);
        }
        return null;
    }

    /**
     * Grants only to a fresh-enough static voter for a future term, when not
     * leader and no valid leader RPC has arrived within the minimum election
     * timeout. Requests do not extend this guard or reset any timer.
     */
    synchronized PreVoteResponseMessage onPreVoteRequest(PreVoteRequestMessage request) {
        Objects.requireNonNull(request, "request");
        boolean recentLeader = lastLeaderContactNanos != null
                && raftClock.nanoTime() - lastLeaderContactNanos
                < TimeUnit.MILLISECONDS.toNanos(electionTimeoutMinMs);
        PreVoteResponseMessage response = raftNode.handlePreVote(request, logMetadata,
                running && allVotingNodeIds.contains(request.getCandidateId())
                        && !raftNode.isLeader() && !recentLeader);
        diagnostic("RX PreVote sender=" + request.getCandidateId() + " prospectiveTerm=" + request.getTerm()
                + " granted=" + response.isVoteGranted() + " localTerm=" + response.getTerm());
        return response;
    }

    void onPreVoteResponse(PreVoteResponseMessage response) {
        Objects.requireNonNull(response, "response");
        Long leaderElectedTerm = null;
        Long steppedDownTerm = null;
        synchronized (this) {
            if (!running || !peerVotingNodeIds.contains(response.getVoterId())) {
                return;
            }
            diagnostic("RX PreVoteResponse sender=" + response.getVoterId()
                    + " prospectiveTerm=" + response.getProspectiveTerm() + " term=" + response.getTerm()
                    + " granted=" + response.isVoteGranted());
            // The response carries the actual durable responder term separately
            // from the echoed prospective term. Never adopt a prospective term.
            if (applyHigherTermIfNeeded(response.getTerm())) {
                steppedDownTerm = response.getTerm();
            } else if (currentPreVote != null && !raftNode.isLeader()
                    && currentPreVote.getRoundId().equals(response.getRoundId())
                    && currentPreVote.getTerm() == response.getProspectiveTerm()
                    && currentPreVote.getTerm() == raftNode.getCurrentTerm() + 1L
                    && response.isVoteGranted() && preVoters.add(response.getVoterId())
                    && hasMajority(preVoters.size())) {
                diagnostic("PreVote majority prospectiveTerm=" + currentPreVote.getTerm());
                leaderElectedTerm = startRealElection();
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
            if (!running || !allVotingNodeIds.contains(request.getCandidateId())) {
                return new RequestVoteResponseMessage(
                        raftNode.getCurrentTerm(),
                        false,
                        localNodeId
                );
            }

            long termBefore = raftNode.getCurrentTerm();
            RaftRole roleBefore = raftNode.getRole();

            response = raftNode.handleRequestVote(request, logMetadata);

            diagnostic("RX RequestVote sender=" + request.getCandidateId() + " term=" + request.getTerm()
                    + " granted=" + response.isVoteGranted());

            long termAfter = raftNode.getCurrentTerm();
            RaftRole roleAfter = raftNode.getRole();

            if (termAfter > termBefore) {
                diagnostic("higher term observed=" + termAfter + " previousTerm=" + termBefore
                        + " source=RequestVote; role=" + roleAfter);
            }

            boolean steppedDownFromActiveRole =
                    (roleBefore == RaftRole.CANDIDATE || roleBefore == RaftRole.LEADER) && roleAfter == RaftRole.FOLLOWER;

            if (termAfter > termBefore || response.isVoteGranted()) {
                clearElectionTracking();
            }

            if (steppedDownFromActiveRole) {
                diagnostic("step down previousRole=" + roleBefore + " term=" + termAfter);
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
     * candidate or leader, it steps down to follower and resets its follower timeout.
     * If this node is already a follower in the same term, it simply refreshes
     * the known leader and resets the election timeout.
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

            lastLeaderContactNanos = raftClock.nanoTime();
            clearElectionTracking();
            if (term != localCurrentTerm || previousLeaderId != leaderId || localRole != RaftRole.FOLLOWER) {
                diagnostic("valid leader activity leader=" + leaderId + " term=" + term
                        + " previousRole=" + localRole);
            }

            if (term > localCurrentTerm) {
                raftNode.becomeFollower(term, leaderId);
                clearElectionTracking();
                stopHeartbeatSchedule();
                resetElectionTimeout();
                notifySteppedDown = true;
                notifyLeaderObserved = previousLeaderId != leaderId;
            } else if (localRole == RaftRole.CANDIDATE
                    || localRole == RaftRole.LEADER) {
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

        }

        if (notifySteppedDown) {
            electionListener.onSteppedDown(term, leaderId);
        }
        if (notifyLeaderObserved) {
            electionListener.onLeaderObserved(leaderId, term);
        }
    }

    /**
     * Handles evidence of a Raft term higher than the local current term.
     *
     * <p>Observing a higher term forces the local node to become follower,
     * clears any election tracking, stops leader heartbeat scheduling,
     * restarts the follower election timeout, and reports the step-down.
     *
     * <p>This method is independent of the node's current role: higher-term
     * information must be incorporated even if the node is no longer the
     * candidate or leader that originally sent the RPC.
     *
     * @param observedTerm higher Raft term observed in an incoming message
     */
    void onHigherTermObserved(long observedTerm) {
        boolean notifySteppedDown;

        synchronized (this) {
            if (!running) {
                return;
            }

            notifySteppedDown = applyHigherTermIfNeeded(observedTerm);
        }

        if (notifySteppedDown) {
            electionListener.onSteppedDown(
                    observedTerm,
                    RaftNode.NO_LEADER
            );
        }
    }

    private boolean applyHigherTermIfNeeded(long observedTerm) {
        long localCurrentTerm = raftNode.getCurrentTerm();

        if (observedTerm <= localCurrentTerm) {
            return false;
        }

        raftNode.stepDownIfHigherTerm(observedTerm);
        diagnostic("higher term observed=" + observedTerm + " previousTerm=" + localCurrentTerm + "; step down");
        clearElectionTracking();
        stopHeartbeatSchedule();
        resetElectionTimeout();

        return true;
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
            if (!running || !peerVotingNodeIds.contains(response.getVoterId())) {
                return;
            }

            long localCurrentTerm = raftNode.getCurrentTerm();

            diagnostic("RX RequestVoteResponse sender=" + response.getVoterId() + " term=" + response.getTerm()
                    + " granted=" + response.isVoteGranted());

            if (response.getTerm() > localCurrentTerm) {
                if (applyHigherTermIfNeeded(response.getTerm())) {
                    steppedDownTerm = response.getTerm();
                }
            } else {
                if (raftNode.getRole() != RaftRole.CANDIDATE) {
                    return;
                }

                if (response.getTerm() < localCurrentTerm) {
                    return;
                }

                if (currentElectionTerm == null) {
                    return;
                }

                if (response.getTerm() != currentElectionTerm) {
                    return;
                }

                if (!response.isVoteGranted()) {
                    return;
                }

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
            electionListener.onSteppedDown(
                    steppedDownTerm,
                    RaftNode.NO_LEADER
            );
        }

        if (leaderElectedTerm != null) {
            electionListener.onLeaderElected(
                    localNodeId,
                    leaderElectedTerm
            );
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
        long generation;
        synchronized (this) {
            generation = heartbeatGeneration;
        }
        onHeartbeatTick(generation);
    }

    private void onHeartbeatTick(long scheduledGeneration) {
        Long heartbeatTerm = null;

        synchronized (this) {
            if (!running || scheduledGeneration != heartbeatGeneration) {
                return;
            }

            if (raftNode.getRole() != RaftRole.LEADER) {
                return;
            }

            heartbeatTerm = raftNode.getCurrentTerm();
        }

        try {
            electionListener.onHeartbeatRoundDue(heartbeatTerm);
        } catch (FileRaftPersistence.RaftPersistenceException failure) {
            LOGGER.log(Level.SEVERE, "node=" + localNodeId + " unrecoverable Raft storage failure", failure);
            throw failure;
        } catch (RuntimeException failure) {
            // ScheduledExecutorService suppresses every later fixed-rate tick
            // when one callback throws. Retain the next round and expose the bug.
            LOGGER.log(Level.WARNING, "node=" + localNodeId + " heartbeat round failed term=" + heartbeatTerm, failure);
        }
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
        currentPreVote = null;
        preVoters.clear();
    }

    /**
     * Promotes the local node to leader for the currently tracked election.
     *
     * <p>This method updates the node role, clears election tracking, cancels the
     * election timeout, starts heartbeat scheduling, and notifies the listener
     * that leadership has been acquired.
     */
    private long becomeLeaderForCurrentElection() {
        diagnostic("RequestVote majority term=" + currentElectionTerm + " voters=" + grantedVoters);
        raftNode.becomeLeader();
        clearElectionTracking();
        cancelElectionTimeout();
        startHeartbeatSchedule();
        diagnostic("leader elected term=" + raftNode.getCurrentTerm());
        return raftNode.getCurrentTerm();
    }

    private void diagnostic(String event) {
        LOGGER.info("[RaftElection node=" + localNodeId + "] " + event);
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
