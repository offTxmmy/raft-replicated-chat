package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.messages.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.messages.raft.ChatCommand;
import it.polimi.ds.chat.messages.raft.RaftLogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Replication counterpart to RaftElectionManager.
 *
 * Responsibilities:
 * - Follower-side handling of AppendEntries requests.
 * - Leader-side replication driver on heartbeat ticks.
 * - Tracking per-follower replication state (nextIndex/matchIndex).
 * - Advancing commit index on majority.
 *
 * This class is transport-agnostic and communicates externally only through
 * RaftAppendEntriesSender and RaftLeaderActivityObserver.
 */
public class RaftReplicationManager implements RaftElectionListener {

    private final int localNodeId;
    private final Set<Integer> allVotingNodeIds;
    private final Set<Integer> peerVotingNodeIds;
    private final int majority;

    private final RaftNode raftNode;
    private final RaftLog log;
    private final RaftCommitManager commitManager;
    private final RaftAppendEntriesSender appendEntriesSender;
    private final RaftLeaderActivityObserver leaderActivityObserver;

    private final Map<Integer, RaftPeerReplicationState> replicationState = new LinkedHashMap<>();

    private volatile boolean running;

    public RaftReplicationManager(
            int localNodeId,
            Set<Integer> allVotingNodeIds,
            RaftNode raftNode,
            RaftLog log,
            RaftCommitManager commitManager,
            RaftAppendEntriesSender appendEntriesSender,
            RaftLeaderActivityObserver leaderActivityObserver
    ) {
        this.localNodeId = localNodeId;
        this.allVotingNodeIds = immutableVotingSet(allVotingNodeIds);
        validateVotingSet(localNodeId, this.allVotingNodeIds);
        this.peerVotingNodeIds = buildPeerVotingSet(localNodeId, this.allVotingNodeIds);
        this.majority = (this.allVotingNodeIds.size() / 2) + 1;
        this.raftNode = Objects.requireNonNull(raftNode, "raftNode");
        this.log = Objects.requireNonNull(log, "log");
        this.commitManager = Objects.requireNonNull(commitManager, "commitManager");
        this.appendEntriesSender = Objects.requireNonNull(appendEntriesSender, "appendEntriesSender");
        this.leaderActivityObserver = leaderActivityObserver;
    }

    /**
     * Starts the replication manager and resets leader-side state.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        replicationState.clear();
    }

    /**
     * Stops the replication manager and clears leader-side replication state.
     */
    public synchronized void stop() {
        running = false;
        replicationState.clear();
    }

    /**
     * Returns whether this replication manager is active.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Appends a new command to the log when this node is leader.
     *
     * <p>If the local node is not leader, the call is ignored and null is returned.
     * The leader is expected to replicate new entries on the next heartbeat round.
     *
     * @param command application command to append
     * @return appended log entry, or null if not leader
     */
    public synchronized RaftLogEntry appendCommandAsLeader(ChatCommand command) {
        if (raftNode.getRole() != RaftRole.LEADER) {
            return null;
        }
        return log.append(raftNode.getCurrentTerm(), command);
    }

    /**
     * Handles an incoming AppendEntries request on the follower side.
     *
     * <p>Flow:
     * - reject stale terms
     * - step down if term is newer or local role is not FOLLOWER
     * - validate prevLogIndex/prevLogTerm and append entries
     * - update commit index from leader
     * - notify leader activity observer on success
     *
     * @param request append entries request from leader
     * @return AppendEntries response for the leader
     */
    public synchronized AppendEntriesResponseMessage handleAppendEntries(AppendEntriesRequestMessage request) {
        Objects.requireNonNull(request, "request");

        long localTerm = raftNode.getCurrentTerm();
        if (request.getTerm() < localTerm) {
            return new AppendEntriesResponseMessage(localTerm, false, localNodeId, 0L);
        }

        if (request.getTerm() > localTerm || raftNode.getRole() != RaftRole.FOLLOWER) {
            raftNode.becomeFollower(request.getTerm(), request.getLeaderId());
        }

        boolean appended = log.appendEntries(
                request.getPrevLogIndex(),
                request.getPrevLogTerm(),
                request.getEntries()
        );

        if (!appended) {
            return new AppendEntriesResponseMessage(raftNode.getCurrentTerm(), false, localNodeId, 0L);
        }

        commitManager.updateCommitIndexFromLeader(request.getLeaderCommit());

        if (leaderActivityObserver != null) {
            leaderActivityObserver.onValidLeaderActivityObserved(request.getTerm(), request.getLeaderId());
        }

        return new AppendEntriesResponseMessage(raftNode.getCurrentTerm(), true, localNodeId, log.lastLogIndex());
    }

    /**
     * Handles an AppendEntries response on the leader side.
     *
     * <p>Flow:
     * - ignore if not running or not leader
     * - step down on higher term
     * - ignore stale responses
     * - on failure, decrement nextIndex to backtrack
     * - on success, update matchIndex/nextIndex and try to advance commit
     *
     * @param followerId responding follower id
     * @param response append entries response
     */
    public synchronized void handleAppendEntriesResponse(int followerId, AppendEntriesResponseMessage response) {
        Objects.requireNonNull(response, "response");

        if (!running || raftNode.getRole() != RaftRole.LEADER) {
            return;
        }

        long localTerm = raftNode.getCurrentTerm();
        if (response.getTerm() > localTerm) {
            raftNode.stepDownIfHigherTerm(response.getTerm());
            replicationState.clear();
            return;
        }

        if (response.getTerm() < localTerm) {
            return;
        }

        RaftPeerReplicationState state = replicationState.get(followerId);
        if (state == null) {
            return;
        }

        if (!response.isSuccess()) {
            state.decrementNextIndex();
            return;
        }

        state.updateMatchIndex(response.getMatchIndex());
        state.setNextIndex(response.getMatchIndex() + 1L);

        advanceCommitFromMatches();
    }

    /**
     * Initializes leader-side replication state when this node becomes leader.
     *
     * @param leaderId elected leader id
     * @param term current term
     */
    @Override
    public synchronized void onLeaderElected(int leaderId, long term) {
        if (!running || leaderId != localNodeId) {
            return;
        }

        replicationState.clear();
        long nextIndex = log.lastLogIndex() + 1L;
        for (Integer peerId : peerVotingNodeIds) {
            replicationState.put(peerId, new RaftPeerReplicationState(nextIndex));
        }
    }

    /**
     * Clears replication state when stepping down from leader.
     *
     * @param newTerm new term observed
     * @param knownLeaderId known leader id for the term
     */
    @Override
    public synchronized void onSteppedDown(long newTerm, int knownLeaderId) {
        if (!running) {
            return;
        }
        replicationState.clear();
    }

    /**
     * Sends AppendEntries to all peers on each heartbeat round.
     *
     * <p>Each follower receives entries starting from its nextIndex; if no new
     * entries are available, an empty AppendEntries acts as a heartbeat.
     *
     * @param term current term from the election heartbeat tick
     */
    @Override
    public synchronized void onHeartbeatRoundDue(long term) {
        if (!running || raftNode.getRole() != RaftRole.LEADER) {
            return;
        }

        if (term != raftNode.getCurrentTerm()) {
            return;
        }

        for (Map.Entry<Integer, RaftPeerReplicationState> entry : replicationState.entrySet()) {
            sendAppendEntries(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Builds and sends an AppendEntries request for a single follower.
     */
    private void sendAppendEntries(int peerId, RaftPeerReplicationState state) {
        long nextIndex = state.getNextIndex();
        long prevLogIndex = nextIndex - 1L;
        long prevLogTerm = log.getTermAt(prevLogIndex);
        List<RaftLogEntry> entries = log.getEntriesFrom(nextIndex);

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                raftNode.getCurrentTerm(),
                localNodeId,
                prevLogIndex,
                prevLogTerm,
                entries,
                commitManager.getCommitIndex()
        );

        appendEntriesSender.sendAppendEntries(peerId, request);
    }

    /**
     * Advances commit index based on current matchIndex values.
     */
    private void advanceCommitFromMatches() {
        List<Long> matchIndexes = new ArrayList<>();
        matchIndexes.add(log.lastLogIndex());

        for (RaftPeerReplicationState state : replicationState.values()) {
            matchIndexes.add(state.getMatchIndex());
        }

        commitManager.tryAdvanceCommitIndex(matchIndexes, majority, raftNode.getCurrentTerm());
    }

    /**
     * Returns an immutable copy of the provided static voting set.
     */
    private static Set<Integer> immutableVotingSet(Set<Integer> allVotingNodeIds) {
        Objects.requireNonNull(allVotingNodeIds, "allVotingNodeIds");
        return Collections.unmodifiableSet(new LinkedHashSet<>(allVotingNodeIds));
    }

    /**
     * Validates that the static voting set is non-empty and includes the local node.
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
     * Builds the immutable set of peer voters by removing the local node id.
     */
    private static Set<Integer> buildPeerVotingSet(int localNodeId, Set<Integer> allVotingNodeIds) {
        LinkedHashSet<Integer> peers = new LinkedHashSet<>(allVotingNodeIds);
        peers.remove(localNodeId);
        return Collections.unmodifiableSet(peers);
    }
}
