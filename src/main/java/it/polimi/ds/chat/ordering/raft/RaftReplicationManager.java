package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

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
    private final RaftHigherTermObserver higherTermObserver;

    private final Map<Integer, RaftPeerReplicationState> replicationState = new LinkedHashMap<>();
    private long initializedLeaderTerm = -1L;

    private volatile boolean running;

    public RaftReplicationManager(
            int localNodeId,
            Set<Integer> allVotingNodeIds,
            RaftNode raftNode,
            RaftLog log,
            RaftCommitManager commitManager,
            RaftAppendEntriesSender appendEntriesSender,
            RaftLeaderActivityObserver leaderActivityObserver,
            RaftHigherTermObserver higherTermObserver
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
        this.higherTermObserver = higherTermObserver;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        replicationState.clear();
        initializedLeaderTerm = -1L;
    }

    public synchronized void stop() {
        running = false;
        replicationState.clear();
        initializedLeaderTerm = -1L;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Appends a new command to the log when this node is leader.
     *
     * <p>Commit advancement is attempted immediately after the append so that
     * a single-node cluster (no peers) can commit without ever receiving an
     * AppendEntries response; in multi-node clusters this is a no-op until
     * peers acknowledge.
     *
     * @param command application command to append; {@code null} produces a no-op entry
     * @return appended log entry, or null if not leader
     */
    public synchronized RaftLogEntry appendCommandAsLeader(ChatCommand command) {
        return appendCommandAsLeader(command, null);
    }

    /** Appends only while the leadership event's term is still authoritative. */
    synchronized RaftLogEntry appendCommandAsLeader(ChatCommand command, Long expectedTerm) {
        if (!running) {
            return null;
        }
        RaftLogEntry entry;

        synchronized (raftNode) {
            if (raftNode.getRole() != RaftRole.LEADER
                    || (expectedTerm != null && expectedTerm != raftNode.getCurrentTerm())) {
                return null;
            }

            long leaderTerm = raftNode.getCurrentTerm();
            entry = log.append(leaderTerm, command);
        }

        advanceCommitFromMatches();
        return entry;
    }

    public AppendEntriesResponseMessage handleAppendEntries(AppendEntriesRequestMessage request) {
        Objects.requireNonNull(request, "request");

        boolean higherTermObserved;

        synchronized (this) {
            if (!running) {
                return rejectedAppendEntriesResponse();
            }

            long localTerm = raftNode.getCurrentTerm();
            if (request.getTerm() < localTerm) {
                return rejectedAppendEntriesResponse();
            }

            higherTermObserved = request.getTerm() > localTerm;
        }

        /*
         * Election lifecycle is owned by RaftElectionManager. In particular, a
         * higher-term AppendEntries must not update RaftNode here before the
         * election layer has had a chance to stop leader timers, clear election
         * tracking, and emit the single step-down callback used by upper layers.
         *
         * Both callbacks deliberately run without this manager's lock: the
         * election listener calls back into onSteppedDown(), so invoking it while
         * holding this lock would create a cross-manager lock cycle.
         */
        if (higherTermObserved && higherTermObserver != null) {
            higherTermObserver.onHigherTermObserved(request.getTerm());
        }

        if (leaderActivityObserver != null) {
            leaderActivityObserver.onValidLeaderActivityObserved(
                    request.getTerm(),
                    request.getLeaderId()
            );
        }

        synchronized (this) {
            if (!running) {
                return rejectedAppendEntriesResponse();
            }

            synchronized (raftNode) {
                /*
                 * A callback may have raced with another election/RPC. Process
                 * this request only if its term is still current and the election
                 * owner has completed the transition to FOLLOWER. Holding the
                 * RaftNode monitor through append/commit prevents a new election
                 * from interleaving with acceptance of this AppendEntries.
                 */
                if (request.getTerm() != raftNode.getCurrentTerm()
                        || raftNode.getRole() != RaftRole.FOLLOWER) {
                    return rejectedAppendEntriesResponse();
                }

                boolean appended = log.appendEntries(
                        request.getPrevLogIndex(),
                        request.getPrevLogTerm(),
                        request.getEntries()
                );

                if (!appended) {
                    long conflictTerm = -1L;
                    long conflictIndex = 0L;
                    long lastIndex = log.lastLogIndex();
                    if (request.getPrevLogIndex() > lastIndex) {
                        conflictIndex = lastIndex + 1L;
                    } else if (request.getPrevLogIndex() > 0L) {
                        conflictTerm = log.getTermAt(request.getPrevLogIndex());
                        conflictIndex = log.firstIndexOfTerm(conflictTerm);
                    }
                    return new AppendEntriesResponseMessage(
                            raftNode.getCurrentTerm(),
                            false,
                            localNodeId,
                            0L,
                            conflictTerm,
                            conflictIndex
                    );
                }

                commitManager.updateCommitIndexFromLeader(request.getLeaderCommit());

                long ackedMatchIndex = request.getPrevLogIndex() + request.getEntries().size();

                return new AppendEntriesResponseMessage(
                        raftNode.getCurrentTerm(),
                        true,
                        localNodeId,
                        ackedMatchIndex,
                        -1L,
                        0L
                );
            }
        }
    }

    public void handleAppendEntriesResponse(
            int followerId,
            AppendEntriesResponseMessage response
    ) {
        Objects.requireNonNull(response, "response");

        Long higherTermObserved = null;

        synchronized (this) {
            if (!running) {
                return;
            }

            long localTerm = raftNode.getCurrentTerm();

            if (response.getTerm() > localTerm) {
                replicationState.clear();
                higherTermObserved = response.getTerm();
            } else {
                if (raftNode.getRole() != RaftRole.LEADER) {
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
                    long nextIndex = state.getNextIndex();
                    long conflictTerm = response.getConflictTerm();
                    long conflictIndex = response.getConflictIndex();

                    if (conflictTerm > 0L) {
                        long lastIndexOfTerm = log.lastIndexOfTerm(conflictTerm);
                        if (lastIndexOfTerm > 0L) {
                            nextIndex = lastIndexOfTerm + 1L;
                        } else if (conflictIndex > 0L) {
                            nextIndex = conflictIndex;
                        } else {
                            nextIndex = Math.max(1L, nextIndex - 1L);
                        }
                    } else if (conflictIndex > 0L) {
                        nextIndex = conflictIndex;
                    } else {
                        nextIndex = Math.max(1L, nextIndex - 1L);
                    }

                    // A delayed failure cannot invalidate an index already
                    // confirmed by a newer successful response.
                    state.setNextIndex(Math.max(
                            nextIndex,
                            state.getMatchIndex() + 1L
                    ));
                    return;
                }

                state.updateMatchIndex(response.getMatchIndex());
                // Success responses may be reordered as well: nextIndex must
                // never move backwards after confirmed replication progress.
                state.setNextIndex(Math.max(
                        state.getNextIndex(),
                        state.getMatchIndex() + 1L
                ));

                advanceCommitFromMatches();
            }
        }

        if (higherTermObserved != null && higherTermObserver != null) {
            higherTermObserver.onHigherTermObserved(higherTermObserved);
        }
    }

    @Override
    public void onLeaderElected(int leaderId, long term) {
        initializeLeaderState(leaderId, term);
    }

    /**
     * Listener delivery can race a later transition because election callbacks
     * run outside the election monitor. Initialize each current leadership once.
     */
    synchronized boolean initializeLeaderState(int leaderId, long term) {
        synchronized (raftNode) {
            if (!running || leaderId != localNodeId
                    || raftNode.getRole() != RaftRole.LEADER
                    || raftNode.getCurrentTerm() != term
                    || initializedLeaderTerm == term) {
                return false;
            }

            replicationState.clear();
            long nextIndex = log.lastLogIndex() + 1L;
            for (Integer peerId : peerVotingNodeIds) {
                replicationState.put(peerId, new RaftPeerReplicationState(nextIndex));
            }
            initializedLeaderTerm = term;
            return true;
        }
    }

    @Override
    public synchronized void onSteppedDown(long newTerm, int knownLeaderId) {
        synchronized (raftNode) {
            if (!running || raftNode.getCurrentTerm() != newTerm
                    || raftNode.getRole() != RaftRole.FOLLOWER) {
                return;
            }
            replicationState.clear();
            initializedLeaderTerm = -1L;
        }
    }

    @Override
    public synchronized void onHeartbeatRoundDue(long term) {
        List<AppendEntriesSendPlan> sendPlans;
        int followerCount;

        synchronized (raftNode) {
            if (!running || raftNode.getRole() != RaftRole.LEADER) {
                return;
            }

            if (term != raftNode.getCurrentTerm()) {
                return;
            }

            sendPlans = new ArrayList<>();
            for (Map.Entry<Integer, RaftPeerReplicationState> entry : replicationState.entrySet()) {
                sendPlans.add(buildAppendEntriesSendPlan(
                        entry.getKey(),
                        entry.getValue(),
                        term
                ));
            }

            followerCount = replicationState.size();
        }

        sendAppendEntries(sendPlans, followerCount);
    }

    private void sendAppendEntries(List<AppendEntriesSendPlan> sendPlans, int followerCount) {
        Map<EmptyHeartbeatKey, HeartbeatBroadcastPlan> emptyHeartbeats = new LinkedHashMap<>();

        for (AppendEntriesSendPlan sendPlan : sendPlans) {
            AppendEntriesRequestMessage request = sendPlan.request();
            if (!request.getEntries().isEmpty()) {
                appendEntriesSender.sendAppendEntries(sendPlan.peerId(), request);
                continue;
            }

            EmptyHeartbeatKey key = EmptyHeartbeatKey.from(request);
            HeartbeatBroadcastPlan broadcastPlan = emptyHeartbeats.computeIfAbsent(
                    key,
                    ignored -> new HeartbeatBroadcastPlan(request)
            );
            broadcastPlan.peerIds().add(sendPlan.peerId());
        }

        for (HeartbeatBroadcastPlan broadcastPlan : emptyHeartbeats.values()) {
            if (broadcastPlan.peerIds().size() == followerCount) {
                appendEntriesSender.broadcastAppendEntries(
                        broadcastPlan.request(),
                        Collections.unmodifiableSet(broadcastPlan.peerIds())
                );
                continue;
            }

            for (Integer peerId : broadcastPlan.peerIds()) {
                appendEntriesSender.sendAppendEntries(peerId, broadcastPlan.request());
            }
        }
    }

    private AppendEntriesSendPlan buildAppendEntriesSendPlan(
            int peerId,
            RaftPeerReplicationState state,
            long leaderTerm
    ) {
        long nextIndex = state.getNextIndex();
        long prevLogIndex = nextIndex - 1L;
        long prevLogTerm = log.getTermAt(prevLogIndex);
        List<RaftLogEntry> entries = log.getEntriesFrom(nextIndex);

        return new AppendEntriesSendPlan(peerId, new AppendEntriesRequestMessage(
                leaderTerm,
                localNodeId,
                prevLogIndex,
                prevLogTerm,
                entries,
                commitManager.getCommitIndex()
        ));
    }

    private void advanceCommitFromMatches() {
        List<Long> matchIndexes = new ArrayList<>();
        matchIndexes.add(log.lastLogIndex());

        for (RaftPeerReplicationState state : replicationState.values()) {
            matchIndexes.add(state.getMatchIndex());
        }

        commitManager.tryAdvanceCommitIndex(matchIndexes, majority, raftNode.getCurrentTerm());
    }

    private AppendEntriesResponseMessage rejectedAppendEntriesResponse() {
        return new AppendEntriesResponseMessage(
                raftNode.getCurrentTerm(),
                false,
                localNodeId,
                0L,
                -1L,
                0L
        );
    }

    private static Set<Integer> immutableVotingSet(Set<Integer> allVotingNodeIds) {
        Objects.requireNonNull(allVotingNodeIds, "allVotingNodeIds");
        return Collections.unmodifiableSet(new LinkedHashSet<>(allVotingNodeIds));
    }

    private static void validateVotingSet(int localNodeId, Set<Integer> allVotingNodeIds) {
        if (allVotingNodeIds.isEmpty()) {
            throw new IllegalArgumentException("The static voting set must not be empty");
        }

        if (!allVotingNodeIds.contains(localNodeId)) {
            throw new IllegalArgumentException("The local node must belong to the static voting set");
        }
    }

    private static Set<Integer> buildPeerVotingSet(int localNodeId, Set<Integer> allVotingNodeIds) {
        LinkedHashSet<Integer> peers = new LinkedHashSet<>(allVotingNodeIds);
        peers.remove(localNodeId);
        return Collections.unmodifiableSet(peers);
    }

    private record AppendEntriesSendPlan(int peerId, AppendEntriesRequestMessage request) {
    }

    private record EmptyHeartbeatKey(long term,
                                     int leaderId,
                                     long prevLogIndex,
                                     long prevLogTerm,
                                     long leaderCommit) {
        static EmptyHeartbeatKey from(AppendEntriesRequestMessage request) {
            return new EmptyHeartbeatKey(
                    request.getTerm(),
                    request.getLeaderId(),
                    request.getPrevLogIndex(),
                    request.getPrevLogTerm(),
                    request.getLeaderCommit()
            );
        }
    }

    private record HeartbeatBroadcastPlan(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
        HeartbeatBroadcastPlan(AppendEntriesRequestMessage request) {
            this(request, new LinkedHashSet<>());
        }
    }
}
