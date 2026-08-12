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

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        replicationState.clear();
    }

    public synchronized void stop() {
        running = false;
        replicationState.clear();
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
        RaftLogEntry entry;

        synchronized (raftNode) {
            if (raftNode.getRole() != RaftRole.LEADER) {
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

        AppendEntriesResponseMessage response;
        boolean notifyLeaderActivity;

        synchronized (this) {
            long localTerm = raftNode.getCurrentTerm();
            if (request.getTerm() < localTerm) {
                return new AppendEntriesResponseMessage(localTerm, false, localNodeId, 0L, -1L, 0L);
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
                long conflictTerm = -1L;
                long conflictIndex = 0L;
                long lastIndex = log.lastLogIndex();
                if (request.getPrevLogIndex() > lastIndex) {
                    conflictIndex = lastIndex + 1L;
                } else if (request.getPrevLogIndex() > 0L) {
                    conflictTerm = log.getTermAt(request.getPrevLogIndex());
                    conflictIndex = log.firstIndexOfTerm(conflictTerm);
                }
                notifyLeaderActivity = leaderActivityObserver != null;
                response = new AppendEntriesResponseMessage(
                        raftNode.getCurrentTerm(),
                        false,
                        localNodeId,
                        0L,
                        conflictTerm,
                        conflictIndex
                );
            } else {
                commitManager.updateCommitIndexFromLeader(request.getLeaderCommit());

                long ackedMatchIndex = request.getPrevLogIndex() + request.getEntries().size();

                notifyLeaderActivity = leaderActivityObserver != null;
                response = new AppendEntriesResponseMessage(
                        raftNode.getCurrentTerm(),
                        true,
                        localNodeId,
                        ackedMatchIndex,
                        -1L,
                        0L
                );
            }
        }

        if (notifyLeaderActivity) {
            leaderActivityObserver.onValidLeaderActivityObserved(request.getTerm(), request.getLeaderId());
        }

        return response;
    }

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

            state.setNextIndex(nextIndex);
            return;
        }

        state.updateMatchIndex(response.getMatchIndex());
        state.setNextIndex(response.getMatchIndex() + 1L);

        advanceCommitFromMatches();
    }

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

    @Override
    public synchronized void onSteppedDown(long newTerm, int knownLeaderId) {
        if (!running) {
            return;
        }
        replicationState.clear();
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