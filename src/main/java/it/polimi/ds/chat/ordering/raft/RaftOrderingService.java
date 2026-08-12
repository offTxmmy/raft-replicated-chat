package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.raft.ForwardClientProposalRequestMessage;
import it.polimi.ds.chat.protocol.raft.ForwardClientProposalResponseMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.ordering.api.OrderingServiceCallback;

import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Raft-based implementation of {@link OrderingService}.
 */
public final class RaftOrderingService implements OrderingService {

    private final BrokerConfig brokerConfig;
    private final RaftConfig raftConfig;
    private final int localNodeId;

    private final CopyOnWriteArrayList<Consumer<ChatDeliverMessage>> deliveryCallbacks =
            new CopyOnWriteArrayList<>();
    private volatile OrderingServiceCallback callback;

    private RaftPersistence persistence;
    private RaftNode raftNode;
    private RaftLog raftLog;
    private RaftCommitManager commitManager;
    private RaftElectionManager electionManager;
    private RaftReplicationManager replicationManager;
    private RaftRpcServer rpcServer;
    private RaftTransport raftTransport;
    private RaftRpcClient tcpClient;
    private DefaultRaftClock raftClock;

    private volatile boolean running;

    private static final long PROPOSE_COMMIT_TIMEOUT_MS = 5_000L;

    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> pendingCommits =
            new ConcurrentHashMap<>();

    // Guards the check-and-register sequence for retry idempotency.
    private final Set<String> committedProposalKeys = ConcurrentHashMap.newKeySet();
    private final Object idempotencyLock = new Object();

    public RaftOrderingService(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        if (brokerConfig.getRaftConfig() == null) {
            throw new IllegalArgumentException("RaftOrderingService requires raftConfig");
        }
        this.raftConfig = brokerConfig.getRaftConfig();
        this.localNodeId = brokerConfig.getBrokerId();
        if (!raftConfig.getVoters().containsKey(localNodeId)) {
            throw new IllegalArgumentException(
                    "Local broker id " + localNodeId + " is not in the static voter set "
                            + raftConfig.getVoters().keySet());
        }
    }

    public void setCallback(OrderingServiceCallback callback) {
        this.callback = callback;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        // 1. Persistence + recovery of (term, vote).
        persistence = new FileRaftPersistence(raftConfig.getStorageDir());
        RaftPersistence.PersistedState persisted = persistence.loadTermAndVote();

        // 2. Core Raft state.
        raftNode = new RaftNode(localNodeId, persistence,
                persisted.currentTerm(), persisted.votedFor());
        raftLog  = new RaftLog(persistence);
        List<RaftLogEntry> persistedEntries = persistence.loadLogEntries();
        raftLog.loadFromPersistence(persistedEntries);

        for (RaftLogEntry entry : persistedEntries) {
            ChatCommand cmd = entry.getCommand();
            if (cmd != null) {
                committedProposalKeys.add(proposalKey(cmd));
            }
        }

        RaftPersistence.CommitProgress persistedProgress = persistence.loadCommitProgress();
        long restoredCommitIndex = Math.min(persistedProgress.commitIndex(), raftLog.lastLogIndex());
        long restoredLastApplied = Math.min(persistedProgress.lastApplied(), restoredCommitIndex);

        // 3. State machine: deliver committed entries as ChatDeliverMessage.
        RaftStateMachineAdapter applyHook = new RaftStateMachineAdapter(this::notifyDelivery);
        commitManager = new RaftCommitManager(raftLog, entry -> {
            applyHook.accept(entry);
            completePendingCommit(entry);
        }, restoredCommitIndex, restoredLastApplied, (commitIndex, lastApplied) ->
                persistence.persistCommitProgress(commitIndex, lastApplied));

        raftLog.setCommitIndexSupplier(commitManager::getCommitIndex);
        raftLog.setTruncationHook(entry -> {
            ChatCommand cmd = entry.getCommand();
            if (cmd != null) {
                committedProposalKeys.remove(proposalKey(cmd));
            }
        });

        // 4. TCP client and outbound Raft transport.
        tcpClient = new RaftRpcClient(localNodeId, raftConfig.getVoters());
        raftTransport = createRaftTransport(tcpClient);

        // 5. Clock for election timeout and heartbeat scheduling.
        raftClock = new DefaultRaftClock();

        // 6. Voter id set, derived from the static voter map.
        Set<Integer> allVoterIds = new HashSet<>(raftConfig.getVoters().keySet());

        // 7. Election observers: forward replication events to the election manager.
        RaftLeaderActivityObserver leaderActivityObserver =
                (term, leaderId) -> electionManager.onValidLeaderActivityObserved(term, leaderId);

        RaftHigherTermObserver higherTermObserver =
                term -> electionManager.onHigherTermObserved(term);

        // 8. Replication manager — also acts as the election listener.
        replicationManager = new RaftReplicationManager(
                localNodeId,
                allVoterIds,
                raftNode,
                raftLog,
                commitManager,
                raftTransport,
                leaderActivityObserver,
                higherTermObserver
        );

        RaftElectionListener electionListener = new RaftElectionListener() {
            @Override
            public void onLeaderElected(int leaderId, long term) {
                replicationManager.onLeaderElected(leaderId, term);
                if (leaderId == localNodeId) {
                    replicationManager.appendCommandAsLeader(null);
                }
                notifyLeaderChanged(leaderId, term);
            }

            @Override
            public void onSteppedDown(long newTerm, int knownLeaderId) {
                replicationManager.onSteppedDown(newTerm, knownLeaderId);
            }

            @Override
            public void onLeaderObserved(int leaderId, long term) {
                replicationManager.onLeaderObserved(leaderId, term);
                notifyLeaderChanged(leaderId, term);
            }

            @Override
            public void onHeartbeatRoundDue(long term) {
                replicationManager.onHeartbeatRoundDue(term);
            }
        };

        // 9. Election manager.
        electionManager = new RaftElectionManager(
                localNodeId,
                allVoterIds,
                raftConfig.getElectionTimeoutMinMs(),
                raftConfig.getElectionTimeoutMaxMs(),
                raftConfig.getHeartbeatIntervalMs(),
                raftNode,
                raftLog,
                raftTransport,
                raftClock,
                electionListener
        );

        // 10. Wire Raft transport response handlers.
        raftTransport.attachHandlers(
                electionManager::onRequestVoteResponse,
                replicationManager::handleAppendEntriesResponse
        );
        if (raftTransport instanceof RaftHybridTransport hybridTransport) {
            hybridTransport.attachRequestHandlers(
                    electionManager::onRequestVoteRequest,
                    replicationManager::handleAppendEntries
            );
        }

        // 11. RPC server: dispatch inbound RPCs.
        rpcServer = new RaftRpcServer(
                raftConfig.getRpcPort(),
                electionManager::onRequestVoteRequest,
                replicationManager::handleAppendEntries,
                this::handleForwardedProposal
        );

        // 12. Start in dependency order.
        try {
            rpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start RaftRpcServer on port "
                    + raftConfig.getRpcPort(), e);
        }
        raftTransport.start();
        replicationManager.start();
        electionManager.start();

        running = true;
        System.out.println("[RaftOrderingService] started, nodeId=" + localNodeId
                + ", voters=" + raftConfig.getVoters().keySet()
                + ", rpcPort=" + raftConfig.getRpcPort()
                + ", transportMode=" + raftConfig.getTransportMode()
                + (raftConfig.getTransportMode() == RaftTransportMode.HYBRID
                    ? ", raftBroadcastPort=" + raftConfig.getRaftBroadcastPort()
                        + ", clusterId=" + raftConfig.getClusterId()
                        + ", udpMaxPayloadBytes=" + raftConfig.getUdpMaxPayloadBytes()
                    : "")
                + ", restoredTerm=" + persisted.currentTerm()
                + ", restoredVote=" + persisted.votedFor()
                + ", restoredLogLastIndex=" + raftLog.lastLogIndex()
                + ", restoredLogLastTerm=" + raftLog.lastLogTerm()
                + ", restoredDedupKeys=" + committedProposalKeys.size());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        pendingCommits.forEach((id, future) -> future.complete(false));
        pendingCommits.clear();
        committedProposalKeys.clear();

        if (electionManager   != null) electionManager.stop();
        if (replicationManager != null) replicationManager.stop();
        if (raftTransport         != null) raftTransport.stop();
        if (rpcServer         != null) rpcServer.stop();
        if (raftClock         != null) raftClock.shutdown();

        System.out.println("[RaftOrderingService] stopped");
    }

    @Override
    public boolean propose(ChatReqMessage request) {
        if (!running) {
            return false;
        }
        if (!raftNode.isLeader()) {
            return forwardProposalToLeader(request);
        }

        return appendAndWaitForCommit(request);
    }

    private boolean appendAndWaitForCommit(ChatReqMessage request) {
        String proposalKey = proposalKey(request);
        CompletableFuture<Boolean> committed;
        boolean owner = false;

        synchronized (idempotencyLock) {
            if (committedProposalKeys.contains(proposalKey)) {
                return true;
            }

            committed = pendingCommits.get(proposalKey);
            if (committed == null) {
                committed = new CompletableFuture<>();
                pendingCommits.put(proposalKey, committed);
                owner = true;
            }
        }

        if (owner) {
            ChatCommand command;
            if (request.hasClientIdentity()) {
                command = new ChatCommand(
                        request.getLocalMsgId(),
                        request.getBrokerId(),
                        request.getUsername(),
                        request.getText(),
                        request.getVectorClock(),
                        request.getClientId(),
                        request.getClientSeq()
                );
            } else {
                command = new ChatCommand(
                        request.getLocalMsgId(),
                        request.getBrokerId(),
                        request.getUsername(),
                        request.getText(),
                        request.getVectorClock()
                );
            }

            RaftLogEntry appended = replicationManager.appendCommandAsLeader(command);
            if (appended == null) {
                synchronized (idempotencyLock) {
                    pendingCommits.remove(proposalKey, committed);
                }
                return false;
            }
        }

        try {
            return committed.get(PROPOSE_COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            System.err.println("[RaftOrderingService] propose timed out waiting for commit: " + proposalKey);
            return false;
        }
    }

    private boolean forwardProposalToLeader(ChatReqMessage request) {
        int leaderId = raftNode.getLeaderId();
        if (leaderId < 0 || leaderId == localNodeId) {
            System.err.println("[RaftOrderingService] cannot forward proposal: known leader = " + leaderId);
            return false;
        }

        ForwardClientProposalResponseMessage response =
                tcpClient.forwardClientProposal(leaderId, request);
        if (response == null || !response.isAccepted()) {
            String reason = response == null ? "no response" : response.getReason();
            System.err.println("[RaftOrderingService] forwarded proposal rejected by leader "
                    + leaderId + ": " + reason);
            return false;
        }
        return true;
    }

    private ForwardClientProposalResponseMessage handleForwardedProposal(
            ForwardClientProposalRequestMessage request) {
        if (!running) {
            return new ForwardClientProposalResponseMessage(false, getLeaderId(), "service not running");
        }
        if (!raftNode.isLeader()) {
            return new ForwardClientProposalResponseMessage(false, getLeaderId(), "receiver is not leader");
        }

        boolean accepted = appendAndWaitForCommit(request.getRequest());
        return new ForwardClientProposalResponseMessage(
                accepted,
                localNodeId,
                accepted ? "committed" : "proposal was not committed");
    }

    @Override
    public void onDeliver(Consumer<ChatDeliverMessage> callback) {
        deliveryCallbacks.add(callback);
    }

    @Override
    public boolean isLeader() {
        return raftNode != null && raftNode.isLeader();
    }

    @Override
    public int getLeaderId() {
        return raftNode == null ? -1 : raftNode.getLeaderId();
    }

    public int getLocalNodeId() {
        return localNodeId;
    }

    RaftTransport createRaftTransport(RaftRpcClient tcpClient) {
        if (raftConfig.getTransportMode() == RaftTransportMode.LOCAL_TCP) {
            return tcpClient;
        }

        RaftUdpBroadcastTransport udpTransport =
                new RaftUdpBroadcastTransport(localNodeId, raftConfig);
        return new RaftHybridTransport(tcpClient, udpTransport);
    }

    private void notifyDelivery(ChatDeliverMessage message) {
        for (Consumer<ChatDeliverMessage> cb : deliveryCallbacks) {
            try {
                cb.accept(message);
            } catch (Exception e) {
                System.err.println("[RaftOrderingService] delivery callback error: "
                        + e.getMessage());
            }
        }
    }

    private void notifyLeaderChanged(int leaderId, long term) {
        OrderingServiceCallback cb = callback;
        if (cb != null) {
            cb.onLeaderChanged(leaderId, term);
        }
    }

    private void completePendingCommit(RaftLogEntry entry) {
        if (entry == null || entry.getCommand() == null) {
            return;
        }

        String proposalKey = proposalKey(entry.getCommand());
        CompletableFuture<Boolean> pending;
        synchronized (idempotencyLock) {
            committedProposalKeys.add(proposalKey);
            pending = pendingCommits.remove(proposalKey);
        }

        if (pending != null) {
            pending.complete(true);
        }
    }

    long getLastLogIndexForTesting() {
        return raftLog == null ? 0L : raftLog.lastLogIndex();
    }

    private static String proposalKey(ChatReqMessage request) {
        if (request.hasClientIdentity()) {
            return "client:" + request.getClientId() + ":" + request.getClientSeq();
        }
        return "local:" + request.getLocalMsgId();
    }

    private static String proposalKey(ChatCommand command) {
        if (command.hasClientIdentity()) {
            return "client:" + command.getClientId() + ":" + command.getClientSeq();
        }
        return "local:" + command.getLocalMsgId();
    }
}
