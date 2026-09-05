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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
    // Invalidates manager callbacks that escaped their old run before stop().
    private long lifecycleGeneration;

    private static final long PROPOSE_COMMIT_TIMEOUT_MS = 5_000L;

    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> pendingCommits =
            new ConcurrentHashMap<>();

    // A JOIN boundary is complete only once this replica has applied it, not
    // merely when the leader has reported the entry committed.
    private final ConcurrentHashMap<String, PendingLocalBarrier> pendingLocalBarriers =
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
        long runGeneration = ++lifecycleGeneration;

        // 1. Persistence + recovery of (term, vote).
        persistence = new FileRaftPersistence(raftConfig.getStorageDir());
        RaftPersistence.PersistedState persisted = persistence.loadTermAndVote();

        // 2. Core Raft state.
        raftNode = new RaftNode(localNodeId, persistence,
                persisted.currentTerm(), persisted.votedFor());
        raftLog  = new RaftLog(persistence);
        List<RaftLogEntry> persistedEntries = persistence.loadLogEntries();
        raftLog.loadFromPersistence(persistedEntries);

        RaftPersistence.CommitProgress persistedProgress = persistence.loadCommitProgress();
        long restoredCommitIndex = Math.min(
                persistedProgress.commitIndex(),
                raftLog.lastLogIndex()
        );
        long restoredLastApplied = Math.min(
                persistedProgress.lastApplied(),
                restoredCommitIndex
        );

        // Rebuild every volatile application projection from the prefix that had
        // already crossed the state-machine boundary before the crash. This runs
        // before Raft networking (and, in Broker.start(), before the client
        // listener), so callbacks reconstruct sequence/causal state without
        // exposing persisted chat history to newly connected clients.
        //
        // Reusing applyCommittedEntryOnce is important: a duplicate client
        // command that occupied two committed log positions was applied only
        // once before the crash and must consume only one application sequence
        // again during reconstruction.
        committedProposalKeys.clear();
        RaftStateMachineAdapter applyHook = new RaftStateMachineAdapter(this::notifyDelivery);
        for (RaftLogEntry entry : persistedEntries) {
            if (entry.getIndex() > restoredLastApplied) {
                break;
            }

            applyCommittedEntryOnce(entry, applyHook);
        }

        // 3. Resume normal application. The constructor applies exactly the
        // committed-but-not-applied suffix (restoredLastApplied, restoredCommitIndex].
        commitManager = new RaftCommitManager(
                raftLog,
                entry -> applyCommittedEntryOnce(entry, applyHook),
                restoredCommitIndex,
                restoredLastApplied,
                (commitIndex, lastApplied) ->
                        persistence.persistCommitProgress(commitIndex, lastApplied)
        );

        raftLog.setCommitIndexSupplier(commitManager::getCommitIndex);

        raftLog.setTruncationHook(entry -> {
            ChatCommand cmd = entry.getCommand();
            if (cmd == null) {
                return;
            }

            String key = proposalKey(cmd);
            CompletableFuture<Boolean> pending;

            synchronized (idempotencyLock) {
                pending = pendingCommits.remove(key);
            }

            if (pending != null) {
                pending.complete(false);
            }

            PendingLocalBarrier localBarrier = pendingLocalBarriers.remove(key);
            if (localBarrier != null) {
                localBarrier.completion.complete(false);
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
        // Each replication manager retains observers tied to its own election
        // manager. Dereferencing the mutable service field here would let an RPC
        // from a stopped run mutate a replacement run after stop()+start().
        AtomicReference<RaftElectionManager> runElectionManager = new AtomicReference<>();
        RaftLeaderActivityObserver leaderActivityObserver = (term, leaderId) -> {
            RaftElectionManager manager = runElectionManager.get();
            if (manager != null) {
                manager.onValidLeaderActivityObserved(term, leaderId);
            }
        };

        RaftHigherTermObserver higherTermObserver = term -> {
            RaftElectionManager manager = runElectionManager.get();
            if (manager != null) {
                manager.onHigherTermObserved(term);
            }
        };

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
        RaftReplicationManager runReplicationManager = replicationManager;

        RaftElectionListener electionListener = new RaftElectionListener() {
            @Override
            public void onLeaderElected(int leaderId, long term) {
                synchronized (RaftOrderingService.this) {
                    if (!isActiveRunLocked(runGeneration, runReplicationManager)) {
                        return;
                    }
                    if (!runReplicationManager.initializeLeaderState(leaderId, term)) {
                        return;
                    }
                    runReplicationManager.appendCommandAsLeader(null, term);
                    if (raftNode.isLeader() && raftNode.getCurrentTerm() == term) {
                        notifyLeaderChanged(leaderId, term);
                    }
                }
            }

            @Override
            public void onSteppedDown(long newTerm, int knownLeaderId) {
                synchronized (RaftOrderingService.this) {
                    if (!isActiveRunLocked(runGeneration, runReplicationManager)) {
                        return;
                    }
                    runReplicationManager.onSteppedDown(newTerm, knownLeaderId);
                    failPendingCommitsForStepDown(newTerm);
                }
            }

            @Override
            public void onLeaderObserved(int leaderId, long term) {
                synchronized (RaftOrderingService.this) {
                    if (!isActiveRunLocked(runGeneration, runReplicationManager)) {
                        return;
                    }
                    if (raftNode.getCurrentTerm() != term
                            || raftNode.getRole() != RaftRole.FOLLOWER
                            || raftNode.getLeaderId() != leaderId) {
                        return;
                    }
                    runReplicationManager.onLeaderObserved(leaderId, term);
                    notifyLeaderChanged(leaderId, term);
                }
            }

            @Override
            public void onHeartbeatRoundDue(long term) {
                synchronized (RaftOrderingService.this) {
                    if (!isActiveRunLocked(runGeneration, runReplicationManager)) {
                        return;
                    }
                }
                // The replication manager has its own running/term checks. Do not
                // hold the service lifecycle monitor across peer network I/O.
                runReplicationManager.onHeartbeatRoundDue(term);
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
                raftTransport,
                raftClock,
                electionListener
        );
        runElectionManager.set(electionManager);

        // 10. Wire Raft transport response handlers.
        raftTransport.attachHandlers(
                electionManager::onRequestVoteResponse,
                replicationManager::handleAppendEntriesResponse
        );
        raftTransport.attachPreVoteHandlers(
                electionManager::onPreVoteRequest,
                electionManager::onPreVoteResponse
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
        rpcServer.attachPreVoteHandler(electionManager::onPreVoteRequest);

        // 12. Start in dependency order.
        try {
            rpcServer.start();
            raftTransport.start();
            replicationManager.start();
            electionManager.start();
            running = true;
        } catch (IOException | RuntimeException e) {
            cleanupStartedComponents();
            throw new RuntimeException("Failed to start Raft ordering service on node "
                    + localNodeId + " (rpcPort=" + raftConfig.getRpcPort() + ")", e);
        }
        System.out.println("[RaftOrderingService] started, nodeId=" + localNodeId
                + ", voters=" + raftConfig.getVoters().keySet()
                + ", rpcPort=" + raftConfig.getRpcPort()
                + ", transportMode=" + raftConfig.getTransportMode()
                + ", preVote=true"
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
        lifecycleGeneration++;

        pendingCommits.forEach((id, future) -> future.complete(false));
        pendingCommits.clear();
        pendingLocalBarriers.forEach((id, barrier) -> barrier.completion.complete(false));
        pendingLocalBarriers.clear();
        committedProposalKeys.clear();

        cleanupStartedComponents();

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

    @Override
    public boolean establishDeliveryBoundary(String boundaryId) {
        return establishDeliveryBoundary(boundaryId, () -> { });
    }

    @Override
    public boolean establishDeliveryBoundary(String boundaryId, Runnable onApplied) {
        Objects.requireNonNull(boundaryId, "boundaryId");
        Objects.requireNonNull(onApplied, "onApplied");
        if (boundaryId.isBlank() || !running) {
            return false;
        }

        ChatReqMessage request = ChatReqMessage.deliveryBarrier(
                "join-barrier:" + boundaryId,
                localNodeId
        );
        String key = proposalKey(request);
        PendingLocalBarrier localBarrier = new PendingLocalBarrier(onApplied);
        PendingLocalBarrier existing = pendingLocalBarriers.putIfAbsent(key, localBarrier);
        if (existing != null) {
            localBarrier = existing;
        }

        synchronized (idempotencyLock) {
            if (committedProposalKeys.contains(key)) {
                localBarrier.activate();
            }
        }

        if (!localBarrier.completion.isDone() && !propose(request)) {
            pendingLocalBarriers.remove(key, localBarrier);
            localBarrier.completion.complete(false);
            return false;
        }

        try {
            return localBarrier.completion.get(
                    PROPOSE_COMMIT_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        } finally {
            pendingLocalBarriers.remove(key, localBarrier);
        }
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
            if (request.isDeliveryBarrier()) {
                command = ChatCommand.deliveryBarrier(
                        request.getLocalMsgId(),
                        request.getBrokerId()
                );
            } else if (request.hasClientIdentity()) {
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

            if (raftLogContainsProposalKey(proposalKey)) {
                synchronized (idempotencyLock) {
                    pendingCommits.remove(proposalKey, committed);
                }

                committed.complete(false);
                return false;
            }

            RaftLogEntry appended = replicationManager.appendCommandAsLeader(command);
            if (appended == null) {
                synchronized (idempotencyLock) {
                    pendingCommits.remove(proposalKey, committed);
                }

                committed.complete(false);
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

    private void applyCommittedEntryOnce(
            RaftLogEntry entry,
            RaftStateMachineAdapter applyHook
    ) {
        if (entry == null || entry.getCommand() == null) {
            return;
        }

        String key = proposalKey(entry.getCommand());
        CompletableFuture<Boolean> pending;
        PendingLocalBarrier localBarrier;
        boolean firstApplication;

        synchronized (idempotencyLock) {
            firstApplication = committedProposalKeys.add(key);
            pending = pendingCommits.remove(key);
            localBarrier = entry.getCommand().isDeliveryBarrier()
                    ? pendingLocalBarriers.get(key)
                    : null;
        }

        if (firstApplication) {
            applyHook.accept(entry);
        }

        if (pending != null) {
            pending.complete(true);
        }
        if (localBarrier != null) {
            localBarrier.activate();
        }
    }

    long getLastLogIndexForTesting() {
        return raftLog == null ? 0L : raftLog.lastLogIndex();
    }

    private boolean raftLogContainsProposalKey(String key) {
        for (RaftLogEntry entry : raftLog.getEntriesFrom(1L)) {
            ChatCommand command = entry.getCommand();

            if (command != null && proposalKey(command).equals(key)) {
                return true;
            }
        }

        return false;
    }

    private void failPendingCommitsForStepDown(long term) {
        List<CompletableFuture<Boolean>> pending;

        // A delayed step-down notification must not reject proposals registered
        // by a newer leader. Capture the old pending set atomically with the
        // term/role check; complete futures after releasing the node monitor.
        synchronized (raftNode) {
            if (raftNode.getCurrentTerm() != term
                    || raftNode.getRole() != RaftRole.FOLLOWER) {
                return;
            }
            synchronized (idempotencyLock) {
                pending = List.copyOf(pendingCommits.values());
                pendingCommits.clear();
            }
        }

        for (CompletableFuture<Boolean> future : pending) {
            future.complete(false);
        }
    }

    private boolean isActiveRunLocked(
            long expectedGeneration,
            RaftReplicationManager expectedReplicationManager
    ) {
        return running
                && lifecycleGeneration == expectedGeneration
                && replicationManager == expectedReplicationManager;
    }

    /**
     * Rolls back runtime components in reverse dependency order. This method is
     * deliberately independent from {@link #running}: startup may fail before
     * the service reaches its externally visible running state.
     */
    private void cleanupStartedComponents() {
        if (electionManager != null) {
            electionManager.stop();
        }
        if (replicationManager != null) {
            replicationManager.stop();
        }
        if (raftTransport != null) {
            raftTransport.stop();
        }
        if (rpcServer != null) {
            rpcServer.stop();
        }
        if (raftClock != null) {
            raftClock.shutdown();
        }
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

    private static final class PendingLocalBarrier {
        private final CompletableFuture<Boolean> completion = new CompletableFuture<>();
        private final Runnable onApplied;

        private PendingLocalBarrier(Runnable onApplied) {
            this.onApplied = onApplied;
        }

        private void activate() {
            if (completion.isDone()) {
                return;
            }
            try {
                onApplied.run();
                completion.complete(true);
            } catch (RuntimeException e) {
                completion.completeExceptionally(e);
            }
        }
    }
}
