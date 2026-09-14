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
import java.util.concurrent.atomic.AtomicBoolean;
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
    private volatile RuntimeException terminalFailure;
    private Consumer<Throwable> failureHandler = failure -> stop();

    /** The owner must withdraw its client listener and Directory lease as well. */
    public synchronized void setFailureHandler(Consumer<Throwable> failureHandler) {
        this.failureHandler = Objects.requireNonNull(failureHandler);
    }

    private void storageFailed(FileRaftPersistence.RaftPersistenceException failure) {
        failTerminally("durable-state", failure);
    }

    private void applicationFailed(RuntimeException failure) {
        failTerminally("state-machine application", failure);
    }

    private void failTerminally(String component, RuntimeException failure) {
        synchronized (this) {
            if (terminalFailure != null) {
                return;
            }
            terminalFailure = failure;
            running = false;
        }
        System.err.println("[RaftOrderingService] node=" + localNodeId + " FATAL "
                + component + " failure; withdrawing broker");
        failure.printStackTrace(System.err);
        // Failure may be observed while Raft component monitors are held.
        Thread shutdown = new Thread(() -> failureHandler.accept(failure),
                "raft-fatal-stop-" + localNodeId);
        shutdown.setDaemon(true);
        shutdown.start();
    }
    // Invalidates manager callbacks that escaped their old run before stop().
    private volatile long lifecycleGeneration;

    private static final long PROPOSE_COMMIT_TIMEOUT_MS = 5_000L;

    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> pendingCommits =
            new ConcurrentHashMap<>();

    // Guards the check-and-register sequence for retry idempotency.
    private final Set<String> committedProposalKeys = ConcurrentHashMap.newKeySet();
    private final Object idempotencyLock = new Object();

    public RaftOrderingService(BrokerConfig brokerConfig) {
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
        if (terminalFailure != null) throw terminalFailure;
        long runGeneration = ++lifecycleGeneration;
        try {
            startRun(runGeneration);
        } catch (RuntimeException failure) {
            running = false;
            lifecycleGeneration++;
            cleanupStartedComponents();
            throw failure;
        }
    }

    private void startRun(long runGeneration) {
        // 1. Persistence + recovery of (term, vote).
        persistence = new GuardedRaftPersistence(new FileRaftPersistence(raftConfig.getStorageDir()), this::storageFailed);
        RaftPersistence runPersistence = persistence;
        RaftPersistence.PersistedState persisted = persistence.loadTermAndVote();

        // 2. Core Raft state.
        raftNode = new RaftNode(localNodeId, persistence,
                persisted.currentTerm(), persisted.votedFor());
        raftLog  = new RaftLog(persistence);
        List<RaftLogEntry> persistedEntries = persistence.loadLogEntries();
        raftLog.loadFromPersistence(persistedEntries);

        RaftPersistence.CommitProgress persistedProgress = persistence.loadCommitProgress();
        if (persisted.currentTerm() < 0 || persistedProgress.commitIndex() < 0
                || persistedProgress.lastApplied() < 0
                || persistedProgress.lastApplied() > persistedProgress.commitIndex()
                || persistedProgress.commitIndex() > raftLog.lastLogIndex()
                || raftLog.lastLogTerm() > persisted.currentTerm()) {
            throw new FileRaftPersistence.RaftPersistenceException(
                    "Inconsistent durable Raft state: refusing to discard committed progress", null);
        }
        long restoredCommitIndex = persistedProgress.commitIndex();
        long restoredLastApplied = persistedProgress.lastApplied();

        // Rebuild durable client-operation deduplication from the applied prefix.
        // Do not replay callbacks: Raft storage is protocol state, not chat history.
        committedProposalKeys.clear();
        for (RaftLogEntry entry : persistedEntries) {
            if (entry.getIndex() > restoredLastApplied) {
                break;
            }
            if (entry.getCommand() != null) {
                committedProposalKeys.add(proposalKey(entry.getCommand()));
            }
        }
        RaftStateMachineAdapter applyHook = new RaftStateMachineAdapter(this::notifyDelivery);

        // 3. Resume normal application. The constructor applies exactly the
        // committed-but-not-applied suffix (restoredLastApplied, restoredCommitIndex].
        commitManager = new RaftCommitManager(
                raftLog,
                entry -> { runPersistence.checkHealthy(); applyCommittedEntryOnce(entry, applyHook); },
                restoredCommitIndex,
                restoredLastApplied,
                (commitIndex, lastApplied) ->
                        runPersistence.persistCommitProgress(commitIndex, lastApplied)
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
        running = false;
        lifecycleGeneration++;

        pendingCommits.forEach((id, future) -> future.complete(false));
        pendingCommits.clear();
        committedProposalKeys.clear();

        cleanupStartedComponents();

        System.out.println("[RaftOrderingService] stopped");
    }

    @Override
    public boolean propose(ChatReqMessage request) {
        if (!running || terminalFailure != null) {
            return false;
        }
        if (!raftNode.isLeader()) {
            return forwardProposalToLeader(request);
        }

        return appendAndWaitForCommit(request);
    }

    @Override
    public boolean executeAtDeliveryBoundary(Runnable action) {
        Objects.requireNonNull(action, "action");
        RaftCommitManager manager;
        long expectedGeneration;
        synchronized (this) {
            if (!running || terminalFailure != null || commitManager == null) {
                return false;
            }
            manager = commitManager;
            expectedGeneration = lifecycleGeneration;
        }

        AtomicBoolean executed = new AtomicBoolean(false);
        manager.executeAtApplyBoundary(() -> {
            // The service may fail or be stopped while this boundary is queued
            // behind an in-progress application. Revalidate only after owning
            // the apply monitor, so a failed state machine cannot publish a JOIN.
            // Volatile state avoids taking the service monitor in the opposite
            // order and therefore preserves the Raft lock-order discipline.
            if (!running || terminalFailure != null
                    || lifecycleGeneration != expectedGeneration) {
                return;
            }
            action.run();
            executed.set(true);
        });
        return executed.get();
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
            ChatCommand command = new ChatCommand(
                    request.getUsername(), request.getClientId(),
                    request.getClientSeq(), request.getText());

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
        if (!running || terminalFailure != null) {
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
        return running && terminalFailure == null && raftNode != null && raftNode.isLeader();
    }

    @Override
    public int getLeaderId() {
        return terminalFailure != null || raftNode == null ? -1 : raftNode.getLeaderId();
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
        int tcpHeartbeatEvery = (int) Math.max(1L, Math.min(5L,
                raftConfig.getElectionTimeoutMinMs() / raftConfig.getHeartbeatIntervalMs() / 2L));
        return new RaftHybridTransport(tcpClient, udpTransport, tcpHeartbeatEvery);
    }

    private void notifyDelivery(ChatDeliverMessage message) {
        for (Consumer<ChatDeliverMessage> cb : deliveryCallbacks) {
            cb.accept(message);
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
        boolean firstApplication;

        synchronized (idempotencyLock) {
            firstApplication = !committedProposalKeys.contains(key);
        }

        if (firstApplication) {
            try {
                applyHook.accept(entry);
            } catch (RuntimeException failure) {
                applicationFailed(failure);
                throw failure;
            }
        }

        synchronized (idempotencyLock) {
            committedProposalKeys.add(key);
            pending = pendingCommits.remove(key);
        }

        if (pending != null) {
            pending.complete(true);
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
                && terminalFailure == null
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
        if (persistence != null) {
            persistence.close();
        }
    }

    private static String proposalKey(ChatReqMessage request) {
        return clientProposalKey(request.getClientId(), request.getClientSeq());
    }

    private static String proposalKey(ChatCommand command) {
        return clientProposalKey(command.getClientId(), command.getClientSeq());
    }

    private static String clientProposalKey(String clientId, long clientSeq) {
        return "client:" + clientId + ":" + clientSeq;
    }
}
