package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.RaftConfig;
import it.polimi.ds.chat.messages.ChatDeliverMessage;
import it.polimi.ds.chat.messages.ChatReqMessage;
import it.polimi.ds.chat.messages.raft.ForwardClientProposalRequestMessage;
import it.polimi.ds.chat.messages.raft.ForwardClientProposalResponseMessage;
import it.polimi.ds.chat.messages.raft.ChatCommand;
import it.polimi.ds.chat.ordering.OrderingService;
import it.polimi.ds.chat.ordering.OrderingServiceCallback;

import it.polimi.ds.chat.messages.raft.RaftLogEntry;

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
 *
 * <p>Wires together: durable {@code (term, vote)} state, the in-memory Raft
 * log, the commit manager, the election manager, the replication manager,
 * an RPC server and an RPC client. Committed log entries are translated into
 * application-level {@link ChatDeliverMessage} delivery via
 * {@link RaftStateMachineAdapter}.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #start()} loads persisted state, constructs all Raft
 *       components, attaches the RPC client response handlers, then starts
 *       (in order): RPC server, RPC client, replication manager, election
 *       manager. Inbound RPCs are answerable as soon as the server is up.
 *   <li>{@link #stop()} reverses the start order and shuts down the clock
 *       and persistence-backed components.
 * </ol>
 *
 * <p>{@link #propose(ChatReqMessage)}: the current leader appends the command
 * to the local log; followers proxy proposals to the known leader. Client
 * retries are idempotent on the leader by (username, MSG timestamp), so a
 * duplicate pending proposal waits for the original commit and a duplicate
 * committed proposal returns success without another append. Replication to
 * peers happens on the next heartbeat tick.
 *
 * <p>Both {@code (currentTerm, votedFor)} and log entries are persisted.
 * On startup, the replicated log is rebuilt from durable storage before
 * election and replication components are created.
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
    private RaftRpcClient rpcClient;
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
        raftLog.loadFromPersistence(persistence.loadLogEntries());

        // 3. State machine: deliver committed entries as ChatDeliverMessage.
        RaftStateMachineAdapter applyHook = new RaftStateMachineAdapter(this::notifyDelivery);
        commitManager = new RaftCommitManager(raftLog, entry -> {
            applyHook.accept(entry);
            completePendingCommit(entry);
        });

        // 4. RPC client (outbound transport). Response handlers are attached
        //    once the election/replication managers exist.
        rpcClient = new RaftRpcClient(localNodeId, raftConfig.getVoters());

        // 5. Clock for election timeout and heartbeat scheduling.
        raftClock = new DefaultRaftClock();

        // 6. Voter id set, derived from the static voter map (Contract A).
        Set<Integer> allVoterIds = new HashSet<>(raftConfig.getVoters().keySet());

        // 7. Leader-activity observer: forwards to the election manager.
        //    Field `electionManager` is null at this point but the lambda
        //    reads it lazily, so it resolves correctly once assigned below.
        RaftLeaderActivityObserver leaderActivityObserver =
                (term, leaderId) -> electionManager.onValidLeaderActivityObserved(term, leaderId);

        // 8. Replication manager — also acts as the election listener.
        replicationManager = new RaftReplicationManager(
                localNodeId,
                allVoterIds,
                raftNode,
                raftLog,
                commitManager,
                rpcClient,
                leaderActivityObserver
        );

        RaftElectionListener electionListener = new RaftElectionListener() {
            @Override
            public void onLeaderElected(int leaderId, long term) {
                replicationManager.onLeaderElected(leaderId, term);
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

        // 9. Election manager. Uses a consistent log-tip snapshot (Contract B).
        electionManager = new RaftElectionManager(
                localNodeId,
                allVoterIds,
                raftConfig.getElectionTimeoutMinMs(),
                raftConfig.getElectionTimeoutMaxMs(),
                raftConfig.getHeartbeatIntervalMs(),
                raftNode,
                raftLog,
                rpcClient,
                raftClock,
                electionListener
        );

        // 10. Wire RPC client response handlers (package-private callbacks
        //     on the election manager are visible here).
        rpcClient.attachHandlers(
                electionManager::onRequestVoteResponse,
                replicationManager::handleAppendEntriesResponse
        );

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
        rpcClient.start();
        replicationManager.start();
        electionManager.start();

        running = true;
        System.out.println("[RaftOrderingService] started, nodeId=" + localNodeId
                + ", voters=" + raftConfig.getVoters().keySet()
                + ", rpcPort=" + raftConfig.getRpcPort()
                + ", restoredTerm=" + persisted.currentTerm()
                + ", restoredVote=" + persisted.votedFor()
                + ", restoredLogLastIndex=" + raftLog.lastLogIndex()
                + ", restoredLogLastTerm=" + raftLog.lastLogTerm());
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
        if (rpcClient         != null) rpcClient.stop();
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
            if (request.hasClientTimestamp()) {
                command = new ChatCommand(
                        request.getLocalMsgId(),
                        request.getBrokerId(),
                        request.getUsername(),
                        request.getText(),
                        request.getVectorClock(),
                        request.getClientTimestamp()
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
                rpcClient.forwardClientProposal(leaderId, request);
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
        if (request.hasClientTimestamp()) {
            return "client:" + request.getUsername() + ":" + request.getClientTimestamp();
        }
        return "local:" + request.getLocalMsgId();
    }

    private static String proposalKey(ChatCommand command) {
        if (command.hasClientTimestamp()) {
            return "client:" + command.getUsername() + ":" + command.getClientTimestamp();
        }
        return "local:" + command.getLocalMsgId();
    }
}
