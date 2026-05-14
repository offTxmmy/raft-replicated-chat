package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.RaftConfig;
import it.polimi.ds.chat.messages.ChatDeliverMessage;
import it.polimi.ds.chat.messages.ChatReqMessage;
import it.polimi.ds.chat.messages.raft.ChatCommand;
import it.polimi.ds.chat.ordering.OrderingService;

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
 * <p>{@link #propose(ChatReqMessage)}: only the current leader appends the
 * command to the local log; followers drop the proposal silently for now.
 * Replication to peers happens on the next heartbeat tick. Client-side
 * redirect via {@link #getLeaderId()} can be added at the application layer.
 *
 * <p><b>Scope note:</b> log entries are not yet persisted in this phase.
 * Only {@code (currentTerm, votedFor)} is durable. Log persistence is a
 * one-line wiring change once {@link RaftLog} accepts a {@link RaftPersistence}.
 */
public final class RaftOrderingService implements OrderingService {

    private final BrokerConfig brokerConfig;
    private final RaftConfig raftConfig;
    private final int localNodeId;

    private final CopyOnWriteArrayList<Consumer<ChatDeliverMessage>> deliveryCallbacks =
            new CopyOnWriteArrayList<>();

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
        raftLog  = new RaftLog();
        // NOTE: log replay from persistence deferred — see scope note above.

        // 3. State machine: deliver committed entries as ChatDeliverMessage.
        RaftStateMachineAdapter applyHook = new RaftStateMachineAdapter(this::notifyDelivery);
        commitManager = new RaftCommitManager(raftLog, applyHook);

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
                replicationManager
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
                replicationManager::handleAppendEntries
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
                + ", restoredVote=" + persisted.votedFor());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        if (electionManager   != null) electionManager.stop();
        if (replicationManager != null) replicationManager.stop();
        if (rpcClient         != null) rpcClient.stop();
        if (rpcServer         != null) rpcServer.stop();
        if (raftClock         != null) raftClock.shutdown();

        System.out.println("[RaftOrderingService] stopped");
    }

    @Override
    public void propose(ChatReqMessage request) {
        if (!running) {
            return;
        }
        if (!raftNode.isLeader()) {
            System.err.println("[RaftOrderingService] propose ignored: not leader. Known leader = "
                    + raftNode.getLeaderId());
            return;
        }
        ChatCommand command = new ChatCommand(
                request.getLocalMsgId(),
                request.getBrokerId(),
                request.getUsername(),
                request.getText(),
                request.getVectorClock()
        );
        replicationManager.appendCommandAsLeader(command);
        // Replication to peers is driven by the next heartbeat tick.
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
}