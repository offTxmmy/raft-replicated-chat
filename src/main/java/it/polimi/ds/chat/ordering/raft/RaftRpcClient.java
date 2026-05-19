package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ForwardClientProposalRequestMessage;
import it.polimi.ds.chat.protocol.raft.ForwardClientProposalResponseMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * TCP client for outbound Raft RPCs.
 *
 * <p>Implements both {@link RaftVoteRequestSender} and {@link RaftAppendEntriesSender}.
 * Each send is fire-and-forget: the call returns immediately and the request
 * is dispatched on a worker thread, which opens a fresh TCP connection, writes
 * the request, reads the response, and routes it to the installed response
 * handler. Failures (unreachable peer, timeout, deserialization) are swallowed
 * — Raft naturally retries on the next heartbeat / election tick.
 *
 * <p>Response handlers must be installed via {@link #attachHandlers(Consumer, BiConsumer)}
 * before {@link #start()}. This solves the chicken-and-egg between the client
 * (needed by the election/replication managers as a sender) and the managers
 * (needed by the client to dispatch responses).
 */
public final class RaftRpcClient implements RaftTransport {

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 500;
    private static final int DEFAULT_READ_TIMEOUT_MS    = 1000;
    private static final int FORWARD_PROPOSAL_READ_TIMEOUT_MS = 7000;

    private final int localNodeId;
    private final Map<Integer, RaftPeerEndpoint> voters;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private Consumer<RequestVoteResponseMessage> voteResponseHandler;
    private BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler;

    private ExecutorService sendExecutor;
    private volatile boolean running;

    public RaftRpcClient(int localNodeId, Map<Integer, RaftPeerEndpoint> voters) {
        this(localNodeId, voters, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public RaftRpcClient(int localNodeId,
                         Map<Integer, RaftPeerEndpoint> voters,
                         int connectTimeoutMs,
                         int readTimeoutMs) {
        this.localNodeId = localNodeId;
        Objects.requireNonNull(voters, "voters");
        this.voters = Collections.unmodifiableMap(new TreeMap<>(voters));
        if (connectTimeoutMs <= 0 || readTimeoutMs <= 0) {
            throw new IllegalArgumentException("timeouts must be > 0");
        }
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs    = readTimeoutMs;
    }

    /**
     * Installs the response handlers. Must be called before {@link #start()}.
     */
    @Override
    public synchronized void attachHandlers(
            Consumer<RequestVoteResponseMessage> voteResponseHandler,
            BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler) {
        this.voteResponseHandler   = Objects.requireNonNull(voteResponseHandler);
        this.appendResponseHandler = Objects.requireNonNull(appendResponseHandler);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (voteResponseHandler == null || appendResponseHandler == null) {
            throw new IllegalStateException("attachHandlers must be called before start");
        }
        sendExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "raft-rpc-client");
            t.setDaemon(true);
            return t;
        });
        running = true;
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (sendExecutor != null) {
            sendExecutor.shutdownNow();
            try {
                sendExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int getLocalNodeId() {
        return localNodeId;
    }

    @Override
    public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
        sendAsync(peerId, request, response -> {
            if (response instanceof RequestVoteResponseMessage r) {
                voteResponseHandler.accept(r);
            }
        });
    }

    @Override
    public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
        sendAsync(peerId, request, response -> {
            if (response instanceof AppendEntriesResponseMessage r) {
                appendResponseHandler.accept(peerId, r);
            }
        });
    }

    public ForwardClientProposalResponseMessage forwardClientProposal(int leaderId, ChatReqMessage request) {
        if (!running) {
            return new ForwardClientProposalResponseMessage(false, -1, "rpc client not running");
        }
        RaftPeerEndpoint endpoint = voters.get(leaderId);
        if (endpoint == null) {
            return new ForwardClientProposalResponseMessage(false, -1, "unknown leader endpoint");
        }

        Object response = exchangeBlocking(
                endpoint,
                new ForwardClientProposalRequestMessage(request),
                FORWARD_PROPOSAL_READ_TIMEOUT_MS);

        if (response instanceof ForwardClientProposalResponseMessage r) {
            return r;
        }
        return new ForwardClientProposalResponseMessage(false, -1, "invalid forward response");
    }

    private void sendAsync(int peerId, Object request, Consumer<Object> onResponse) {
        if (!running) {
            return;
        }
        RaftPeerEndpoint endpoint = voters.get(peerId);
        if (endpoint == null) {
            return;
        }
        sendExecutor.submit(() -> {
            Object response = exchangeBlocking(endpoint, request, readTimeoutMs);
            if (response != null) {
                onResponse.accept(response);
            }
        });
    }

    private Object exchangeBlocking(RaftPeerEndpoint endpoint, Object request, int socketReadTimeoutMs) {
        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress(endpoint.host(), endpoint.rpcPort()), connectTimeoutMs);
            sock.setSoTimeout(socketReadTimeoutMs);

            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            out.writeObject(request);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            // peer unreachable, timeout, or malformed reply: Raft will retry naturally
            return null;
        }
    }
}
