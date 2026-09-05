package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ForwardClientProposalRequestMessage;
import it.polimi.ds.chat.protocol.raft.ForwardClientProposalResponseMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * TCP server for inbound Raft RPCs.
 *
 * <p>One request-response per connection. Dispatch is delegated to handler
 * functions injected at construction time, so the server has no knowledge of
 * the election or replication logic (Contract C).
 *
 * <ul>
 *   <li>{@link RequestVoteRequestMessage}     → {@code voteHandler}     → response written back
 *   <li>{@link AppendEntriesRequestMessage}   → {@code appendHandler}   → response written back
 *   <li>{@link ForwardClientProposalRequestMessage} → {@code forwardHandler} → response written back
 * </ul>
 *
 * <p>Handler functions run on the per-connection worker thread. They are
 * expected to be synchronous and reasonably fast (the existing election /
 * replication managers are both synchronous and short-running).
 */
public final class RaftRpcServer {

    private final int requestedPort;
    private final Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteHandler;
    private final Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendHandler;
    private final Function<ForwardClientProposalRequestMessage, ForwardClientProposalResponseMessage> forwardHandler;
    private Function<PreVoteRequestMessage, PreVoteResponseMessage> preVoteHandler;

    private ServerSocket serverSocket;
    private ExecutorService acceptExecutor;
    private ExecutorService handlerExecutor;
    private volatile boolean running;
    private int boundPort = -1;

    public RaftRpcServer(int port,
                         Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteHandler,
                         Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendHandler) {
        this(port, voteHandler, appendHandler,
                req -> new ForwardClientProposalResponseMessage(false, -1, "forward handler unavailable"));
    }

    public RaftRpcServer(int port,
                         Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteHandler,
                         Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendHandler,
                         Function<ForwardClientProposalRequestMessage, ForwardClientProposalResponseMessage> forwardHandler) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        this.requestedPort = port;
        this.voteHandler = Objects.requireNonNull(voteHandler, "voteHandler");
        this.appendHandler = Objects.requireNonNull(appendHandler, "appendHandler");
        this.forwardHandler = Objects.requireNonNull(forwardHandler, "forwardHandler");
    }

    /** Installs the preliminary election handler before the server starts. */
    public synchronized void attachPreVoteHandler(
            Function<PreVoteRequestMessage, PreVoteResponseMessage> preVoteHandler) {
        if (running) {
            throw new IllegalStateException("PreVote handler must be installed before start");
        }
        this.preVoteHandler = Objects.requireNonNull(preVoteHandler, "preVoteHandler");
    }

    public synchronized void start() throws IOException {
        if (running) {
            return;
        }
        serverSocket = new ServerSocket(requestedPort);
        boundPort = serverSocket.getLocalPort();
        acceptExecutor  = Executors.newSingleThreadExecutor(r -> daemon(r, "raft-rpc-accept"));
        handlerExecutor = Executors.newCachedThreadPool(r -> daemon(r, "raft-rpc-handler"));
        running = true;
        acceptExecutor.submit(this::acceptLoop);
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        if (acceptExecutor != null) acceptExecutor.shutdownNow();
        if (handlerExecutor != null) handlerExecutor.shutdownNow();
        try {
            if (handlerExecutor != null) handlerExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Actual TCP port the server is bound on. Useful when {@code port == 0}
     * was passed to the constructor to obtain an ephemeral port (typical in
     * tests). Returns -1 if the server has never been started.
     */
    public int getBoundPort() {
        return boundPort;
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket sock = serverSocket.accept();
                handlerExecutor.submit(() -> handleConnection(sock));
            } catch (IOException e) {
                if (running) {
                    System.err.println("[RaftRpcServer] accept failed: " + e.getMessage());
                }
            }
        }
    }

    private void handleConnection(Socket sock) {
        try (sock) {
            // ObjectOutputStream must be created and flushed BEFORE ObjectInputStream
            // on both ends: its constructor writes a stream header that the peer's
            // ObjectInputStream reads. Creating OIS first on both sides would deadlock.
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            Object msg = in.readObject();
            Object response = dispatch(msg);
            if (response != null) {
                out.writeObject(response);
                out.flush();
            }
        } catch (EOFException eof) {
            // peer closed before sending — ignore
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[RaftRpcServer] connection error: " + e.getMessage());
        }
    }

    private Object dispatch(Object msg) {
        if (msg instanceof PreVoteRequestMessage r) {
            if (preVoteHandler == null) {
                System.err.println("[RaftRpcServer] PreVote handler unavailable");
                return null;
            }
            return preVoteHandler.apply(r);
        }
        if (msg instanceof RequestVoteRequestMessage r) {
            return voteHandler.apply(r);
        }
        if (msg instanceof AppendEntriesRequestMessage r) {
            return appendHandler.apply(r);
        }
        if (msg instanceof ForwardClientProposalRequestMessage r) {
            return forwardHandler.apply(r);
        }
        System.err.println("[RaftRpcServer] unknown message type: "
                + (msg == null ? "null" : msg.getClass().getName()));
        return null;
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }
}
