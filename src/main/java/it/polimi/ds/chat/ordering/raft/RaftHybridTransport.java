package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Hybrid Raft transport.
 *
 * <p>Uses UDP LAN broadcast where it is naturally beneficial and small
 * ({@code RequestVote}), while keeping TCP for payload-bearing or strictly
 * peer-specific traffic.
 */
public final class RaftHybridTransport implements RaftTransport {

    private final RaftTransport tcpTransport;
    private final RaftTransport udpTransport;

    public RaftHybridTransport(RaftRpcClient tcpTransport,
                               RaftUdpBroadcastTransport udpTransport) {
        this((RaftTransport) tcpTransport, udpTransport);
    }

    RaftHybridTransport(RaftTransport tcpTransport, RaftTransport udpTransport) {
        this.tcpTransport = Objects.requireNonNull(tcpTransport, "tcpTransport");
        this.udpTransport = Objects.requireNonNull(udpTransport, "udpTransport");
    }

    @Override
    public void attachHandlers(
            Consumer<RequestVoteResponseMessage> voteResponseHandler,
            BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler) {
        tcpTransport.attachHandlers(voteResponseHandler, appendResponseHandler);
        udpTransport.attachHandlers(voteResponseHandler, appendResponseHandler);
    }

    public void attachRequestHandlers(
            Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteRequestHandler,
            Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendRequestHandler) {
        if (!(udpTransport instanceof RaftUdpBroadcastTransport udpBroadcastTransport)) {
            throw new IllegalStateException("UDP transport does not support request handlers");
        }
        udpBroadcastTransport.attachRequestHandlers(voteRequestHandler, appendRequestHandler);
    }

    @Override
    public void start() {
        tcpTransport.start();
        udpTransport.start();
    }

    @Override
    public void stop() {
        udpTransport.stop();
        tcpTransport.stop();
    }

    @Override
    public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
        tcpTransport.sendRequestVote(peerId, request);
    }

    @Override
    public void broadcastRequestVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
        udpTransport.broadcastRequestVote(request, peerIds);
    }

    @Override
    public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
        tcpTransport.sendAppendEntries(peerId, request);
    }

    @Override
    public void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
        if (request.getEntries().isEmpty()) {
            udpTransport.broadcastAppendEntries(request, peerIds);
            return;
        }

        tcpTransport.broadcastAppendEntries(request, peerIds);
    }
}
