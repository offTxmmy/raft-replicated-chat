package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
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

    // Preserve UDP broadcast while periodically probing TCP: a filtered broadcast
    // must not prevent a TCP-connected majority from electing or staying idle.
    private int heartbeatRounds;
    private final int tcpHeartbeatEvery;
    private final RaftTransport tcpTransport;
    private final RaftTransport udpTransport;

    public RaftHybridTransport(RaftRpcClient tcpTransport,
                               RaftUdpBroadcastTransport udpTransport) {
        this((RaftTransport) tcpTransport, udpTransport);
    }

    RaftHybridTransport(RaftTransport tcpTransport, RaftTransport udpTransport) {
        this(tcpTransport, udpTransport, 1);
    }

    RaftHybridTransport(RaftTransport tcpTransport, RaftTransport udpTransport, int tcpHeartbeatEvery) {
        this.tcpTransport = Objects.requireNonNull(tcpTransport, "tcpTransport");
        this.udpTransport = Objects.requireNonNull(udpTransport, "udpTransport");
        this.tcpHeartbeatEvery = Math.max(1, tcpHeartbeatEvery);
    }

    @Override
    public void attachHandlers(
            Consumer<RequestVoteResponseMessage> voteResponseHandler,
            BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler) {
        tcpTransport.attachHandlers(voteResponseHandler, appendResponseHandler);
        udpTransport.attachHandlers(voteResponseHandler, appendResponseHandler);
    }

    @Override
    public void attachPreVoteHandlers(
            Function<PreVoteRequestMessage, PreVoteResponseMessage> requestHandler,
            Consumer<PreVoteResponseMessage> responseHandler) {
        tcpTransport.attachPreVoteHandlers(requestHandler, responseHandler);
        udpTransport.attachPreVoteHandlers(requestHandler, responseHandler);
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
        heartbeatRounds = 0;
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
    public void sendPreVote(int peerId, PreVoteRequestMessage request) {
        tcpTransport.sendPreVote(peerId, request);
    }

    @Override
    public void broadcastPreVote(PreVoteRequestMessage request, Set<Integer> peerIds) {
        try { udpTransport.broadcastPreVote(request, peerIds); }
        finally { tcpTransport.broadcastPreVote(request, peerIds); }
    }

    @Override
    public void broadcastRequestVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
        try { udpTransport.broadcastRequestVote(request, peerIds); }
        finally { tcpTransport.broadcastRequestVote(request, peerIds); }
    }

    @Override
    public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
        tcpTransport.sendAppendEntries(peerId, request);
    }

    @Override
    public void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
        if (request.getEntries().isEmpty()) {
            try { udpTransport.broadcastAppendEntries(request, peerIds); }
            finally {
                // The first and periodic common heartbeats also travel over TCP.
                // The period stays below the configured minimum election timeout.
                if (tcpHeartbeatDue()) tcpTransport.broadcastAppendEntries(request, peerIds);
            }
            return;
        }

        tcpTransport.broadcastAppendEntries(request, peerIds);
    }
    private synchronized boolean tcpHeartbeatDue() {
        boolean due = heartbeatRounds == 0;
        heartbeatRounds = (heartbeatRounds + 1) % tcpHeartbeatEvery;
        return due;
    }

}
