package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftHybridTransportTest {

    @Test
    void broadcastPreVoteUsesUdpAndTargetedPreVoteUsesTcp() {
        RecordingTransport tcp = new RecordingTransport();
        RecordingTransport udp = new RecordingTransport();
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, udp);
        PreVoteRequestMessage request = new PreVoteRequestMessage(3L, 1, 5L, 2L, "round-1");

        hybrid.broadcastPreVote(request, Set.of(2, 3));
        hybrid.sendPreVote(2, request);

        assertEquals(List.of(new BroadcastPreVote(request, Set.of(2, 3))), tcp.broadcastPreVoteRequests);
        assertEquals(List.of(new BroadcastPreVote(request, Set.of(2, 3))), udp.broadcastPreVoteRequests);
        assertEquals(List.of(new SentPreVote(2, request)), tcp.sentPreVoteRequests);
        assertTrue(udp.sentPreVoteRequests.isEmpty());
    }

    @Test
    void preVoteHandlersAreAttachedToBothTransports() {
        RecordingTransport tcp = new RecordingTransport();
        RecordingTransport udp = new RecordingTransport();
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, udp);
        Function<PreVoteRequestMessage, PreVoteResponseMessage> requestHandler =
                request -> new PreVoteResponseMessage(2L, request.getTerm(), request.getRoundId(), true, 1);
        Consumer<PreVoteResponseMessage> responseHandler = response -> { };

        hybrid.attachPreVoteHandlers(requestHandler, responseHandler);

        assertEquals(requestHandler, tcp.preVoteRequestHandler);
        assertEquals(requestHandler, udp.preVoteRequestHandler);
        assertEquals(responseHandler, tcp.preVoteResponseHandler);
        assertEquals(responseHandler, udp.preVoteResponseHandler);
    }

    @Test
    void broadcastRequestVoteShouldUseUdpTransport() {
        RecordingTransport tcp = new RecordingTransport();
        RecordingTransport udp = new RecordingTransport();
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, udp);

        RequestVoteRequestMessage request = new RequestVoteRequestMessage(2L, 1, 5L, 2L);

        hybrid.broadcastRequestVote(request, Set.of(2, 3));

        assertEquals(1, tcp.broadcastVoteRequests.size());
        assertEquals(1, udp.broadcastVoteRequests.size());
        assertEquals(Set.of(2, 3), udp.broadcastVoteRequests.get(0).peerIds());
    }

    @Test
    void emptyAppendEntriesBroadcastShouldUseUdpTransport() {
        RecordingTransport tcp = new RecordingTransport();
        RecordingTransport udp = new RecordingTransport();
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, udp);

        AppendEntriesRequestMessage heartbeat = new AppendEntriesRequestMessage(
                2L,
                1,
                5L,
                2L,
                List.of(),
                4L
        );

        hybrid.broadcastAppendEntries(heartbeat, Set.of(2, 3));

        assertEquals(1, tcp.broadcastAppendEntriesRequests.size());
        assertEquals(1, udp.broadcastAppendEntriesRequests.size());
        BroadcastAppendEntries broadcast = udp.broadcastAppendEntriesRequests.get(0);
        assertEquals(heartbeat, broadcast.request());
        assertEquals(Set.of(2, 3), broadcast.peerIds());
    }

    @Test
    void nonEmptyAppendEntriesBroadcastShouldUseTcpTransport() {
        RecordingTransport tcp = new RecordingTransport();
        RecordingTransport udp = new RecordingTransport();
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, udp);

        AppendEntriesRequestMessage appendEntries = new AppendEntriesRequestMessage(
                2L,
                1,
                5L,
                2L,
                List.of(new RaftLogEntry(6L, 2L, command("x"))),
                4L
        );

        hybrid.broadcastAppendEntries(appendEntries, Set.of(2, 3));

        assertEquals(1, tcp.broadcastAppendEntriesRequests.size());
        assertEquals(0, udp.broadcastAppendEntriesRequests.size());
        BroadcastAppendEntries broadcast = tcp.broadcastAppendEntriesRequests.get(0);
        assertEquals(appendEntries, broadcast.request());
        assertEquals(Set.of(2, 3), broadcast.peerIds());
    }

    @Test
    void directAppendEntriesShouldUseTcpTransport() {
        RecordingTransport tcp = new RecordingTransport();
        RecordingTransport udp = new RecordingTransport();
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, udp);

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                2L,
                1,
                5L,
                2L,
                List.of(),
                4L
        );

        hybrid.sendAppendEntries(2, request);

        assertEquals(List.of(new SentAppendEntries(2, request)), tcp.sentAppendEntriesRequests);
        assertTrue(udp.sentAppendEntriesRequests.isEmpty());
    }

    private static ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }

    private record BroadcastVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
    }

    private record BroadcastPreVote(PreVoteRequestMessage request, Set<Integer> peerIds) {
    }

    private record SentPreVote(int peerId, PreVoteRequestMessage request) {
    }

    private record BroadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
    }

    private record SentAppendEntries(int peerId, AppendEntriesRequestMessage request) {
    }

    private static final class RecordingTransport implements RaftTransport {
        private final List<BroadcastPreVote> broadcastPreVoteRequests = new ArrayList<>();
        private final List<SentPreVote> sentPreVoteRequests = new ArrayList<>();
        private Function<PreVoteRequestMessage, PreVoteResponseMessage> preVoteRequestHandler;
        private Consumer<PreVoteResponseMessage> preVoteResponseHandler;
        private final List<BroadcastVote> broadcastVoteRequests = new ArrayList<>();
        private final List<BroadcastAppendEntries> broadcastAppendEntriesRequests = new ArrayList<>();
        private final List<SentAppendEntries> sentAppendEntriesRequests = new ArrayList<>();

        @Override
        public void attachPreVoteHandlers(
                Function<PreVoteRequestMessage, PreVoteResponseMessage> requestHandler,
                Consumer<PreVoteResponseMessage> responseHandler) {
            preVoteRequestHandler = requestHandler;
            preVoteResponseHandler = responseHandler;
        }

        @Override
        public void sendPreVote(int peerId, PreVoteRequestMessage request) {
            sentPreVoteRequests.add(new SentPreVote(peerId, request));
        }

        @Override
        public void broadcastPreVote(PreVoteRequestMessage request, Set<Integer> peerIds) {
            broadcastPreVoteRequests.add(new BroadcastPreVote(request, Set.copyOf(peerIds)));
        }

        @Override
        public void attachHandlers(
                Consumer<RequestVoteResponseMessage> voteResponseHandler,
                BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler) {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
        }

        @Override
        public void broadcastRequestVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
            broadcastVoteRequests.add(new BroadcastVote(request, Set.copyOf(peerIds)));
        }

        @Override
        public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
            sentAppendEntriesRequests.add(new SentAppendEntries(peerId, request));
        }

        @Override
        public void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
            broadcastAppendEntriesRequests.add(new BroadcastAppendEntries(request, Set.copyOf(peerIds)));
        }
    }
}
