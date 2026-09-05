package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.*;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import static org.junit.jupiter.api.Assertions.*;

class AdversarialNetworkTest {
    @Test
    void repeatedUnansweredRpcUsesOneWorkerAndStopClosesItBeforeRestart() throws Exception {
        try (ServerSocket peer = new ServerSocket(0)) {
            CountDownLatch firstRead = new CountDownLatch(1);
            CountDownLatch firstClosed = new CountDownLatch(1);
            ExecutorService peerWorker = Executors.newSingleThreadExecutor();
            Future<?> exchange = peerWorker.submit(() -> {
                try {
                    try (Socket socket = peer.accept()) {
                        socket.setSoTimeout(3000);
                        new ObjectOutputStream(socket.getOutputStream()).flush();
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        assertTrue(in.readObject() instanceof RequestVoteRequestMessage);
                        firstRead.countDown();
                        assertThrows(EOFException.class, in::readObject);
                        firstClosed.countDown();
                    }
                    try (Socket socket = peer.accept()) {
                        socket.setSoTimeout(3000);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.flush();
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                        RequestVoteRequestMessage request = (RequestVoteRequestMessage) in.readObject();
                        assertEquals(2, request.getTerm());
                        out.writeObject(new RequestVoteResponseMessage(2, true, 2));
                        out.flush();
                    }
                } catch (Exception e) { throw new RuntimeException(e); }
            });
            RaftRpcClient client = new RaftRpcClient(1, Map.of(2,
                    new RaftPeerEndpoint(2, "127.0.0.1", peer.getLocalPort(), 50002)));
            BlockingQueue<Long> replies = new LinkedBlockingQueue<>();
            client.attachHandlers(r -> replies.add(r.getTerm()), (id, r) -> {});
            client.start();
            try {
                var request = new RequestVoteRequestMessage(1, 1, 0, 0);
                client.sendRequestVote(2, request);
                assertTrue(firstRead.await(2, TimeUnit.SECONDS));
                for (int retry = 0; retry < 1000; retry++) client.sendRequestVote(2, request);
                var field = RaftRpcClient.class.getDeclaredField("sendExecutor");
                field.setAccessible(true);
                assertEquals(1, ((ThreadPoolExecutor) field.get(client)).getLargestPoolSize(),
                        "retries created workers while the same peer RPC was still blocked");
                client.stop();
                assertTrue(firstClosed.await(2, TimeUnit.SECONDS));
                client.start();
                client.sendRequestVote(2, new RequestVoteRequestMessage(2, 1, 0, 0));
                assertEquals(2L, replies.poll(2, TimeUnit.SECONDS));
                assertTrue(replies.isEmpty());
                exchange.get(2, TimeUnit.SECONDS);
            } finally { client.stop(); peerWorker.shutdownNow(); }
        }
    }
    @Test
    void stopClosesAcceptedSocketEvenBeforeClientStreamHeader() throws Exception {
        RaftRpcServer server = new RaftRpcServer(0, r -> null, r -> null);
        server.start();
        try (Socket client = new Socket("127.0.0.1", server.getBoundPort())) {
            client.setSoTimeout(500);
            new ObjectInputStream(client.getInputStream()); // proves the handler accepted this socket
            server.stop();
            assertEquals(-1, client.getInputStream().read(), "stopped server left a handler/socket blocked");
        } finally { server.stop(); }
    }

    @Test
    void hybridElectionAndHeartbeatsReachTcpWhenUdpDropsEverything() throws Exception {
        RaftRpcServer server = new RaftRpcServer(0,
                r -> new RequestVoteResponseMessage(r.getTerm(), true, 2),
                r -> new AppendEntriesResponseMessage(r.getTerm(), true, 2, 0, -1, 0));
        server.attachPreVoteHandler(r -> new PreVoteResponseMessage(0, r.getTerm(), r.getRoundId(), true, 2));
        server.start();
        RaftRpcClient tcp = new RaftRpcClient(1, Map.of(2,
                new RaftPeerEndpoint(2, "127.0.0.1", server.getBoundPort(), 50002)));
        RaftTransport dropUdp = new RaftTransport() {
            public void attachPreVoteHandlers(Function<PreVoteRequestMessage, PreVoteResponseMessage> request, Consumer<PreVoteResponseMessage> response) { }
            public void attachHandlers(Consumer<RequestVoteResponseMessage> v, BiConsumer<Integer, AppendEntriesResponseMessage> a) { }
            public void start() { }
            public void stop() { }
            public void sendRequestVote(int peer, RequestVoteRequestMessage r) { }
            public void sendAppendEntries(int peer, AppendEntriesRequestMessage r) { }
            public void sendPreVote(int peer, PreVoteRequestMessage r) { }
        };
        RaftHybridTransport hybrid = new RaftHybridTransport(tcp, dropUdp);
        CountDownLatch responses = new CountDownLatch(3);
        hybrid.attachHandlers(r -> responses.countDown(), (peer, r) -> responses.countDown());
        hybrid.attachPreVoteHandlers(r -> null, r -> responses.countDown());
        hybrid.start();
        try {
            hybrid.broadcastPreVote(new PreVoteRequestMessage(1, 1, 0, 0, "round"), Set.of(2));
            hybrid.broadcastRequestVote(new RequestVoteRequestMessage(1, 1, 0, 0), Set.of(2));
            hybrid.broadcastAppendEntries(new AppendEntriesRequestMessage(1, 1, 0, 0, List.of(), 0), Set.of(2));
            assertTrue(responses.await(2, TimeUnit.SECONDS), "TCP majority is unreachable through HYBRID control path");
        } finally { hybrid.stop(); server.stop(); }
    }

    @Test
    void unavailableForwardIsNotReportedAsMalformedResponse() throws Exception {
        try (ServerSocket reservation = new ServerSocket(0)) {
            int port = reservation.getLocalPort();
            reservation.close();
            RaftRpcClient client = new RaftRpcClient(1, Map.of(2,
                    new RaftPeerEndpoint(2, "127.0.0.1", port, 50002)), 100, 100);
            client.attachHandlers(r -> {}, (peer, r) -> {});
            client.start();
            try {
                var response = client.forwardClientProposal(2, new ChatReqMessage("id", 1, "a", "b", new VectorClock()));
                assertFalse(response.isAccepted());
                assertFalse(response.getReason().contains("invalid"), response.getReason());
            } finally { client.stop(); }
        }
    }
}
