package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.RaftPeerEndpoint;
import it.polimi.ds.chat.messages.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.messages.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.messages.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.messages.raft.RequestVoteResponseMessage;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for the {@link RaftRpcServer} / {@link RaftRpcClient} pair:
 * round-trip serialization over loopback, correct dispatch by message type,
 * peer-id propagation for AppendEntries responses, and silent handling of
 * unreachable peers.
 */
class RaftRpcIntegrationTest {

    @Test
    void requestVoteRoundTrip() throws Exception {
        RaftRpcServer server = new RaftRpcServer(0,
                req -> new RequestVoteResponseMessage(req.getTerm(), true, 99),
                req -> { throw new AssertionError("append handler should not be invoked"); });
        server.start();
        int port = server.getBoundPort();

        try {
            Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
            voters.put(99, new RaftPeerEndpoint(99, "127.0.0.1", port));
            RaftRpcClient client = new RaftRpcClient(0, voters);

            AtomicReference<RequestVoteResponseMessage> received = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            client.attachHandlers(
                    response -> { received.set(response); latch.countDown(); },
                    (peer, r) -> { });
            client.start();

            client.sendRequestVote(99, new RequestVoteRequestMessage(3L, 0, 0L, 0L));

            assertTrue(latch.await(3, TimeUnit.SECONDS), "response not received");
            assertEquals(3L, received.get().getTerm());
            assertTrue(received.get().isVoteGranted());
            assertEquals(99, received.get().getVoterId());

            client.stop();
        } finally {
            server.stop();
        }
    }

    @Test
    void appendEntriesRoundTripCarriesPeerId() throws Exception {
        RaftRpcServer server = new RaftRpcServer(0,
                req -> { throw new AssertionError("vote handler should not be invoked"); },
            req -> new AppendEntriesResponseMessage(req.getTerm(), true, 42, 7L, -1L, 0L));
        server.start();
        int port = server.getBoundPort();

        try {
            Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
            voters.put(42, new RaftPeerEndpoint(42, "127.0.0.1", port));
            RaftRpcClient client = new RaftRpcClient(0, voters);

            AtomicReference<Integer> seenPeer = new AtomicReference<>();
            AtomicReference<AppendEntriesResponseMessage> seenResp = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            client.attachHandlers(
                    r -> { },
                    (peer, r) -> { seenPeer.set(peer); seenResp.set(r); latch.countDown(); });
            client.start();

            client.sendAppendEntries(42, new AppendEntriesRequestMessage(
                    5L, 0, 0L, 0L, Collections.emptyList(), 0L));

            assertTrue(latch.await(3, TimeUnit.SECONDS), "response not received");
            assertEquals(Integer.valueOf(42), seenPeer.get());
            assertEquals(5L, seenResp.get().getTerm());
            assertTrue(seenResp.get().isSuccess());
            assertEquals(7L, seenResp.get().getMatchIndex());

            client.stop();
        } finally {
            server.stop();
        }
    }

    @Test
    void unreachablePeerIsSilentlyIgnored() throws InterruptedException {
        // Port 1 is privileged on Unix and unbound on Windows: connection will be refused.
        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(1, new RaftPeerEndpoint(1, "127.0.0.1", 1));
        RaftRpcClient client = new RaftRpcClient(0, voters, 100, 100);

        AtomicBoolean voteCalled = new AtomicBoolean(false);
        client.attachHandlers(r -> voteCalled.set(true), (peer, r) -> { });
        client.start();

        client.sendRequestVote(1, new RequestVoteRequestMessage(1L, 0, 0L, 0L));

        Thread.sleep(400); // give the worker time to fail
        assertFalse(voteCalled.get(), "no response should arrive from an unreachable peer");

        client.stop();
    }
}