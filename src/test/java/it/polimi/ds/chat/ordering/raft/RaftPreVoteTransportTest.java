package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.RaftUdpEnvelope;
import it.polimi.ds.chat.protocol.raft.RaftUdpMessageType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Socket-level integration of the dedicated preliminary election protocol. */
class RaftPreVoteTransportTest {

    @Test
    void tcpBroadcastPreVoteRoundTripsAndNeverInvokesNormalVoteHandler() throws Exception {
        BlockingQueue<PreVoteRequestMessage> requests = new LinkedBlockingQueue<>();
        BlockingQueue<PreVoteResponseMessage> responses = new LinkedBlockingQueue<>();
        RaftRpcServer server = new RaftRpcServer(0,
                request -> { throw new AssertionError("PreVote must not reach RequestVote handler"); },
                request -> { throw new AssertionError("PreVote must not reach AppendEntries handler"); });
        server.attachPreVoteHandler(request -> {
            requests.add(request);
            return new PreVoteResponseMessage(3L, request.getTerm(), request.getRoundId(), true, 1);
        });
        server.start();
        RaftRpcClient client = new RaftRpcClient(0, Map.of(
                1, new RaftPeerEndpoint(1, "127.0.0.1", server.getBoundPort(), 50001)));
        client.attachHandlers(response -> {
            throw new AssertionError("PreVote response must not reach RequestVote handler");
        }, (peer, response) -> {
            throw new AssertionError("PreVote response must not reach AppendEntries handler");
        });
        client.attachPreVoteHandlers(request -> {
            throw new AssertionError("TCP client does not serve incoming requests");
        }, responses::add);
        client.start();
        try {
            client.broadcastPreVote(new PreVoteRequestMessage(4L, 0, 8L, 3L, "tcp-round"), Set.of(1));
            PreVoteRequestMessage request = requests.poll(3, TimeUnit.SECONDS);
            PreVoteResponseMessage response = responses.poll(3, TimeUnit.SECONDS);

            assertNotNull(request, "server did not dispatch the PreVote request");
            assertEquals(0, request.getCandidateId());
            assertEquals(8L, request.getLastLogIndex());
            assertEquals(3L, request.getLastLogTerm());
            assertNotNull(response, "client did not dispatch the PreVote response");
            assertEquals(3L, response.getTerm());
            assertEquals(4L, response.getProspectiveTerm());
            assertEquals("tcp-round", response.getRoundId());
            assertEquals(1, response.getVoterId());
            assertTrue(response.isVoteGranted());
        } finally {
            client.stop();
            server.stop();
        }
    }

    @Test
    void udpReceivesDedicatedPreVoteRequestsAndResponses() throws Exception {
        int port;
        try (DatagramSocket reservation = new DatagramSocket(0)) {
            port = reservation.getLocalPort();
        }
        RaftConfig config = new RaftConfig(200, 400, 40, 7000, RaftTransportMode.HYBRID,
                port, RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES, "prevote-test", Path.of("unused"), Map.of(
                0, new RaftPeerEndpoint(0, "127.0.0.1", 7000, 50000),
                1, new RaftPeerEndpoint(1, "127.0.0.1", 7001, 50001)));
        RaftUdpBroadcastTransport transport = new RaftUdpBroadcastTransport(0, config);
        BlockingQueue<PreVoteRequestMessage> requests = new LinkedBlockingQueue<>();
        BlockingQueue<PreVoteResponseMessage> responses = new LinkedBlockingQueue<>();
        transport.attachHandlers(response -> {
            throw new AssertionError("PreVote must not reach RequestVote response handler");
        }, (peer, response) -> {
            throw new AssertionError("PreVote must not reach AppendEntries response handler");
        });
        transport.attachRequestHandlers(request -> {
            throw new AssertionError("PreVote must not reach RequestVote request handler");
        }, request -> {
            throw new AssertionError("PreVote must not reach AppendEntries request handler");
        });
        transport.attachPreVoteHandlers(request -> {
            requests.add(request);
            return new PreVoteResponseMessage(3L, request.getTerm(), request.getRoundId(), true, 0);
        }, responses::add);
        transport.start();
        try (DatagramSocket sender = new DatagramSocket()) {
            send(sender, port, new RaftUdpEnvelope("prevote-test", "request-1", 1, -1,
                    RaftUdpMessageType.PRE_VOTE_REQUEST, 4L,
                    new PreVoteRequestMessage(4L, 1, 8L, 3L, "udp-round"), 1L));
            PreVoteRequestMessage request = requests.poll(3, TimeUnit.SECONDS);
            assertNotNull(request, "UDP receiver did not dispatch the PreVote request");
            assertEquals("udp-round", request.getRoundId());
            assertEquals(1, request.getCandidateId());

            send(sender, port, new RaftUdpEnvelope("prevote-test", "response-1", 1, 0,
                    RaftUdpMessageType.PRE_VOTE_RESPONSE, 3L,
                    new PreVoteResponseMessage(3L, 4L, "udp-round", true, 1), 2L));
            PreVoteResponseMessage response = responses.poll(3, TimeUnit.SECONDS);
            assertNotNull(response, "UDP receiver did not dispatch the PreVote response");
            assertEquals(3L, response.getTerm());
            assertEquals(4L, response.getProspectiveTerm());
            assertEquals("udp-round", response.getRoundId());
            assertEquals(1, response.getVoterId());
            assertTrue(response.isVoteGranted());
        } finally {
            transport.stop();
        }
    }

    private static void send(DatagramSocket socket, int port, RaftUdpEnvelope envelope) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(envelope);
        }
        byte[] payload = bytes.toByteArray();
        socket.send(new DatagramPacket(payload, payload.length, InetAddress.getLoopbackAddress(), port));
    }
}
