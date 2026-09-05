package it.polimi.ds.chat.protocol.raft;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreVoteMessageSerializationTest {

    @Test
    void requestEnvelopePreservesProspectiveTermLogAndRoundWithinDefaultPayloadLimit() throws Exception {
        PreVoteRequestMessage request = new PreVoteRequestMessage(7L, 1, 12L, 5L, "round-123");
        RaftUdpEnvelope envelope = new RaftUdpEnvelope("lan-demo", "message-1", 1, -1,
                RaftUdpMessageType.PRE_VOTE_REQUEST, 7L, request, 1L);

        RaftUdpEnvelope copy = roundTrip(envelope);
        PreVoteRequestMessage payload = (PreVoteRequestMessage) copy.getPayload();

        assertEquals(RaftUdpMessageType.PRE_VOTE_REQUEST, copy.getType());
        assertTrue(copy.isBroadcast());
        assertEquals(7L, payload.getTerm());
        assertEquals(1, payload.getCandidateId());
        assertEquals(12L, payload.getLastLogIndex());
        assertEquals(5L, payload.getLastLogTerm());
        assertEquals("round-123", payload.getRoundId());
        assertEquals(1L, ObjectStreamClass.lookup(PreVoteRequestMessage.class).getSerialVersionUID());
    }

    @Test
    void responseEnvelopeKeepsActualAndProspectiveTermsDistinct() throws Exception {
        PreVoteResponseMessage response = new PreVoteResponseMessage(6L, 7L, "round-123", false, 2);
        RaftUdpEnvelope envelope = new RaftUdpEnvelope("lan-demo", "message-2", 2, 1,
                RaftUdpMessageType.PRE_VOTE_RESPONSE, 6L, response, 2L);

        RaftUdpEnvelope copy = roundTrip(envelope);
        PreVoteResponseMessage payload = (PreVoteResponseMessage) copy.getPayload();

        assertEquals(RaftUdpMessageType.PRE_VOTE_RESPONSE, copy.getType());
        assertEquals(6L, copy.getTerm());
        assertTrue(copy.isAddressedTo(1));
        assertFalse(copy.isAddressedTo(3));
        assertEquals(6L, payload.getTerm());
        assertEquals(7L, payload.getProspectiveTerm());
        assertEquals("round-123", payload.getRoundId());
        assertFalse(payload.isVoteGranted());
        assertEquals(2, payload.getVoterId());
        assertEquals(1L, ObjectStreamClass.lookup(PreVoteResponseMessage.class).getSerialVersionUID());
    }

    private static RaftUdpEnvelope roundTrip(RaftUdpEnvelope envelope) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(envelope);
        }
        assertTrue(bytes.size() <= RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES,
                "PreVote envelopes must fit the default UDP datagram budget");
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (RaftUdpEnvelope) input.readObject();
        }
    }
}
