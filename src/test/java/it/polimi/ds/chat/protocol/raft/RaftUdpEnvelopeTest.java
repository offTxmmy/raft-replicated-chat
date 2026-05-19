package it.polimi.ds.chat.protocol.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftUdpEnvelopeTest {

    @Test
    void broadcastEnvelopeIsAddressedToEveryNode() {
        RequestVoteRequestMessage payload = new RequestVoteRequestMessage(2L, 1, 5L, 1L);
        RaftUdpEnvelope envelope = new RaftUdpEnvelope(
                "cluster-a",
                "msg-1",
                1,
                RaftUdpEnvelope.BROADCAST_TARGET,
                RaftUdpMessageType.REQUEST_VOTE_REQUEST,
                2L,
                payload,
                123L);

        assertTrue(envelope.isBroadcast());
        assertTrue(envelope.isAddressedTo(1));
        assertTrue(envelope.isAddressedTo(2));
        assertEquals("cluster-a", envelope.getClusterId());
        assertEquals("msg-1", envelope.getMessageId());
        assertEquals(1, envelope.getSenderId());
        assertEquals(RaftUdpEnvelope.BROADCAST_TARGET, envelope.getTargetId());
        assertEquals(RaftUdpMessageType.REQUEST_VOTE_REQUEST, envelope.getType());
        assertEquals(2L, envelope.getTerm());
        assertEquals(payload, envelope.getPayload());
        assertEquals(123L, envelope.getCreatedAtMillis());
    }

    @Test
    void unicastEnvelopeIsAddressedOnlyToTargetNode() {
        RequestVoteResponseMessage payload = new RequestVoteResponseMessage(2L, true, 3);
        RaftUdpEnvelope envelope = new RaftUdpEnvelope(
                "cluster-a",
                "msg-2",
                3,
                1,
                RaftUdpMessageType.REQUEST_VOTE_RESPONSE,
                2L,
                payload,
                456L);

        assertFalse(envelope.isBroadcast());
        assertTrue(envelope.isAddressedTo(1));
        assertFalse(envelope.isAddressedTo(2));
    }

    @Test
    void blankClusterIdAndMessageIdRejected() {
        RequestVoteRequestMessage payload = new RequestVoteRequestMessage(2L, 1, 5L, 1L);

        assertThrows(IllegalArgumentException.class,
                () -> new RaftUdpEnvelope(
                        " ",
                        "msg-1",
                        1,
                        RaftUdpEnvelope.BROADCAST_TARGET,
                        RaftUdpMessageType.REQUEST_VOTE_REQUEST,
                        2L,
                        payload,
                        123L));

        assertThrows(IllegalArgumentException.class,
                () -> new RaftUdpEnvelope(
                        "cluster-a",
                        "",
                        1,
                        RaftUdpEnvelope.BROADCAST_TARGET,
                        RaftUdpMessageType.REQUEST_VOTE_REQUEST,
                        2L,
                        payload,
                        123L));
    }
}
