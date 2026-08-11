package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BrokerMainTest {

    @Test
    void raftCommandKeepsHybridAsDefault() {
        assertEquals(RaftTransportMode.HYBRID, BrokerMain.transportModeForCommand("raft"));
    }

    @Test
    void raftLocalCommandSelectsTcpOnlyTransport() {
        assertEquals(RaftTransportMode.LOCAL_TCP,
                BrokerMain.transportModeForCommand("raft-local"));
    }

    @Test
    void unknownCommandIsRejected() {
        assertNull(BrokerMain.transportModeForCommand("unknown"));
    }
}
