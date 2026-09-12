package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BrokerMainTest {
    @Test
    void raftCommandKeepsHybridAsDefault() {
        assertEquals(RaftTransportMode.HYBRID, BrokerMain.transportModeForCommand("raft"));
    }

    @Test
    void raftLocalCommandSelectsTcpOnlyTransport() {
        assertEquals(RaftTransportMode.LOCAL_TCP, BrokerMain.transportModeForCommand("raft-local"));
    }

    @Test
    void unknownCommandIsRejected() {
        assertNull(BrokerMain.transportModeForCommand("unknown"));
    }

    @Test
    void parsesStaticVotersAndDerivesTheLocalEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = RaftVoterParser.parse(
                "2@broker-2:7002:52345,1@broker-1:7001");
        assertEquals(new RaftPeerEndpoint(2, "broker-2", 7002, 52345), voters.get(2));
        assertEquals(50001, voters.get(1).clientPort());
    }

    @Test
    void rejectsMalformedOrDuplicateVoters() {
        assertThrows(IllegalArgumentException.class,
                () -> RaftVoterParser.parse("2@broker-2:7002,,1@broker-1:7001"));
        assertThrows(IllegalArgumentException.class,
                () -> RaftVoterParser.parse("2@broker-2:7002,2@other:7003"));
        assertThrows(IllegalArgumentException.class,
                () -> RaftVoterParser.parse("2@broker-2:not-a-port"));
    }

    @Test
    void hybridDirectoryEndpointDefaultsRemainAvailable() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft", "1", "1@broker-1:7001:51001"}, RaftTransportMode.HYBRID);
        assertEquals("localhost", endpoint.host());
        assertEquals(60000, endpoint.port());
    }

    @Test
    void hybridDirectoryEndpointIsReadAfterTransportOptions() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft", "1", "1@broker-1:7001:51001", "7100", "demo", "1300",
                        "192.0.2.10", "62000"}, RaftTransportMode.HYBRID);
        assertEquals("192.0.2.10", endpoint.host());
        assertEquals(62000, endpoint.port());
    }

    @Test
    void localTcpDirectoryEndpointDoesNotDependOnHybridOptions() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft-local", "2", "2@broker-2:7002:51002", "directory.lan", "62001"},
                RaftTransportMode.LOCAL_TCP);
        assertEquals("directory.lan", endpoint.host());
        assertEquals(62001, endpoint.port());
    }

    @Test
    void invalidDirectoryEndpointIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.directoryEndpointForArgs(
                new String[]{"raft-local", "2", "2@broker-2:7002:51002", "", "62001"},
                RaftTransportMode.LOCAL_TCP));
        assertThrows(IllegalArgumentException.class, () -> BrokerMain.directoryEndpointForArgs(
                new String[]{"raft-local", "2", "2@broker-2:7002:51002", "host", "0"},
                RaftTransportMode.LOCAL_TCP));
    }
}
