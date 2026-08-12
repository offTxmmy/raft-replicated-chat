package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
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
        assertEquals(RaftTransportMode.LOCAL_TCP,
                BrokerMain.transportModeForCommand("raft-local"));
    }

    @Test
    void unknownCommandIsRejected() {
        assertNull(BrokerMain.transportModeForCommand("unknown"));
    }

    @Test
    void hybridDirectoryEndpointDefaultsRemainBackwardsCompatible() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft", "1", "7001"},
                RaftTransportMode.HYBRID);

        assertEquals("localhost", endpoint.host());
        assertEquals(60000, endpoint.port());
    }

    @Test
    void hybridDirectoryEndpointIsReadAfterTransportOptions() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{
                        "raft", "1", "7001", "51001", "7100", "demo", "1300",
                        "192.0.2.10", "62000"
                },
                RaftTransportMode.HYBRID);

        assertEquals("192.0.2.10", endpoint.host());
        assertEquals(62000, endpoint.port());
    }

    @Test
    void localTcpDirectoryEndpointDoesNotDependOnHybridOptions() {
        BrokerMain.DirectoryEndpoint endpoint = BrokerMain.directoryEndpointForArgs(
                new String[]{"raft-local", "2", "7002", "51002", "directory.lan", "62001"},
                RaftTransportMode.LOCAL_TCP);

        assertEquals("directory.lan", endpoint.host());
        assertEquals(62001, endpoint.port());
    }

    @Test
    void invalidDirectoryEndpointIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                BrokerMain.directoryEndpointForArgs(
                        new String[]{"raft-local", "2", "7002", "51002", "", "62001"},
                        RaftTransportMode.LOCAL_TCP));
        assertThrows(IllegalArgumentException.class, () ->
                BrokerMain.directoryEndpointForArgs(
                        new String[]{"raft-local", "2", "7002", "51002", "host", "0"},
                        RaftTransportMode.LOCAL_TCP));
    }

    @Test
    void omittedClientPortUsesDirectoryVoterEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7002, 52345));

        assertEquals(52345, BrokerMain.resolveClientPort(2, null, voters));
    }

    @Test
    void explicitClientPortOverridesDirectoryVoterEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7002, 52345));

        assertEquals(53333, BrokerMain.resolveClientPort(2, 53333, voters));
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.resolveClientPort(2, 0, voters));
    }

    @Test
    void rpcPortMustMatchTheStaticVoterEndpoint() {
        Map<Integer, RaftPeerEndpoint> voters = Map.of(
                2, new RaftPeerEndpoint(2, "broker-2", 7002, 52345));

        BrokerMain.validateRpcPort(2, 7002, voters);
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.validateRpcPort(2, 7999, voters));
    }
}
