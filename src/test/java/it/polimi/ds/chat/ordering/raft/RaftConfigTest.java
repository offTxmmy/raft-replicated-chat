package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftConfigTest {

    private static Map<Integer, RaftPeerEndpoint> threeVoters() {
        Map<Integer, RaftPeerEndpoint> v = new HashMap<>();
        v.put(0, new RaftPeerEndpoint(0, "host0", 7000, 50000));
        v.put(1, new RaftPeerEndpoint(1, "host1", 7001, 50001));
        v.put(2, new RaftPeerEndpoint(2, "host2", 7002, 50002));
        return v;
    }

    @Test
    void validRaftConfigBuilds(@TempDir Path dir) {
        RaftConfig cfg = new RaftConfig(150, 300, 30, 7000, dir, threeVoters());

        assertEquals(150, cfg.getElectionTimeoutMinMs());
        assertEquals(300, cfg.getElectionTimeoutMaxMs());
        assertEquals(30, cfg.getHeartbeatIntervalMs());
        assertEquals(7000, cfg.getRpcPort());
        assertEquals(RaftConfig.DEFAULT_TRANSPORT_MODE, cfg.getTransportMode());
        assertEquals(RaftConfig.DEFAULT_RAFT_BROADCAST_PORT, cfg.getRaftBroadcastPort());
        assertEquals(RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES, cfg.getUdpMaxPayloadBytes());
        assertEquals(RaftConfig.DEFAULT_CLUSTER_ID, cfg.getClusterId());
        assertSame(dir, cfg.getStorageDir());
        assertEquals(3, cfg.getVoters().size());
        assertEquals(2, cfg.getQuorumSize());
    }

    @Test
    void fullRaftConfigBuildsWithHybridTransport(@TempDir Path dir) {
        RaftConfig cfg = new RaftConfig(
                150,
                300,
                30,
                7000,
                RaftTransportMode.HYBRID,
                7101,
                1200,
                "test-cluster",
                dir,
                threeVoters());

        assertEquals(RaftTransportMode.HYBRID, cfg.getTransportMode());
        assertEquals(7101, cfg.getRaftBroadcastPort());
        assertEquals(1200, cfg.getUdpMaxPayloadBytes());
        assertEquals("test-cluster", cfg.getClusterId());
        assertSame(dir, cfg.getStorageDir());
        assertEquals(3, cfg.getVoters().size());
    }

    @Test
    void quorumOnFiveNodes(@TempDir Path dir) {
        Map<Integer, RaftPeerEndpoint> v = threeVoters();
        v.put(3, new RaftPeerEndpoint(3, "host3", 7003, 50003));
        v.put(4, new RaftPeerEndpoint(4, "host4", 7004, 50004));

        RaftConfig cfg = new RaftConfig(150, 300, 30, 7000, dir, v);
        assertEquals(3, cfg.getQuorumSize());
    }

    @Test
    void votersMapIsUnmodifiable(@TempDir Path dir) {
        RaftConfig cfg = new RaftConfig(150, 300, 30, 7000, dir, threeVoters());
        assertThrows(UnsupportedOperationException.class,
                () -> cfg.getVoters().put(99, new RaftPeerEndpoint(99, "x", 9000, 50099)));
    }

    @Test
    void mismatchedVoterIdRejected(@TempDir Path dir) {
        Map<Integer, RaftPeerEndpoint> bad = new HashMap<>();
        bad.put(0, new RaftPeerEndpoint(7, "host0", 7000, 50007)); // key 0 vs endpoint id 7

        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(150, 300, 30, 7000, dir, bad));
    }

    @Test
    void emptyVotersRejected(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(150, 300, 30, 7000, dir, new HashMap<>()));
    }

    @Test
    void heartbeatNotSmallerThanElectionTimeoutRejected(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(150, 300, 150, 7000, dir, threeVoters()));
    }

    @Test
    void electionTimeoutMaxSmallerThanMinRejected(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(300, 150, 30, 7000, dir, threeVoters()));
    }

    @Test
    void invalidRaftTransportSettingsRejected(@TempDir Path dir) {
        assertThrows(NullPointerException.class,
                () -> new RaftConfig(150, 300, 30, 7000,
                        null, 7100, 1400, "cluster", dir, threeVoters()));

        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(150, 300, 30, 7000,
                        RaftTransportMode.HYBRID, 0, 1400, "cluster", dir, threeVoters()));

        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(150, 300, 30, 7000,
                        RaftTransportMode.HYBRID, 7100, 0, "cluster", dir, threeVoters()));

        assertThrows(IllegalArgumentException.class,
                () -> new RaftConfig(150, 300, 30, 7000,
                        RaftTransportMode.HYBRID, 7100, 1400, " ", dir, threeVoters()));
    }

    @Test
    void raftBrokerConfigCarriesRaftConfig(@TempDir Path dir) {
        RaftConfig raft = new RaftConfig(150, 300, 30, 7000, dir, threeVoters());
        BrokerConfig cfg = new BrokerConfig(
                0,
                "127.0.0.1",
                50000,
                50000,
                raft);

        assertNotNull(cfg.getRaftConfig());
        assertEquals(0, cfg.getBrokerId());
        assertEquals("127.0.0.1", cfg.getBrokerHost());
        assertEquals(50000, cfg.getBrokerPort());
        assertEquals(50000, cfg.getClientPort());
        assertEquals("localhost", cfg.getDirectoryHost());
        assertEquals(60000, cfg.getDirectoryPort());
        assertEquals(2, cfg.getRaftConfig().getQuorumSize());
        assertTrue(cfg.getRaftConfig().getVoters().containsKey(0));
    }

    @Test
    void brokerConfigCarriesRemoteDirectoryEndpoint(@TempDir Path dir) {
        RaftConfig raft = new RaftConfig(150, 300, 30, 7000, dir, threeVoters());
        BrokerConfig cfg = new BrokerConfig(
                0,
                "127.0.0.1",
                50000,
                50000,
                raft,
                "192.0.2.50",
                62000);

        assertEquals("192.0.2.50", cfg.getDirectoryHost());
        assertEquals(62000, cfg.getDirectoryPort());
    }

    @Test
    void fullRaftConfigBuildsWithLocalTcpTransport(@TempDir Path dir) {
        RaftConfig cfg = new RaftConfig(
                150,
                300,
                30,
                7000,
                RaftTransportMode.LOCAL_TCP,
                RaftConfig.DEFAULT_RAFT_BROADCAST_PORT,
                RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES,
                RaftConfig.DEFAULT_CLUSTER_ID,
                dir,
                threeVoters());

        assertEquals(RaftTransportMode.LOCAL_TCP, cfg.getTransportMode());
    }

    @Test
    void brokerConfigRejectsMissingRaftConfig() {
        assertThrows(NullPointerException.class, () -> new BrokerConfig(
                0,
                "127.0.0.1",
                50000,
                50000,
                null));
    }
}
