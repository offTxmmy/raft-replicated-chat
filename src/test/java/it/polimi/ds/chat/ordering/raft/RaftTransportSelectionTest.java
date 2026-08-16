package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class RaftTransportSelectionTest {

    @Test
    void localTcpReusesTcpClientWithoutCreatingUdpTransport(@TempDir Path storageDir) {
        RaftOrderingService service = service(RaftTransportMode.LOCAL_TCP, storageDir);
        RaftRpcClient tcpClient = new RaftRpcClient(0, voters());

        RaftTransport selected = service.createRaftTransport(tcpClient);

        assertSame(tcpClient, selected);
    }

    @Test
    void hybridStillBuildsHybridTransport(@TempDir Path storageDir) {
        RaftOrderingService service = service(RaftTransportMode.HYBRID, storageDir);
        RaftRpcClient tcpClient = new RaftRpcClient(0, voters());

        RaftTransport selected = service.createRaftTransport(tcpClient);

        assertInstanceOf(RaftHybridTransport.class, selected);
    }

    private static RaftOrderingService service(RaftTransportMode mode, Path storageDir) {
        RaftConfig raft = new RaftConfig(
                150,
                300,
                30,
                61000,
                mode,
                RaftConfig.DEFAULT_RAFT_BROADCAST_PORT,
                RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES,
                RaftConfig.DEFAULT_CLUSTER_ID,
                storageDir,
                voters());
        BrokerConfig broker = new BrokerConfig(0, "127.0.0.1", 50000, 50000, raft);
        return new RaftOrderingService(broker);
    }

    private static Map<Integer, RaftPeerEndpoint> voters() {
        return Map.of(
                0, new RaftPeerEndpoint(0, "127.0.0.1", 61000, 50000),
                1, new RaftPeerEndpoint(1, "127.0.0.1", 61001, 50001),
                2, new RaftPeerEndpoint(2, "127.0.0.1", 61002, 50002));
    }
}
