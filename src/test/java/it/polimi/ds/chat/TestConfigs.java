package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public final class TestConfigs {

    private TestConfigs() {
    }

    public static BrokerConfig raftBrokerConfig(int brokerId, int clientPort) {
        try {
            Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
            voters.put(0, new RaftPeerEndpoint(0, "localhost", 7000, 5000));
            voters.put(1, new RaftPeerEndpoint(1, "localhost", 7001, 5002));
            voters.put(2, new RaftPeerEndpoint(2, "localhost", 7002, 5003));

            RaftConfig raftConfig = new RaftConfig(
                    150,
                    300,
                    30,
                    7000 + brokerId,
                    Files.createTempDirectory("raft-test-n" + brokerId),
                    voters);

            return new BrokerConfig(
                    brokerId,
                    "localhost",
                    clientPort,
                    clientPort,
                    raftConfig);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create test Raft config", e);
        }
    }
}
