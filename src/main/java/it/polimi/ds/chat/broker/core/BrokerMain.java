package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry point for starting a Raft broker in the replicated chat infrastructure.
 *
 * <pre>
 *   java BrokerMain raft &lt;nodeId&gt; &lt;rpcPort&gt; &lt;votersCSV&gt;
 *       [clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes]
 * </pre>
 *
 * {@code votersCSV} is a comma-separated list of voter endpoints in the form
 * {@code id@host:rpcPort[:clientPort]}, identical on every node of the cluster.
 * Raft always uses the LAN-aware hybrid transport: {@code RequestVote} and
 * empty heartbeat traffic over UDP LAN broadcast, log entries over TCP.
 */
public class BrokerMain {

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: RAFT BROKER---");

        if (args.length > 0 && "raft".equalsIgnoreCase(args[0])) {
            startRaftMode(args);
            return;
        }

        printUsageAndExit();
    }

    private static void startRaftMode(String[] args) {
        if (args.length < 4) {
            printUsageAndExit();
        }

        int nodeId = Integer.parseInt(args[1]);
        int rpcPort = Integer.parseInt(args[2]);
        String votersCsv = args[3];
        int clientPort = (args.length >= 5) ? Integer.parseInt(args[4]) : 50000 + nodeId;
        int raftBroadcastPort = (args.length >= 6)
                ? Integer.parseInt(args[5])
                : RaftConfig.DEFAULT_RAFT_BROADCAST_PORT;
        String clusterId = (args.length >= 7)
                ? args[6]
                : RaftConfig.DEFAULT_CLUSTER_ID;
        int udpMaxPayloadBytes = (args.length >= 8)
                ? Integer.parseInt(args[7])
                : RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES;

        Map<Integer, RaftPeerEndpoint> voters = parseVoters(votersCsv);
        if (!voters.containsKey(nodeId)) {
            System.err.println("Local nodeId " + nodeId + " is not in the voter set " + voters.keySet());
            System.exit(2);
        }

        Path storageDir = Paths.get("raft-data", "n" + nodeId);

        RaftConfig raftConfig = new RaftConfig(
                200,
                400,
                40,
                rpcPort,
                RaftConfig.DEFAULT_TRANSPORT_MODE,
                raftBroadcastPort,
                udpMaxPayloadBytes,
                clusterId,
                storageDir,
                voters);

        String brokerHost = voters.get(nodeId).host();

        BrokerConfig config = new BrokerConfig(
                nodeId,
                brokerHost,
                clientPort,
                clientPort,
                50002 + nodeId,
                raftConfig);

        System.out.println("Starting RAFT broker, nodeId=" + nodeId
                + ", rpcPort=" + rpcPort
                + ", clientPort=" + clientPort
                + ", transportMode=" + RaftConfig.DEFAULT_TRANSPORT_MODE
                + ", raftBroadcastPort=" + raftBroadcastPort
                + ", clusterId=" + clusterId
                + ", udpMaxPayloadBytes=" + udpMaxPayloadBytes
                + ", voters=" + voters.keySet()
                + ", storageDir=" + storageDir.toAbsolutePath());

        Broker broker = new Broker(config);
        try {
            broker.start();
        } catch (IOException e) {
            System.err.println("Broker failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<Integer, RaftPeerEndpoint> parseVoters(String csv) {
        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        for (String token : csv.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) continue;

            int at = trimmed.indexOf('@');
            if (at <= 0) {
                throw new IllegalArgumentException("Bad voter token: '" + trimmed
                        + "' (expected id@host:rpcPort[:clientPort])");
            }

            int id = Integer.parseInt(trimmed.substring(0, at));
            String endpoint = trimmed.substring(at + 1);
            String[] parts = endpoint.split(":");

            if (parts.length != 2 && parts.length != 3) {
                throw new IllegalArgumentException("Bad voter token: '" + trimmed
                        + "' (expected id@host:rpcPort[:clientPort])");
            }

            String host = parts[0];
            int rpcPort = Integer.parseInt(parts[1]);
            int clientPort = (parts.length == 3)
                    ? Integer.parseInt(parts[2])
                    : 50000 + id;

            voters.put(id, new RaftPeerEndpoint(id, host, rpcPort, clientPort));
        }
        return voters;
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: raft <nodeId> <rpcPort> <votersCSV> "
                + "[clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes]");
        System.err.println("  votersCSV: id@host:rpcPort[:clientPort],id@host:rpcPort[:clientPort],...");
        System.err.println("  transport: HYBRID only; RequestVote and empty heartbeats use UDP LAN broadcast.");
        System.exit(2);
    }
}
