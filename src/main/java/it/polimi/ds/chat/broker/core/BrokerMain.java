package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.directory.GetClusterRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetClusterResponseMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Entry point for starting a Raft broker in the replicated chat infrastructure.
 *
 * <pre>
 *   java BrokerMain raft &lt;nodeId&gt; &lt;rpcPort&gt;
 *       [clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes]
 * </pre>
 *
 * The voter set (static cluster topology) is fetched from the DirectoryService
 * at startup via a GetClusterRequestMessage. Raft always uses the LAN-aware
 * hybrid transport: {@code RequestVote} and empty heartbeat traffic over UDP
 * LAN broadcast, log entries over TCP.
 */
public class BrokerMain {

    // Directory Service location (mirrors the constants used inside Broker).
    private static final String DIRECTORY_HOST = "localhost";
    private static final int DIRECTORY_PORT = 60000;

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: RAFT BROKER---");

        if (args.length > 0 && "raft".equalsIgnoreCase(args[0])) {
            startRaftMode(args);
            return;
        }

        printUsageAndExit();
    }

    private static void startRaftMode(String[] args) {
        if (args.length < 3) {
            printUsageAndExit();
        }

        int nodeId = Integer.parseInt(args[1]);
        int rpcPort = Integer.parseInt(args[2]);
        int clientPort = (args.length >= 4) ? Integer.parseInt(args[3]) : 50000 + nodeId;
        int raftBroadcastPort = (args.length >= 5)
                ? Integer.parseInt(args[4])
                : RaftConfig.DEFAULT_RAFT_BROADCAST_PORT;
        String clusterId = (args.length >= 6)
                ? args[5]
                : RaftConfig.DEFAULT_CLUSTER_ID;
        int udpMaxPayloadBytes = (args.length >= 7)
                ? Integer.parseInt(args[6])
                : RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES;

        Map<Integer, RaftPeerEndpoint> voters = fetchVotersFromDirectory(nodeId);
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

    /**
     * Contact the DirectoryService and retrieve the static cluster topology
     * (voter set) for this broker. Fails fast on any error: without a voter
     * set the broker cannot construct its RaftConfig.
     */
    private static Map<Integer, RaftPeerEndpoint> fetchVotersFromDirectory(int nodeId) {
        System.out.println("Fetching cluster voters from Directory Service at "
                + DIRECTORY_HOST + ":" + DIRECTORY_PORT + " for nodeId=" + nodeId + "...");

        try (Socket socket = new Socket(DIRECTORY_HOST, DIRECTORY_PORT);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            out.writeObject(new GetClusterRequestMessage(nodeId));
            out.flush();

            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                Object obj = in.readObject();
                if (!(obj instanceof GetClusterResponseMessage resp)) {
                    System.err.println("Unexpected response from Directory Service: " + obj);
                    System.exit(2);
                    return null; // unreachable
                }

                if (!resp.isOk() || resp.getVoters() == null || resp.getVoters().isEmpty()) {
                    System.err.println("Directory Service returned no cluster info for nodeId=" + nodeId
                            + " (resp=" + resp + ")");
                    System.exit(2);
                    return null; // unreachable
                }

                System.out.println("Received voters from Directory Service: " + resp.getVoters().keySet());
                return resp.getVoters();
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to fetch cluster voters from Directory Service: " + e.getMessage());
            System.exit(2);
            return null; // unreachable
        }
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: raft <nodeId> <rpcPort> "
                + "[clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes]");
        System.err.println("  Cluster voters are fetched from the DirectoryService at startup.");
        System.err.println("  transport: HYBRID only; RequestVote and empty heartbeats use UDP LAN broadcast.");
        System.exit(2);
    }
}