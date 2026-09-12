package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Entry point for starting a Raft broker in the replicated chat infrastructure.
 *
 * <pre>
 *   java BrokerMain raft &lt;nodeId&gt; &lt;votersCSV&gt;
 *       [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes]
 *       [directoryHost] [directoryPort]
 *   java BrokerMain raft-local &lt;nodeId&gt; &lt;votersCSV&gt;
 *       [directoryHost] [directoryPort]
 * </pre>
 *
 * The voter set is static and is supplied identically to every broker at
 * startup. Directory Service is deliberately not involved in Raft membership.
 * The default {@code raft} command
 * uses the LAN-aware hybrid transport: {@code RequestVote} and empty heartbeat
 * traffic over UDP LAN broadcast, log entries over TCP. The explicit
 * {@code raft-local} command is a development facility that sends every Raft
 * RPC via TCP unicast so multiple brokers can run on one host.
 */
public class BrokerMain {

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: RAFT BROKER---");

        RaftTransportMode transportMode = args.length == 0
                ? null
                : transportModeForCommand(args[0]);
        if (transportMode != null) {
            startRaftMode(args, transportMode);
            return;
        }

        printUsageAndExit();
    }

    private static void startRaftMode(String[] args, RaftTransportMode transportMode) {
        if (args.length < 3) {
            printUsageAndExit();
        }
        if (transportMode == RaftTransportMode.LOCAL_TCP && args.length > 5) {
            printUsageAndExit();
        }
        if (transportMode == RaftTransportMode.HYBRID && args.length > 8) {
            printUsageAndExit();
        }

        int nodeId = Integer.parseInt(args[1]);
        Map<Integer, RaftPeerEndpoint> voters = RaftVoterParser.parse(args[2]);
        RaftPeerEndpoint localEndpoint = voters.get(nodeId);
        if (localEndpoint == null) {
            throw new IllegalArgumentException(
                    "Local nodeId " + nodeId + " is not in the voter set " + voters.keySet());
        }
        int rpcPort = localEndpoint.rpcPort();
        int clientPort = localEndpoint.clientPort();
        int raftBroadcastPort = (transportMode == RaftTransportMode.HYBRID && args.length >= 4)
                ? Integer.parseInt(args[3])
                : RaftConfig.DEFAULT_RAFT_BROADCAST_PORT;
        String clusterId = (transportMode == RaftTransportMode.HYBRID && args.length >= 5)
                ? args[4]
                : RaftConfig.DEFAULT_CLUSTER_ID;
        int udpMaxPayloadBytes = (transportMode == RaftTransportMode.HYBRID && args.length >= 6)
                ? Integer.parseInt(args[5])
                : RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES;
        DirectoryEndpoint directoryEndpoint = directoryEndpointForArgs(args, transportMode);

        Path storageDir = Paths.get("raft-data", "n" + nodeId);

        RaftConfig raftConfig = new RaftConfig(
                1500,
                3000,
                100,
                rpcPort,
                transportMode,
                raftBroadcastPort,
                udpMaxPayloadBytes,
                clusterId,
                storageDir,
                voters);

        String brokerHost = localEndpoint.host();

        BrokerConfig config = new BrokerConfig(
                nodeId,
                brokerHost,
                clientPort,
                raftConfig,
                directoryEndpoint.host(),
                directoryEndpoint.port());

        System.out.println("Starting RAFT broker, nodeId=" + nodeId
                + ", rpcPort=" + rpcPort
                + ", clientPort=" + clientPort
                + ", transportMode=" + transportMode
                + (transportMode == RaftTransportMode.HYBRID
                    ? ", raftBroadcastPort=" + raftBroadcastPort
                        + ", clusterId=" + clusterId
                        + ", udpMaxPayloadBytes=" + udpMaxPayloadBytes
                    : "")
                + ", directory=" + directoryEndpoint.host() + ":" + directoryEndpoint.port()
                + ", voters=" + voters.keySet()
                + ", storageDir=" + storageDir.toAbsolutePath());

        Broker broker = new Broker(config);
        try (BrokerConsoleStatus consoleStatus = BrokerConsoleStatus.create()) {
            broker.setClientStatusOutput(consoleStatus);
            broker.start();
        } catch (IOException e) {
            System.err.println("Broker failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static RaftTransportMode transportModeForCommand(String command) {
        if ("raft".equalsIgnoreCase(command)) {
            return RaftConfig.DEFAULT_TRANSPORT_MODE;
        }
        if ("raft-local".equalsIgnoreCase(command)) {
            return RaftTransportMode.LOCAL_TCP;
        }
        return null;
    }

    static DirectoryEndpoint directoryEndpointForArgs(
            String[] args,
            RaftTransportMode transportMode) {
        int hostIndex = transportMode == RaftTransportMode.HYBRID ? 6 : 3;
        int portIndex = hostIndex + 1;

        String host = args.length > hostIndex
                ? args[hostIndex]
                : BrokerConfig.DEFAULT_DIRECTORY_HOST;
        int port = args.length > portIndex
                ? Integer.parseInt(args[portIndex])
                : BrokerConfig.DEFAULT_DIRECTORY_PORT;

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("directoryHost must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("directoryPort out of range: " + port);
        }
        return new DirectoryEndpoint(host, port);
    }

    record DirectoryEndpoint(String host, int port) {
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: raft <nodeId> <votersCSV> "
                + "[raftBroadcastPort] [clusterId] [udpMaxPayloadBytes] "
                + "[directoryHost] [directoryPort]");
        System.err.println("   or: raft-local <nodeId> <votersCSV> "
                + "[directoryHost] [directoryPort]");
        System.err.println("  votersCSV: id@host:rpcPort[:clientPort],id@host:rpcPort[:clientPort],...");
        System.err.println("  The local RPC port, client port, and host come from its voter endpoint.");
        System.err.println("  raft: HYBRID (default); RequestVote and empty heartbeats use UDP LAN broadcast.");
        System.err.println("  raft-local: local development only; all Raft RPCs use TCP unicast.");
        System.exit(2);
    }
}
