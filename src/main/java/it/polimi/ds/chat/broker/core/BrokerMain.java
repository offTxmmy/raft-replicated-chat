package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.protocol.directory.GetClusterRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetClusterResponseMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
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
 *       [directoryHost] [directoryPort]
 *   java BrokerMain raft-local &lt;nodeId&gt; &lt;rpcPort&gt;
 *       [clientPort] [directoryHost] [directoryPort]
 * </pre>
 *
 * The voter set (static cluster topology) is fetched from the DirectoryService
 * at startup via a GetClusterRequestMessage. The default {@code raft} command
 * uses the LAN-aware hybrid transport: {@code RequestVote} and empty heartbeat
 * traffic over UDP LAN broadcast, log entries over TCP. The explicit
 * {@code raft-local} command is a development facility that sends every Raft
 * RPC via TCP unicast so multiple brokers can run on one host.
 */
public class BrokerMain {

    private static final int DIRECTORY_CONNECT_TIMEOUT_MS = 1_000;
    private static final int DIRECTORY_RESPONSE_TIMEOUT_MS = 2_000;

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
        if (transportMode == RaftTransportMode.LOCAL_TCP && args.length > 6) {
            printUsageAndExit();
        }
        if (transportMode == RaftTransportMode.HYBRID && args.length > 9) {
            printUsageAndExit();
        }

        int nodeId = Integer.parseInt(args[1]);
        int rpcPort = Integer.parseInt(args[2]);
        Integer clientPortOverride = (args.length >= 4)
                ? Integer.parseInt(args[3])
                : null;
        int raftBroadcastPort = (transportMode == RaftTransportMode.HYBRID && args.length >= 5)
                ? Integer.parseInt(args[4])
                : RaftConfig.DEFAULT_RAFT_BROADCAST_PORT;
        String clusterId = (transportMode == RaftTransportMode.HYBRID && args.length >= 6)
                ? args[5]
                : RaftConfig.DEFAULT_CLUSTER_ID;
        int udpMaxPayloadBytes = (transportMode == RaftTransportMode.HYBRID && args.length >= 7)
                ? Integer.parseInt(args[6])
                : RaftConfig.DEFAULT_UDP_MAX_PAYLOAD_BYTES;
        DirectoryEndpoint directoryEndpoint = directoryEndpointForArgs(args, transportMode);

        Map<Integer, RaftPeerEndpoint> voters;
        try {
            voters = fetchVotersFromDirectory(
                    nodeId,
                    directoryEndpoint.host(),
                    directoryEndpoint.port());
        } catch (IOException e) {
            System.err.println("Failed to fetch cluster voters from Directory Service: " + e.getMessage());
            System.exit(2);
            return;
        }
        if (!voters.containsKey(nodeId)) {
            System.err.println("Local nodeId " + nodeId + " is not in the voter set " + voters.keySet());
            System.exit(2);
        }
        validateRpcPort(nodeId, rpcPort, voters);
        int clientPort = resolveClientPort(nodeId, clientPortOverride, voters);

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

        String brokerHost = voters.get(nodeId).host();

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
        try {
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

    /**
     * Contact the DirectoryService and retrieve the static cluster topology
     * (voter set) for this broker. Fails fast on any error: without a voter
     * set the broker cannot construct its RaftConfig.
     */
    static Map<Integer, RaftPeerEndpoint> fetchVotersFromDirectory(
            int nodeId,
            String directoryHost,
            int directoryPort) throws IOException {
        return fetchVotersFromDirectory(
                nodeId,
                directoryHost,
                directoryPort,
                DIRECTORY_CONNECT_TIMEOUT_MS,
                DIRECTORY_RESPONSE_TIMEOUT_MS
        );
    }

    static Map<Integer, RaftPeerEndpoint> fetchVotersFromDirectory(
            int nodeId,
            String directoryHost,
            int directoryPort,
            int connectTimeoutMs,
            int responseTimeoutMs) throws IOException {
        if (connectTimeoutMs <= 0 || responseTimeoutMs <= 0) {
            throw new IllegalArgumentException("Directory timeouts must be positive");
        }
        System.out.println("Fetching cluster voters from Directory Service at "
                + directoryHost + ":" + directoryPort + " for nodeId=" + nodeId + "...");

        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(directoryHost, directoryPort),
                    connectTimeoutMs
            );
            socket.setSoTimeout(responseTimeoutMs);

            try (ObjectOutputStream out =
                         new ObjectOutputStream(socket.getOutputStream())) {

                out.writeObject(new GetClusterRequestMessage(nodeId));
                out.flush();

                try (ObjectInputStream in =
                             new ObjectInputStream(socket.getInputStream())) {
                    Object obj = in.readObject();
                    if (!(obj instanceof GetClusterResponseMessage resp)) {
                        throw new IOException("Unexpected response from Directory Service: " + obj);
                    }

                    if (!resp.isOk() || resp.getVoters() == null
                            || resp.getVoters().isEmpty()) {
                        throw new IOException("Directory Service returned no cluster info for nodeId="
                                + nodeId + " (resp=" + resp + ")");
                    }

                    System.out.println("Received voters from Directory Service: "
                            + resp.getVoters().keySet());
                    return resp.getVoters();
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Invalid cluster response from Directory Service", e);
        }
    }

    static DirectoryEndpoint directoryEndpointForArgs(
            String[] args,
            RaftTransportMode transportMode) {
        int hostIndex = transportMode == RaftTransportMode.HYBRID ? 7 : 4;
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

    static int resolveClientPort(
            int nodeId,
            Integer cliOverride,
            Map<Integer, RaftPeerEndpoint> voters) {
        if (cliOverride != null) {
            if (cliOverride < 1 || cliOverride > 65535) {
                throw new IllegalArgumentException(
                        "clientPort out of range: " + cliOverride);
            }
            return cliOverride;
        }

        RaftPeerEndpoint localEndpoint = voters.get(nodeId);
        if (localEndpoint == null) {
            throw new IllegalArgumentException(
                    "Local nodeId " + nodeId + " is not in the voter set");
        }
        return localEndpoint.clientPort();
    }

    static void validateRpcPort(
            int nodeId,
            int cliRpcPort,
            Map<Integer, RaftPeerEndpoint> voters
    ) {
        RaftPeerEndpoint localEndpoint = voters.get(nodeId);
        if (localEndpoint == null) {
            throw new IllegalArgumentException(
                    "Local nodeId " + nodeId + " is not in the voter set");
        }
        if (cliRpcPort != localEndpoint.rpcPort()) {
            throw new IllegalArgumentException(
                    "rpcPort " + cliRpcPort + " does not match voter endpoint "
                            + localEndpoint.host() + ":" + localEndpoint.rpcPort()
                            + " for nodeId " + nodeId);
        }
    }

    record DirectoryEndpoint(String host, int port) {
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: raft <nodeId> <rpcPort> "
                + "[clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes] "
                + "[directoryHost] [directoryPort]");
        System.err.println("   or: raft-local <nodeId> <rpcPort> "
                + "[clientPort] [directoryHost] [directoryPort]");
        System.err.println("  Cluster voters are fetched from the DirectoryService at startup.");
        System.err.println("  raft: HYBRID (default); RequestVote and empty heartbeats use UDP LAN broadcast.");
        System.err.println("  raft-local: local development only; all Raft RPCs use TCP unicast.");
        System.exit(2);
    }
}
