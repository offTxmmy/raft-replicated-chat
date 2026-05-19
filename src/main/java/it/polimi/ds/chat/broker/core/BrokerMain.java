package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.config.OrderingMode;
import it.polimi.ds.chat.broker.session.HandlerState;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.Scanner;

/**
 * Entry point for starting a broker in the replicated chat infrastructure.
 *
 * <p>Two modes are supported, selected by the first CLI argument:
 *
 * <h4>Sequencer mode (legacy, default)</h4>
 * <pre>
 *   java BrokerMain [first]
 * </pre>
 * The optional {@code first} flag designates this broker as the sequencer
 * (leader). The remaining configuration is collected interactively.
 *
 * <h4>Raft mode</h4>
 * <pre>
 *   java BrokerMain raft &lt;nodeId&gt; &lt;rpcPort&gt; &lt;votersCSV&gt; [clientPort]
 * </pre>
 * where {@code votersCSV} is a comma-separated list of voter endpoints in
 * the form {@code id@host:rpcPort[:clientPort]}, identical on every node of the cluster.
 * Example:
 * <pre>
 *   java BrokerMain raft 0 7000 0@127.0.0.1:7000:50000,1@127.0.0.1:7001:50001,2@127.0.0.1:7002:50002 50000
 * </pre>
 * Storage directory defaults to {@code ./raft-data/n&lt;nodeId&gt;}.
 */
public class BrokerMain {

    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: BROKER---");

        if (args.length > 0 && "raft".equalsIgnoreCase(args[0])) {
            startRaftMode(args);
        } else {
            startSequencerMode(args);
        }
    }

    // =========================================================================
    // RAFT MODE
    // =========================================================================

    private static void startRaftMode(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: raft <nodeId> <rpcPort> <votersCSV> [clientPort]");
            System.err.println("  votersCSV: id@host:rpcPort[:clientPort],id@host:rpcPort[:clientPort],...");
            System.exit(2);
        }

        int nodeId      = Integer.parseInt(args[1]);
        int rpcPort     = Integer.parseInt(args[2]);
        String votersCsv = args[3];
        int clientPort  = (args.length >= 5) ? Integer.parseInt(args[4]) : 50000 + nodeId;

        Map<Integer, RaftPeerEndpoint> voters = parseVoters(votersCsv);
        if (!voters.containsKey(nodeId)) {
            System.err.println("Local nodeId " + nodeId + " is not in the voter set " + voters.keySet());
            System.exit(2);
        }

        Path storageDir = Paths.get("raft-data", "n" + nodeId);

        RaftConfig raftConfig = new RaftConfig(
                /* electionTimeoutMinMs */ 200,
                /* electionTimeoutMaxMs */ 400,
                /* heartbeatIntervalMs  */ 40,
                rpcPort,
                storageDir,
                voters);

        String brokerHost = voters.get(nodeId).host();

        BrokerConfig config = new BrokerConfig(
                nodeId,
                /* isSequencer */ false,
                brokerHost,
                clientPort,
                clientPort,
                /* sequencerHost */ brokerHost,
                /* sequencerPort */ 0,
                /* udpPort       */ 50002 + nodeId,
                /* handlerState  */ null,
                OrderingMode.RAFT,
                raftConfig);

        System.out.println("Starting RAFT broker, nodeId=" + nodeId
                + ", rpcPort=" + rpcPort
                + ", clientPort=" + clientPort
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
     * Parses a voter list of the form {@code id@host:port,id@host:port,...}.
     */
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

    // =========================================================================
    // SEQUENCER MODE (legacy)
    // =========================================================================

    private static void startSequencerMode(String[] args) {
        boolean isSequencer = args.length > 0 && "first".equalsIgnoreCase(args[0]);

        String brokerIp = takeBrokerIp();
        int brokerPort = askForBrokerPort();

        int clientPort = brokerPort;
        int sequencerPort = 50001;
        int udpPort = 50002;

        String sequencerHost;
        HandlerState handlerState = null;
        int brokerId;

        if (isSequencer) {
            handlerState = new HandlerState();
            brokerId = handlerState.getNewBrokerId();
            sequencerHost = brokerIp;
            System.out.println("Starting as SEQUENCER with brokerId = " + brokerId);
            System.out.println("Sequencer address for other brokers: "
                    + sequencerHost + ":" + sequencerPort);
        } else {
            brokerId = -1;
            sequencerHost = askForSequencerHost();
            System.out.println("Starting as FOLLOWER, waiting for ID from sequencer at "
                    + sequencerHost + ":" + sequencerPort);
        }

        BrokerConfig config = new BrokerConfig(
                brokerId,
                isSequencer,
                brokerIp,
                brokerPort,
                clientPort,
                sequencerHost,
                sequencerPort,
                udpPort,
                handlerState
        );

        Broker broker = new Broker(config);
        try {
            broker.start();
        } catch (IOException e) {
            System.err.println("Broker failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static String takeBrokerIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    public static Integer askForBrokerPort() {
        Integer brokerPort = null;
        while (brokerPort == null) {
            System.out.println("Insert a port for the broker to listen on for clients:");
            try {
                int port = scanner.nextInt();
                if (port < 1024 || port > 65535) {
                    System.out.println("Please enter a valid port number between 1024 and 65535.");
                } else {
                    brokerPort = port;
                }
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a numeric port number.");
            }
            scanner.nextLine();
        }
        return brokerPort;
    }

    private static String askForSequencerHost() {
        System.out.println("Insert sequencer host (press ENTER for localhost):");
        String line = scanner.nextLine().trim();
        return line.isEmpty() ? "localhost" : line;
    }
}
