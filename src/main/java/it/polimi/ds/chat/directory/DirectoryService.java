package it.polimi.ds.chat.directory;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.broker.*;
import it.polimi.ds.chat.protocol.chat.*;
import it.polimi.ds.chat.protocol.client.*;
import it.polimi.ds.chat.protocol.directory.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DirectoryService manages broker registration, heartbeats, and client/broker lookup requests.
 * It listens for broker and client connections, maintains broker liveness, and provides peer information.
 */
public class DirectoryService {

    private static final long HEARTBEAT_TIMEOUT_MS = 10_000;
    private static final long REAPER_INTERVAL_MS   = 5_000;

    // broker configuration + client count
    private final Map<BrokerConfig, Integer> registeredBrokers = new ConcurrentHashMap<>();
    // last heartbeat time per brokerId
    private final Map<Integer, Long> lastHeartbeats = new ConcurrentHashMap<>();
    // index by brokerId for fast lookup
    private final Map<Integer, BrokerConfig> brokersById = new ConcurrentHashMap<>();

    // Static cluster topology (id -> endpoint). Configured at startup, served to brokers on request.
    private final Map<Integer, RaftPeerEndpoint> clusterVoters;

    /**
     * Main entry point for the Directory Service.
     * Starts listeners for broker and client connections.
     *
     * @param args [0] = votersCSV (id@host:rpcPort[:clientPort],...)
     */
    public static void main(String[] args) {
        int brokerPort = 60000;
        int clientPort = 60001;

        if (args.length < 1) {
            System.err.println("Usage: DirectoryService <votersCSV>");
            System.err.println("  votersCSV: id@host:rpcPort[:clientPort],id@host:rpcPort[:clientPort],...");
            System.exit(2);
        }

        Map<Integer, RaftPeerEndpoint> voters = parseVoters(args[0]);

        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: DIRECTORY SERVICE---");
        System.out.println("Configured cluster voters: " + voters.keySet());

        DirectoryService service = new DirectoryService(voters);

        // Lister on brokerPort (register + heartbeat
        new Thread(() -> service.startBrokersListener(brokerPort), "Dir-BrokerListener").start();

        // Listen on clientPort (GET_BROKER)
        new Thread(() -> service.startClientsListener(clientPort), "Dir-ClientListener").start();
    }

    /**
     * Constructs a DirectoryService with a preconfigured static cluster topology.
     *
     * @param clusterVoters static voter set served to brokers via GetClusterRequest
     */
    public DirectoryService(Map<Integer, RaftPeerEndpoint> clusterVoters) {
        this.clusterVoters = (clusterVoters == null)
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(clusterVoters));
        startReaperThread();
    }

    /**
     * Backwards-compatible constructor: no preconfigured cluster topology.
     * Brokers requesting the cluster will receive a negative response.
     */
    public DirectoryService() {
        this(Collections.emptyMap());
    }

    /**
     * Starts a TCP listener for broker connections (registration and heartbeat).
     *
     * @param port the port to listen on for broker connections
     */
    public void startBrokersListener(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Directory Service listening on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New connection from " + socket.getRemoteSocketAddress());

                new Thread(() -> handleConnection(socket)).start();
            }
        } catch (IOException e) {
            System.err.println("Directory Service error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Starts a TCP listener for client connections (broker lookup requests).
     *
     * @param port the port to listen on for client connections
     */
    public void startClientsListener(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Directory Service listening for CLIENTS on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New client directory request from " + socket.getRemoteSocketAddress());

                new Thread(() -> handleClientConnection(socket)).start();
            }
        } catch (IOException e) {
            System.err.println("Directory Service client listener error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Handles a client connection, processing broker or peer list requests.
     *
     * @param socket the client socket
     */
    private void handleClientConnection(Socket socket) {
        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            Object obj = in.readObject();

            if (obj instanceof GetBrokerRequestMessage req) {
                handleGetBrokerRequest(out);
            } else {
                System.out.println("Unknown client request object: " + obj);
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error handling client directory request: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Handles a client request for the best broker and sends a response.
     *
     * @param out the output stream to the client
     * @throws IOException if an I/O error occurs
     */
    private void handleGetBrokerRequest(ObjectOutputStream out) throws IOException {
        BrokerConfig best = chooseBestBroker();

        GetBrokerResponseMessage resp;
        if (best != null) {
            resp = new GetBrokerResponseMessage(
                    true,
                    best.getBrokerHost(),
                    best.getBrokerPort(),
                    best.getBrokerId()
            );
            System.out.println("Returned broker " + best.getBrokerId() +
                    " (" + best.getBrokerHost() + ":" + best.getBrokerPort() + ") to client");
        } else {
            resp = new GetBrokerResponseMessage(false, null, -1, -1);
            System.out.println("No brokers available to client request");
        }

        out.writeObject(resp);
        out.flush();
    }

    /**
     * Handles a broker connection, processing registration and heartbeats.
     * Also handles one-shot GetClusterRequestMessage queries used at broker startup.
     *
     * @param socket the broker socket
     */
    private void handleConnection(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Expected first message: DirectoryRegisterMessage OR GetClusterRequestMessage
            Object first = in.readObject();

            if (first instanceof GetClusterRequestMessage req) {
                handleGetClusterRequest(socket, req);
                return;
            }

            if (!(first instanceof DirectoryRegisterMessage msg)) {
                System.out.println("Unknown first object from " + socket.getRemoteSocketAddress() + ": " + first);
                return;
            }

            registerBroker(msg);
            int brokerId = msg.getBrokerId();

            // Loop: receive HeartbeatMessage on the same connection
            while (true) {
                Object obj = in.readObject();
                if (obj instanceof HeartbeatMessage hb) {
                    // update last heartbeat time using the local receive clock; the
                    // heartbeat payload now carries a monotonic sequence number
                    // (not a wall-clock value), so we can't subtract it from `now`.
                    lastHeartbeats.put(brokerId, System.currentTimeMillis());
                    // System.out.println("Heartbeat from broker " + brokerId);
                } else if (obj instanceof ClientCountUpdateMessage cc) {
                    updateClientCount(cc);
                } else {
                    System.out.println("Unknown object from broker " + brokerId + ": " + obj);
                }
            }

        } catch (IOException e) {
            System.err.println("Connection with broker died: " + e.getMessage());
            // The reaper will clean up based on heartbeat timeout
        } catch (ClassNotFoundException e) {
            System.err.println("Unknown class from broker: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Handles a broker's startup request for the static cluster topology.
     * Writes a GetClusterResponseMessage and closes the connection.
     */
    private void handleGetClusterRequest(Socket socket, GetClusterRequestMessage req) throws IOException {
        int nodeId = req.getNodeId();
        boolean ok = !clusterVoters.isEmpty() && clusterVoters.containsKey(nodeId);

        GetClusterResponseMessage resp = new GetClusterResponseMessage(
                ok,
                ok ? clusterVoters : Collections.emptyMap()
        );

        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(resp);
        out.flush();

        System.out.println("Served cluster info to broker nodeId=" + nodeId
                + " (ok=" + ok + ", voters=" + clusterVoters.keySet() + ")");
    }

    /**
     * Updates the client count for a broker based on a received message.
     *
     * @param cc the client count update message
     */
    public void updateClientCount(ClientCountUpdateMessage cc) {
        int brokerId = cc.getBrokerId();
        int clientCount = cc.getClientCount();

        BrokerConfig cfg = brokersById.get(brokerId);
        if (cfg != null) {
            registeredBrokers.put(cfg, clientCount);
            // System.out.println("Updated client count for broker " + brokerId + ": " + clientCount);
        } else {
            System.out.println("Received ClientCountUpdate for unknown brokerId=" + brokerId);
        }
    }

    /**
     * Registers a broker in the directory based on the registration message.
     *
     * @param msg the registration message from the broker
     * @return the created BrokerConfig
     */
    private BrokerConfig registerBroker(DirectoryRegisterMessage msg) {
        int brokerId = msg.getBrokerId();
        String host = msg.getBrokerHost();
        int port = msg.getBrokerPort();
        int clientPort = port;
        int udpPort = 50002 + brokerId;
        Map<Integer, RaftPeerEndpoint> voters = new HashMap<>();
        voters.put(brokerId, new RaftPeerEndpoint(brokerId, host, port, clientPort));
        RaftConfig raftConfig = new RaftConfig(
                200,
                400,
                40,
                port,
                Paths.get("directory-raft-data", "n" + brokerId),
                voters);

        BrokerConfig config = new BrokerConfig(
                brokerId,
                host,
                port,
                clientPort,
                udpPort,
                raftConfig
        );

        registeredBrokers.putIfAbsent(config, 0);
        brokersById.put(brokerId, config);
        lastHeartbeats.put(brokerId, System.currentTimeMillis());

        System.out.println("Registered broker: id=" + brokerId + " "
                + config.getBrokerHost() + ":" + config.getBrokerPort()
                + ", clientCount=0");

        return config;
    }

    /**
     * Starts the reaper thread that periodically removes dead brokers based on heartbeat timeouts.
     */
    private void startReaperThread() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    long now = System.currentTimeMillis();

                    // Check all brokers by id
                    for (Map.Entry<Integer, Long> entry : lastHeartbeats.entrySet()) {
                        int brokerId = entry.getKey();
                        ;
                        long last = entry.getValue();

                        if (now - last > HEARTBEAT_TIMEOUT_MS) {
                            // Consider broker dead
                            System.out.println("Broker " + brokerId + " considered DEAD (no heartbeat for " + (now - last) + " ms). Removing from directory.");

                            lastHeartbeats.remove(brokerId);

                            BrokerConfig cfg = brokersById.remove(brokerId);
                            if (cfg != null) {
                                registeredBrokers.remove(cfg);
                            }
                        }
                    }

                    Thread.sleep(REAPER_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Directory-Reaper");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Chooses the best broker to assign to a client, preferring the broker with the fewest clients.
     * If multiple brokers have the same client count, chooses the one with the lowest broker id.
     *
     * @return the selected BrokerConfig, or null if none available
     */
    private BrokerConfig chooseBestBroker() {
        BrokerConfig best = null;
        int bestCount = Integer.MAX_VALUE;
        int bestId = Integer.MAX_VALUE;

        for (Map.Entry<BrokerConfig, Integer> entry : registeredBrokers.entrySet()) {
            BrokerConfig cfg = entry.getKey();
            int count = entry.getValue();
            int id = cfg.getBrokerId();

            if (count < bestCount || (count == bestCount && id < bestId)) {
                bestCount = count;
                bestId = id;
                best = cfg;
            }
        }

        return best;
    }

    /**
     * Parses a comma-separated voters CSV (id@host:rpcPort[:clientPort],...) into a voter map.
     * Moved from BrokerMain: the static cluster topology now lives on the Directory.
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
}