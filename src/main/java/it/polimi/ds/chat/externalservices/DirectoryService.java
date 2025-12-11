package it.polimi.ds.chat.externalservices;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
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

    /**
     * Main entry point for the Directory Service.
     * Starts listeners for broker and client connections.
     *
     * @param args command-line arguments (unused)
     */
    public static void main(String[] args) {
        int brokerPort = 60000;
        int clientPort = 60001;

        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: DIRECTORY SERVICE---");
        DirectoryService service = new DirectoryService();

        // Lister on brokerPort (register + heartbeat
        new Thread(() -> service.startBrokersListener(brokerPort), "Dir-BrokerListener").start();

        // Listen on clientPort (GET_BROKER)
        new Thread(() -> service.startClientsListener(clientPort), "Dir-ClientListener").start();
    }

    /**
     * Constructs a DirectoryService and starts the reaper thread for broker liveness.
     */
    public DirectoryService() {
        startReaperThread();
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
            }/* else if (obj instanceof GetPeerListRequestMessage req) {
                handleGetPeerListRequest(req, out);
            }*/ else {
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
     * Handles a client request for the peer list and sends a response.
     *
     * @param req the peer list request message
     * @param out the output stream to the client
     * @throws IOException if an I/O error occurs
     */
    /*private void handleGetPeerListRequest(GetPeerListRequestMessage req, ObjectOutputStream out) throws IOException {
        int requestingBrokerId = req.getRequestingBrokerId();
        List<PeerInfo> peers = new ArrayList<>();

        for (Map.Entry<Integer, BrokerConfig> entry : brokersById.entrySet()) {
            int peerId = entry.getKey();
            BrokerConfig cfg = entry.getValue();

            // Include all brokers (including self, requester can filter if needed)
            peers.add(new PeerInfo(
                    peerId,
                    cfg.getBrokerHost(),
                    cfg.getBrokerPort(),
                    cfg.isSequencer()
            ));
        }

        GetPeerListResponseMessage resp = new GetPeerListResponseMessage(true, peers);
        out.writeObject(resp);
        out.flush();

        System.out.println("Returned peer list (" + peers.size() + " brokers) to broker " + requestingBrokerId);
    }*/

    /**
     * Handles a broker connection, processing registration and heartbeats.
     *
     * @param socket the broker socket
     */
    private void handleConnection(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Expected first message: DirectoryRegisterMessage
            Object first = in.readObject();
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
                    // update last heartbeat time
                    lastHeartbeats.put(brokerId, hb.getTimestamp());
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
        boolean isSequencer = msg.isSequencer();

        int clientPort = port;
        String sequencerHost = host;
        int sequencerPort = 0;
        int udpPort = 0;
        HandlerState handlerState = null;

        BrokerConfig config = new BrokerConfig(
                brokerId,
                isSequencer,
                host,
                port,
                clientPort,
                sequencerHost,
                sequencerPort,
                udpPort,
                handlerState
        );

        registeredBrokers.putIfAbsent(config, 0);
        brokersById.put(brokerId, config);
        lastHeartbeats.put(brokerId, System.currentTimeMillis());

        System.out.println("Registered broker: id=" + brokerId + " "
                + config.getBrokerHost() + ":" + config.getBrokerPort()
                + " (sequencer=" + isSequencer + "), clientCount=0");

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
}