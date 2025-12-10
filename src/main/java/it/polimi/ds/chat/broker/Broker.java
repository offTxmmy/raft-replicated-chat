package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.client.ClientHandler;
import it.polimi.ds.chat.messages.*;
import it.polimi.ds.chat.ordering.OrderingService;
import it.polimi.ds.chat.ordering.OrderingServiceCallback;
import it.polimi.ds.chat.ordering.SequencerOrderingService;
import it.polimi.ds.chat.utilities.HoldBackQueue;
import it.polimi.ds.chat.utilities.VectorClock;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

/**
 * Broker node in the replicated chat infrastructure.
 *
 * Responsibilities:
 * - Accept TCP connections from chat clients.
 * - Use OrderingService for message ordering (sequencer or Raft in future).
 * - Deliver ordered messages to its local clients.
 */
public class Broker implements Serializable, OrderingServiceCallback {

    // Directory Service config (for now hardcoded)
    private static final String DIRECTORY_HOST = "localhost";
    private static final int DIRECTORY_PORT = 60000;
    private static final int DIRECTORY_CLIENT_PORT = 60001; // Port for peer list queries
    private static final long HEARTBEAT_INTERVAL_MS = 3000;

    // Connection to directory service
    private transient Socket directorySocket;
    private transient ObjectOutputStream directoryOut;
    private final transient Object directoryLock = new Object();

    // Peer registry for broker-to-broker discovery
    private transient PeerRegistry peerRegistry;

    // Static configuration for this broker (ports, host, isSequencer, etc.)
    private final BrokerConfig config;

    // Runtime brokerId (may differ from initial config value for followers)
    private int brokerId;

    // Vector clock for tracking causal dependencies when sending messages
    private final VectorClock vectorClock = new VectorClock();

    // Hold-back queue for ordered delivery (enforces total order + causal order)
    private final HoldBackQueue holdBackQueue = new HoldBackQueue();

    // All clients currently connected to this broker.
    private final List<ClientHandler> clients = Collections.synchronizedList(new ArrayList<>());

    // Local sequence counter used in fallback / local mode.
    private long nextSeq = 1;

    // Ordering service for message ordering (decoupled from networking)
    private transient OrderingService orderingService;

    // Local per-broker message counter to build unique localMsgId values.
    private long localMsgCounter = 0;

    /**
     * Construct a broker with the given configuration.
     * Initializes the OrderingService based on configuration.
     */
    public Broker(BrokerConfig config) {
        this.config = config;
        this.brokerId = config.getBrokerId(); // 0 for leader, -1 for followers at startup

        // Validate config for sequencer
        if (config.isSequencer() && config.getHandlerState() == null) {
            throw new IllegalArgumentException("Sequencer broker must have a HandlerState");
        }

        // Initialize OrderingService
        initializeOrderingService();

        // Initialize PeerRegistry (will be started in start())
        this.peerRegistry = new PeerRegistry(brokerId, DIRECTORY_HOST, DIRECTORY_CLIENT_PORT);
    }

    /**
     * Initialize the ordering service.
     * Currently uses SequencerOrderingService, can be replaced with RaftOrderingService.
     */
    private void initializeOrderingService() {
        SequencerOrderingService seqService = new SequencerOrderingService(config, brokerId);
        seqService.setCallback(this);
        seqService.onDeliver(this::handleOrderedMessage);
        this.orderingService = seqService;
    }

    // =========================================================================
    // OrderingServiceCallback implementation
    // =========================================================================

    @Override
    public void onBrokerIdAssigned(int newBrokerId) {
        System.out.println("[Broker] Broker ID updated from " + this.brokerId + " to " + newBrokerId);
        this.brokerId = newBrokerId;

        // Update peer registry with new broker ID
        if (this.peerRegistry != null) {
            this.peerRegistry = new PeerRegistry(newBrokerId, DIRECTORY_HOST, DIRECTORY_CLIENT_PORT);
        }
    }

    @Override
    public void onConnectionLost() {
        System.err.println("[Broker] Connection to ordering service lost!");
    }

    @Override
    public void onConnectionEstablished() {
        System.out.println("[Broker] Connected to ordering service");
    }

    // =========================================================================
    // Getters
    // =========================================================================

    /**
     * Get the vector clock tracking delivered messages.
     */
    public VectorClock getVectorClock() {
        return holdBackQueue.getDeliveredClock();
    }

    /**
     * Get the vector clock used for outgoing messages.
     */
    public VectorClock getSendVectorClock() {
        return vectorClock;
    }

    public int getBrokerId() {
        return brokerId;
    }

    /**
     * Get the ordering service used by this broker.
     */
    public OrderingService getOrderingService() {
        return orderingService;
    }

    /**
     * Set a custom ordering service (useful for testing or switching to Raft).
     */
    public void setOrderingService(OrderingService orderingService) {
        this.orderingService = orderingService;
        if (orderingService instanceof SequencerOrderingService seqService) {
            seqService.setCallback(this);
            seqService.onDeliver(this::handleOrderedMessage);
        }
    }

    /**
     * Get the peer registry for broker-to-broker discovery.
     */
    public PeerRegistry getPeerRegistry() {
        return peerRegistry;
    }

    // =========================================================================
    // Broker lifecycle
    // =========================================================================

    /**
     * Start the broker:
     * - Start the OrderingService (handles sequencer/follower logic)
     * - Open TCP listener for clients
     * - Connect to directory service
     * - Start peer discovery
     */
    public void start() throws IOException {
        // Start the ordering service (handles sequencer connections)
        orderingService.start();

        // Wait a bit for follower to get broker ID from sequencer
        if (!config.isSequencer()) {
            try {
                Thread.sleep(500); // Allow time for connection and ID assignment
            } catch (InterruptedException ignored) {}
        }

        // Connect to directory service, register, and start sending heartbeats
        connectAndRegisterWithDirectoryService();
        startHeartbeatLoop();

        // Start peer discovery after registration
        startPeerDiscovery();

        int port = config.getBrokerPort();
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker " + brokerId + " listening for clients on port " + port);

        // Main loop: accept client TCP connections and spawn a ClientHandler for each.
        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("New client connected from " + clientSocket.getRemoteSocketAddress());

            // One handler per client, running in its own thread.
            ClientHandler handler = new ClientHandler(clientSocket, this);
            clients.add(handler);
            sendClientCountUpdate();

            Thread t = new Thread(handler);
            t.setDaemon(true);  // daemon so it doesn't block JVM shutdown
            t.start();
        }
    }

    /**
     * Remove a client from the internal list (called by ClientHandler when the client disconnects).
     */
    public void removeClient(ClientHandler handler) {
        clients.remove(handler);
        sendClientCountUpdate();
    }

    /**
     * Assign a sequence number to a message and deliver it to local clients.
     * Currently used in local/fallback mode.
     */
    public void broadcastToClients(String sender, String text) {
        long seq;
        synchronized (this) {
            seq = nextSeq++;
        }
        onChatDeliver(seq, sender, text);
    }

    /**
     * Deliver a message that has already been assigned a global sequence number
     * to all locally connected clients.
     */
    public void onChatDeliver(long seq, String sender, String text) {
        synchronized (clients) {
            for (ClientHandler handler : clients) {
                if (!handler.getUsername().equals(sender)) {  // Don't send the message to the sender
                    handler.sendMessageToClient(seq, sender, text);
                }
            }
        }
    }

    /**
     * Entry point for messages sent by clients connected to THIS broker.
     * Uses the OrderingService to propose messages for global ordering.
     */
    public void onClientMessage(ClientMessage message) {
        // Build the chat request with vector clock
        ChatReqMessage chatReq = buildChatReq(message.getUsername(), message.getText());

        // Propose to ordering service
        orderingService.propose(chatReq);

        // Send ACK to client
        sendAckToClient(message.getUsername(), message.getTimestamp());
    }

    /**
     * Handle an ordered message from the OrderingService.
     * Uses HoldBackQueue to ensure both total order and causal order.
     */
    public void handleOrderedMessage(ChatDeliverMessage chatDeliver) {
        // Enqueue message and get all messages ready for delivery
        List<ChatDeliverMessage> readyMessages = holdBackQueue.enqueue(chatDeliver);

        // Deliver all ready messages to local clients
        for (ChatDeliverMessage msg : readyMessages) {
            onChatDeliver(msg.getSeq(), msg.getUsername(), msg.getText());
        }

        // Log if messages are being held back
        if (holdBackQueue.hasPendingMessages()) {
            System.out.println("[Broker " + brokerId + "] " + holdBackQueue.getPendingCount() +
                    " message(s) held back, waiting for seq=" + holdBackQueue.getExpectedSeq());
        }
    }

    public void notifyLeave(String username) {
        broadcastToClients("[system]", username + " left the chat");
    }

    public void notifyJoin(String username) {
        broadcastToClients("[system]", username + " joined the chat");
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    private synchronized ChatReqMessage buildChatReq(String username, String text) {
        vectorClock.increment(brokerId);
        String localMsgId = brokerId + "-" + (++localMsgCounter);
        return new ChatReqMessage(localMsgId, brokerId, username, text, new VectorClock(vectorClock));
    }

    private void connectAndRegisterWithDirectoryService() {
        System.out.println("Connecting to Directory Service at " + DIRECTORY_HOST + ":" + DIRECTORY_PORT + "...");
        try {
            directorySocket = new Socket(DIRECTORY_HOST, DIRECTORY_PORT);
            directoryOut = new ObjectOutputStream(directorySocket.getOutputStream());

            DirectoryRegisterMessage msg = new DirectoryRegisterMessage(
                    this.brokerId,
                    config.getBrokerHost(),
                    config.getBrokerPort(),
                    config.isSequencer()
            );

            synchronized (directoryLock) {
                directoryOut.writeObject(msg);
                directoryOut.flush();
            }

            System.out.println("Registered broker in Directory Service: " + msg);
        } catch (IOException e) {
            System.err.println("Failed to connect/register with Directory Service: " + e.getMessage());
        }
    }

    private void startHeartbeatLoop() {
        if (directoryOut == null) {
            System.err.println("Heartbeat not started: no connection to Directory Service.");
            return;
        }

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    HeartbeatMessage hb = new HeartbeatMessage(this.brokerId, System.currentTimeMillis());

                    synchronized (directoryLock) {
                        directoryOut.writeObject(hb);
                        directoryOut.flush();
                    }

                    // Send heartbeat to OrderingService (for sequencer)
                    if (!config.isSequencer() && orderingService instanceof SequencerOrderingService seqService) {
                        seqService.sendHeartbeat(hb);
                    }

                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                } catch (IOException e) {
                    System.err.println("Failed to send heartbeat: " + e.getMessage());
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            try {
                if (directorySocket != null) {
                    directorySocket.close();
                }
            } catch (IOException ignored) {}
        }, "HeartbeatSender-" + brokerId);

        t.setDaemon(true);
        t.start();
    }

    private void sendClientCountUpdate() {
        if (directoryOut == null) return;

        ClientCountUpdateMessage msg = new ClientCountUpdateMessage(brokerId, clients.size());

        try {
            synchronized (directoryLock) {
                directoryOut.writeObject(msg);
                directoryOut.flush();
            }
        } catch (IOException e) {
            System.err.println("Failed to send client count update: " + e.getMessage());
        }
    }

    /**
     * Start the peer discovery mechanism.
     * Registers for peer list updates and logs changes.
     */
    private void startPeerDiscovery() {
        if (peerRegistry == null) {
            System.err.println("[Broker] PeerRegistry not initialized");
            return;
        }

        // Register listener for peer changes
        peerRegistry.addPeerChangeListener(peers -> {
            System.out.println("[Broker " + brokerId + "] Peer list updated: " + peers.size() + " peers");
            for (PeerInfo peer : peers) {
                System.out.println("  - " + peer);
            }
        });

        // Start the registry
        peerRegistry.start();
        System.out.println("[Broker " + brokerId + "] Peer discovery started");
    }

    private void sendAckToClient(String username, long timestamp) {
        synchronized (clients) {
            for (ClientHandler handler : clients) {
                if (username.equals(handler.getUsername())) {
                    handler.sendLine(ClientAckMessages.buildAck(timestamp, username));
                    break;
                }
            }
        }
    }
}


