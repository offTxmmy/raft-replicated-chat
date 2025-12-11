package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.client.ClientHandler;
import it.polimi.ds.chat.externalservices.LanDiscoveryService;
import it.polimi.ds.chat.messages.*;
import it.polimi.ds.chat.ordering.OrderingService;
import it.polimi.ds.chat.ordering.OrderingServiceCallback;
import it.polimi.ds.chat.ordering.SequencerOrderingService;
import it.polimi.ds.chat.utilities.HoldBackQueue;
import it.polimi.ds.chat.utilities.VectorClock;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
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

    // Peer discovery over LAN using UDP broadcast
    private transient LanDiscoveryService lanDiscoveryService;

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

        // Initialize PeerRegistry (LAN discovery only)
        this.peerRegistry = new PeerRegistry(brokerId);
        this.lanDiscoveryService = new LanDiscoveryService(config, peerRegistry);
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

    /**
     * Callback invoked when the ordering service assigns a broker ID to this instance.
     *
     * Updates internal brokerId, logs the change, and reinitializes the PeerRegistry with the new id.
     *
     * @param newBrokerId the broker id assigned by the ordering service
     */
    @Override
    public void onBrokerIdAssigned(int newBrokerId) {
        System.out.println("[Broker] Broker ID updated from " + this.brokerId + " to " + newBrokerId);
        this.brokerId = newBrokerId;

        // Update peer registry with new broker ID
        if (this.peerRegistry != null) {
            this.peerRegistry = new PeerRegistry(newBrokerId);
        }

        if (this.lanDiscoveryService != null) {
            this.lanDiscoveryService.setBrokerId(newBrokerId);
        }
    }

    /**
     * Callback invoked when the connection to the ordering service is lost.
     *
     * This method should perform any required cleanup or reconnection logic (currently logs an error).
     */
    @Override
    public void onConnectionLost() {
        System.err.println("[Broker] Connection to ordering service lost!");
    }

    /**
     * Callback invoked when the connection to the ordering service is established.
     *
     * Can be used to notify the operator or trigger follow-up actions.
     */
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

    /**
     * Get the broker identifier assigned to this broker instance.
     *
     * @return current broker id
     */
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
     * - Start the OrderingService (handles sequencer or follower logic)
     * - Open TCP listener for clients
     * - Connect to directory service
     * - Start peer discovery
     *
     * This method blocks in a loop accepting client connections.
     *
     * @throws IOException if the server socket cannot be opened
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
     *
     * @param handler the client handler to remove
     */
    public void removeClient(ClientHandler handler) {
        clients.remove(handler);
        sendClientCountUpdate();
    }

    /**
     * Notify local clients with a broadcast assigned locally (fallback/local mode).
     *
     * Assigns a local sequence number and delivers the message to local clients.
     *
     * @param sender username of the sender (will not receive the message)
     * @param text   message text to broadcast
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
     *
     * The sender will not receive the message back.
     *
     * @param seq    global sequence number
     * @param sender username of the sender
     * @param text   message text
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
     *
     * Wraps the client message into a ChatReqMessage (including vector clock) and proposes it
     * to the OrderingService for global sequencing. Also sends an immediate ACK to the client.
     *
     * @param message raw client message received by this broker
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
     *
     * Enqueues the delivered ChatDeliverMessage into the HoldBackQueue to enforce causal +
     * total order, obtains all messages that are now ready, and delivers them locally.
     *
     * @param chatDeliver message delivered by the ordering service (contains global seq)
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

    /**
     * Notify local clients that a user has left the chat.
     *
     * @param username username of the user who left
     */
    public void notifyLeave(String username) {
        broadcastToClients("[system]", username + " left the chat");
    }

    /**
     * Notify local clients that a user has joined the chat.
     *
     * @param username username of the user who joined
     */
    public void notifyJoin(String username) {
        broadcastToClients("[system]", username + " joined the chat");
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    /**
     * Build a ChatReqMessage including an updated vector clock and a unique local message id.
     *
     * This method increments the local vector clock for this broker and generates a localMsgId.
     *
     * @param username sender username
     * @param text     message text
     * @return constructed ChatReqMessage ready for proposing to the ordering service
     */
    private synchronized ChatReqMessage buildChatReq(String username, String text) {
        vectorClock.increment(brokerId);
        String localMsgId = brokerId + "-" + (++localMsgCounter);
        return new ChatReqMessage(localMsgId, brokerId, username, text, new VectorClock(vectorClock));
    }

    /**
     * Connect to the Directory Service and register this broker.
     *
     * Establishes a TCP connection and sends a DirectoryRegisterMessage. Errors are logged.
     */
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

    /**
     * Start a background thread that periodically sends heartbeats to the Directory Service.
     *
     * If the ordering service is a SequencerOrderingService, also forward the heartbeat to it.
     * The thread runs until an IO error occurs or it is interrupted.
     */
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

    /**
     * Send an update message to the Directory Service reporting the current number of connected clients.
     *
     * Errors while sending are logged.
     */
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

        // Only LAN discovery is used
        if (lanDiscoveryService != null) {
            try {
                lanDiscoveryService.start();
                for (int i = 0; i < 3; i++) {
                    lanDiscoveryService.announcePresence();
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {}
                }
            } catch (SocketException e) {
                System.err.println("[Broker " + brokerId + "] Failed to start LAN discovery: " + e.getMessage());
            }
        }
        System.out.println("[Broker " + brokerId + "] Peer discovery started (LAN broadcast only)");
    }

    /**
     * Send an ACK line to a specific connected client (by username).
     *
     * @param username  recipient username
     * @param timestamp original client message timestamp to include in the ACK
     */
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
