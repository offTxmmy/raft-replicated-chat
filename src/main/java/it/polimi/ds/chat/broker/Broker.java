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
    private static final long HEARTBEAT_INTERVAL_MS = 3000; // Interval to send heartbeats (3 seconds)
    private static final long HEARTBEAT_TIMEOUT_MS = 5000; // Timeout for broker failure detection (5 seconds)

    // Connection to directory service
    private transient Socket directorySocket;
    private transient ObjectOutputStream directoryOut;
    private final transient Object directoryLock = new Object();

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

    // Legacy: Holds the global sequencing state when this broker acts as sequencer.
    // Kept for backward compatibility, will be removed once OrderingService is fully integrated.
    @Deprecated
    private transient SequencerState sequencerState;

    // TCP connection from this broker to the sequencer (only if NOT sequencer).
    // Legacy: will be managed by OrderingService
    @Deprecated
    private transient Socket sequencerSocket;
    @Deprecated
    private transient ObjectOutputStream sequencerOut;
    @Deprecated
    private transient ObjectInputStream sequencerIn;

    // Local per-broker message counter to build unique localMsgId values.
    private long localMsgCounter = 0;

    /**
     * Construct a broker with the given configuration.
     * Initializes the OrderingService based on configuration.
     */
    public Broker(BrokerConfig config) {
        this.config = config;
        this.brokerId = config.getBrokerId(); // 0 for leader, -1 for followers at startup

        // Initialize OrderingService
        initializeOrderingService();

        // Legacy: keep SequencerState for backward compatibility during transition
        if(config.isSequencer()) {
            if (config.getHandlerState() == null) {
                throw new IllegalArgumentException("Sequencer broker must have a HandleState");
            }
            sequencerState = new SequencerState(this);
        }
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
    }

    @Override
    public void onConnectionLost() {
        System.err.println("[Broker] Connection to ordering service lost!");
        // TODO: implement reconnection logic
    }

    @Override
    public void onConnectionEstablished() {
        System.out.println("[Broker] Connected to ordering service");
    }

    /**
     * Get the vector clock tracking delivered messages.
     * This returns the delivered clock from the HoldBackQueue for test/debug purposes.
     */
    public VectorClock getVectorClock() {
        return holdBackQueue.getDeliveredClock();
    }

    /**
     * Get the vector clock used for outgoing messages (message sending).
     */
    public VectorClock getSendVectorClock() {
        return vectorClock;
    }

    public int getBrokerId() {
        return brokerId;
    }

    /**
     * Get the ordering service used by this broker.
     * Useful for testing and advanced configurations.
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
     * Start the broker:
     * - Start the OrderingService (handles sequencer/follower logic)
     * - Open TCP listener for clients
     * - Connect to directory service
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
     * Legacy start method using old sequencer logic.
     * @deprecated Use start() which uses OrderingService
     */
    @Deprecated
    public void startLegacy() throws IOException {
        // Followers must first join the sequencer to obtain their brokerId
        if (!config.isSequencer()) {
            connectToSequencer();
        }

        // Connect to directory service, register, and start sending heartbeats
        connectAndRegisterWithDirectoryService();
        startHeartbeatLoop();

        int port = config.getBrokerPort();
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker " + brokerId + " listening for clients on port " + port);

        // If this broker is the sequencer, ensure sequencer state exists
        // and start the listener for CHAT_REQ from other brokers.
        if (config.isSequencer()) {
            if (sequencerState == null) {
                sequencerState = new SequencerState(this);
            }
            startSequencerListener();
        }

        // Main loop: accept client TCP connections and spawn a ClientHandler for each.
        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("New client connected from " + clientSocket.getRemoteSocketAddress());

            // One handler per client, running in its own thread.
            ClientHandler handler = new ClientHandler(clientSocket, this);
            clients.add(handler);
            sendClientCountUpdate();

            Thread t = new Thread(handler);
            t.setDaemon(true);
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
     * Legacy method: Entry point for messages using old sequencer pattern.
     * @deprecated Use onClientMessage which delegates to OrderingService
     */
    @Deprecated
    public void onClientMessageLegacy(ClientMessage message) {
        if (config.isSequencer()) {
            // If this broker is the sequencer, handle the message directly
            ChatReqMessage chatReq = buildChatReq(message.getUsername(), message.getText());

            // Reuse the same ordering path used for follower brokers so that
            // local messages are globally sequenced and delivered to every
            // broker (including this one).
            sequencerState.handleChatFromBroker(chatReq);

            // Confirm reception to the local client after handing the message
            // to the sequencer pipeline.
            sendAckToClient(message.getUsername(), message.getTimestamp());
        } else {
            // Delegate ordering to the sequencer via CHAT_REQ.
            sendChatReqToSequencer(message.getUsername(), message.getText(), message.getTimestamp());
        }
    }

    public void notifyLeave(String username) {
        broadcastToClients("[system]", username + " left the chat");
    }

    public void notifyJoin(String username) {
        broadcastToClients("[system]", username + " joined the chat");
    }

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
            // you can decide later if you want to System.exit(1) here
        }
    }

    /**
     * Start the heartbeat loop for both the Directory Service and the Sequencer.
     */
    private void startHeartbeatLoop() {
        if (directoryOut == null) {
            System.err.println("Heartbeat not started: no connection to Directory Service.");
            return;
        }

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    HeartbeatMessage hb = new HeartbeatMessage(
                            this.brokerId,
                            System.currentTimeMillis()
                    );

                    // Send heartbeat to the DirectoryService
                    synchronized (directoryLock) {
                        directoryOut.writeObject(hb);
                        directoryOut.flush();
                    }

                    // Send the heartbeat to the Sequencer via OrderingService (if it's not the sequencer itself)
                    if(!config.isSequencer() && orderingService instanceof SequencerOrderingService seqService) {
                        seqService.sendHeartbeat(hb);
                    }

                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                } catch (IOException e) {
                    System.err.println("Failed to send heartbeat to Directory Service: " + e.getMessage());
                    // Connection is probably dead; stop the loop
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
     * Send heartbeat to the sequencer
     * @deprecated Use OrderingService for heartbeat sending
     */
    @Deprecated
    public void sendHeartbeatToSequencer(HeartbeatMessage heartbeatMessage) {
        try {
            if (sequencerOut != null) {
                sequencerOut.writeObject(heartbeatMessage);
                sequencerOut.flush();
            }
        } catch (IOException e) {
            System.err.println("Failed to send heartbeat to Sequencer: " + e.getMessage());
        }
    }

    private void sendClientCountUpdate() {
        if (directoryOut == null) {
            return;
        }

        int count = clients.size();
        ClientCountUpdateMessage msg = new ClientCountUpdateMessage(brokerId, count);

        try {
            synchronized (directoryLock) {
                directoryOut.writeObject(msg);
                directoryOut.flush();
            }
        } catch (IOException e) {
            System.err.println("Failed to send client count update to Directory Service: " + e.getMessage());
        }
    }

    /* =========================================================
       1) FOLLOWER SIDE: connect to sequencer and obtain brokerId
       ========================================================= */

    /**
     * Open a TCP connection to the sequencer broker and perform a simple
     * join handshake to obtain a brokerId using object messages:
     *
     * Broker → Sequencer:  BrokerJoinMessage(host, port)
     * Sequencer → Broker:  BrokerJoinAck(brokerId)
     */
    private void connectToSequencer() {
        try {
            System.out.println("Connecting to the sequencer at " + config.getSequencerHost() + ":" + config.getSequencerPort());
            sequencerSocket = new Socket(config.getSequencerHost(), config.getSequencerPort());

            // IMPORTANT: always create ObjectOutputStream first, then flush, then ObjectInputStream
            sequencerOut = new ObjectOutputStream(sequencerSocket.getOutputStream());
            sequencerOut.flush();
            sequencerIn = new ObjectInputStream(sequencerSocket.getInputStream());

            System.out.println("Connected to sequencer, sending BrokerJoinMessage...");

            // send join message
            BrokerJoinMessage join = new BrokerJoinMessage(config.getBrokerHost(), config.getBrokerPort(), brokerId);
            sequencerOut.writeObject(join);
            sequencerOut.flush();

            // read ack
            Object obj = sequencerIn.readObject();
            if (obj instanceof BrokerJoinAck ack) {
                this.brokerId = ack.getBrokerId();;
                System.out.println("Sequencer assigned broker ID: " + this.brokerId);

                startSequencerInboundLoop();
            } else {
                throw new IOException("Unexpected response from sequencer: " + obj);
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to connect/join to sequencer: " + e.getMessage());
            e.printStackTrace();
            // TODO: retry or exit gracefully
        }
    }

    private void startSequencerInboundLoop() {
        if (sequencerIn == null) {
            return;
        }

        Thread t = new Thread(() -> {
            try {
                while (true) {
                    Object obj = sequencerIn.readObject();
                    if (obj instanceof BrokerMessage brokerMessage) {
                        handleMessageFromSequencer(brokerMessage);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Sequencer inbound loop stopped: " + e.getMessage());
            }
        }, "SequencerInbound-" + brokerId);

        t.setDaemon(true);
        t.start();
    }

    private void handleMessageFromSequencer(BrokerMessage message) {
        if (message instanceof ChatReqAck chatReqAck) {
            System.out.println("Received ChatReqAck for message " + chatReqAck.getLocalMsgId() + " with seq " + chatReqAck.getGlobalSeq());
        } else if (message instanceof ChatDeliverMessage chatDeliver) {
            handleOrderedMessage(chatDeliver);
        } else {
            System.out.println("Received broker message from sequencer: " + message.getClass().getSimpleName());
        }
    }

    /**
     * Send a ChatReqMessage to the sequencer when a local client sends a chat message.
     * @deprecated Use OrderingService.propose() instead
     */
    @Deprecated
    public void sendChatReqToSequencer(String username, String text, long timestamp) {
        if (sequencerOut == null) {
            System.err.println("No connection to sequencer; falling back to local broadcast.");
            broadcastToClients(username, text);
            sendAckToClient(username, timestamp);
            return;
        }

        ChatReqMessage msg = buildChatReq(username, text);

        try {
            sequencerOut.writeObject(msg);
            sequencerOut.flush();
            sendAckToClient(username, timestamp);
        } catch (IOException e) {
            System.err.println("Failed to send ChatReqMessage to sequencer: " + e.getMessage());
            broadcastToClients(username, text); // fallback
            sendAckToClient(username, timestamp);
        }
    }

    /**
     * Handle an ordered message from the sequencer.
     * Uses HoldBackQueue to ensure both total order (by sequence number)
     * and causal order (by vector clocks) before delivery.
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

    /* =========================================================
       2) SEQUENCER SIDE: listen for brokers (JOIN + CHAT_REQ)
       ========================================================= */

    /**
     * Start a dedicated thread that listens for connections from other brokers.
     * First message on each connection is expected to be BrokerJoinMessage;
     * the sequencer responds with ASSIGN_ID using HandlerState (AtomicInteger).
     */
    private void startSequencerListener() {
        new Thread(() -> {
            try {
                int port = config.getSequencerPort();
                ServerSocket serverSocket = new ServerSocket(port);
                System.out.println("Sequencer " + config.getBrokerId() + " listening for ChatReqMessage on port " + port);

                while (true) {
                    Socket brokerSocket = serverSocket.accept();
                    System.out.println("Sequencer accepted connection from broker: " + brokerSocket.getRemoteSocketAddress());

                    // Object streams for this broker connection
                    ObjectOutputStream out = new ObjectOutputStream(brokerSocket.getOutputStream());
                    out.flush();
                    ObjectInputStream in = new ObjectInputStream(brokerSocket.getInputStream());

                    // 1) First message must be BrokerJoinMessage
                    Object obj = in.readObject();
                    if (!(obj instanceof BrokerJoinMessage)) {
                        System.err.println("Unexpected first message from broker: " + obj);
                        brokerSocket.close();
                        continue;
                    }

                    int newBrokerId = config.getHandlerState().getNewBrokerId();
                    System.out.println("Assigned broker ID " + newBrokerId + " to broker " + brokerSocket.getRemoteSocketAddress());

                    // send ack
                    BrokerJoinAck ack = new BrokerJoinAck(newBrokerId);
                    out.writeObject(ack);
                    out.flush();

                    // 2) Register connection within the sequencer state to listen/send messages
                    sequencerState.registerBrokerConnection(newBrokerId, brokerSocket, in, out);
                }
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Sequencer listener failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, "SequencerListener-" + config.getBrokerId()).start();
    }

    /**
     * Manda un ACK al client con dato username.
     */
    private void sendAckToClient(String username, long timestamp) {
        synchronized (clients) {
            for (ClientHandler handler : clients) {
                if (username.equals(handler.getUsername())) {
                    String ackLine = ClientAckMessages.buildAck(timestamp, username);
                    handler.sendLine(ackLine);
                    break;
                }
            }
        }
    }
}