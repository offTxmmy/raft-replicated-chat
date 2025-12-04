package it.polimi.ds.chat.ordering;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Sequencer-based implementation of OrderingService.
 *
 * This implementation uses a single sequencer (leader) to assign
 * global sequence numbers to messages. All brokers send their
 * ChatReqMessages to the sequencer, which orders them and broadcasts
 * ChatDeliverMessages back to all brokers.
 *
 * Architecture:
 * - One broker is designated as the sequencer (isSequencer=true in config)
 * - Non-sequencer brokers connect to the sequencer via TCP
 * - The sequencer assigns monotonically increasing sequence numbers
 * - Ordered messages are broadcast to all connected brokers
 *
 * This will be replaced by RaftOrderingService for fault tolerance.
 */
public class SequencerOrderingService implements OrderingService {

    private final BrokerConfig config;
    private int localBrokerId;

    // Callback for broker events
    private OrderingServiceCallback callback;

    // Global sequence counter (only used if this is the sequencer)
    private long globalSeq = 0;

    // Callbacks to notify when messages are ready for delivery
    private final CopyOnWriteArrayList<Consumer<ChatDeliverMessage>> deliveryCallbacks = new CopyOnWriteArrayList<>();

    // Broker connections (only used by sequencer)
    private final Map<Integer, Socket> brokerConnections = new HashMap<>();
    private final Map<Integer, ObjectOutputStream> brokerOutStreams = new HashMap<>();
    private final Map<Integer, ObjectInputStream> brokerInStreams = new HashMap<>();
    private final Map<Integer, Long> lastHeartbeats = new HashMap<>();

    // Connection to sequencer (only used by non-sequencer brokers)
    private Socket sequencerSocket;
    private ObjectOutputStream sequencerOut;
    private ObjectInputStream sequencerIn;

    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public SequencerOrderingService(BrokerConfig config, int localBrokerId) {
        this.config = config;
        this.localBrokerId = localBrokerId;
    }

    /**
     * Set a callback to be notified of ordering service events.
     */
    public void setCallback(OrderingServiceCallback callback) {
        this.callback = callback;
    }

    /**
     * Get the current broker ID (may be updated after connecting to sequencer).
     */
    public int getBrokerId() {
        return localBrokerId;
    }

    @Override
    public void start() {
        running = true;

        if (config.isSequencer()) {
            startSequencerListener();
            startHeartbeatReaper();
        } else {
            connectToSequencer();
        }
    }

    @Override
    public void stop() {
        running = false;

        // Close server socket if sequencer
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
        }

        // Close sequencer connection if follower
        if (sequencerSocket != null) {
            try {
                sequencerSocket.close();
            } catch (IOException ignored) {}
        }

        // Close all broker connections if sequencer
        synchronized (brokerConnections) {
            for (Socket socket : brokerConnections.values()) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
            brokerConnections.clear();
            brokerOutStreams.clear();
            brokerInStreams.clear();
        }
    }

    @Override
    public void propose(ChatReqMessage request) {
        if (config.isSequencer()) {
            // Handle\ locally - assign sequence and deliver
            handleChatRequest(request);
        } else {
            // Forward to sequencer
            sendToSequencer(request);
        }
    }

    @Override
    public void onDeliver(Consumer<ChatDeliverMessage> callback) {
        deliveryCallbacks.add(callback);
    }

    @Override
    public boolean isLeader() {
        return config.isSequencer();
    }

    @Override
    public int getLeaderId() {
        return config.isSequencer() ? localBrokerId : 0; // Sequencer is always broker 0
    }

    // =========================================================================
    // SEQUENCER LOGIC
    // =========================================================================

    /**
     * Handle an incoming chat request by assigning a sequence number
     * and delivering to all brokers.
     */
    private synchronized void handleChatRequest(ChatReqMessage chatReq) {
        long seq = ++globalSeq;

        ChatDeliverMessage deliver = new ChatDeliverMessage(
                seq,
                chatReq.getBrokerId(),
                chatReq.getUsername(),
                chatReq.getText(),
                chatReq.getVectorClock()
        );

        // Deliver locally first
        notifyDelivery(deliver);

        // Broadcast to all connected brokers
        broadcastToAllBrokers(deliver);

        // Send ACK to the originating broker
        sendToBroker(chatReq.getBrokerId(), new ChatReqAck(chatReq.getLocalMsgId(), seq));
    }

    private void notifyDelivery(ChatDeliverMessage message) {
        for (Consumer<ChatDeliverMessage> callback : deliveryCallbacks) {
            try {
                callback.accept(message);
            } catch (Exception e) {
                System.err.println("Error in delivery callback: " + e.getMessage());
            }
        }
    }

    private void broadcastToAllBrokers(BrokerMessage message) {
        synchronized (brokerOutStreams) {
            for (Map.Entry<Integer, ObjectOutputStream> entry : brokerOutStreams.entrySet()) {
                try {
                    entry.getValue().writeObject(message);
                    entry.getValue().flush();
                } catch (IOException e) {
                    System.err.println("Failed to send message to broker " + entry.getKey() + ": " + e.getMessage());
                }
            }
        }
    }

    private void sendToBroker(int brokerId, BrokerMessage message) {
        ObjectOutputStream out;
        synchronized (brokerOutStreams) {
            out = brokerOutStreams.get(brokerId);
        }

        if (out == null) {
            // Broker might be local (the sequencer itself)
            return;
        }

        try {
            out.writeObject(message);
            out.flush();
        } catch (IOException e) {
            System.err.println("Failed to send message to broker " + brokerId + ": " + e.getMessage());
        }
    }

    // =========================================================================
    // SEQUENCER NETWORK LISTENER
    // =========================================================================

    private void startSequencerListener() {
        Thread listenerThread = new Thread(() -> {
            try {
                int port = config.getSequencerPort();
                serverSocket = new ServerSocket(port);
                System.out.println("[OrderingService] Sequencer listening on port " + port);

                while (running) {
                    Socket brokerSocket = serverSocket.accept();
                    System.out.println("[OrderingService] New broker connection from " + brokerSocket.getRemoteSocketAddress());

                    handleNewBrokerConnection(brokerSocket);
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("[OrderingService] Sequencer listener error: " + e.getMessage());
                }
            }
        }, "OrderingService-SequencerListener");

        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void handleNewBrokerConnection(Socket brokerSocket) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(brokerSocket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(brokerSocket.getInputStream());

            // First message must be BrokerJoinMessage
            Object obj = in.readObject();
            if (!(obj instanceof BrokerJoinMessage joinMsg)) {
                System.err.println("[OrderingService] Unexpected first message: " + obj);
                brokerSocket.close();
                return;
            }

            // Assign broker ID
            HandlerState handlerState = config.getHandlerState();
            int newBrokerId = handlerState.getNewBrokerId();
            System.out.println("[OrderingService] Assigned broker ID " + newBrokerId);

            // Send acknowledgment
            out.writeObject(new BrokerJoinAck(newBrokerId));
            out.flush();

            // Register connection
            synchronized (brokerConnections) {
                brokerConnections.put(newBrokerId, brokerSocket);
                brokerOutStreams.put(newBrokerId, out);
                brokerInStreams.put(newBrokerId, in);
                lastHeartbeats.put(newBrokerId, System.currentTimeMillis());
            }

            // Start message handler thread for this broker
            startBrokerMessageHandler(newBrokerId, in);

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[OrderingService] Error handling broker connection: " + e.getMessage());
            try {
                brokerSocket.close();
            } catch (IOException ignored) {}
        }
    }

    private void startBrokerMessageHandler(int brokerId, ObjectInputStream in) {
        Thread handler = new Thread(() -> {
            try {
                while (running) {
                    Object msg = in.readObject();

                    if (msg instanceof HeartbeatMessage hb) {
                        synchronized (lastHeartbeats) {
                            lastHeartbeats.put(hb.getBrokerId(), hb.getTimestamp());
                        }
                    } else if (msg instanceof ChatReqMessage chatReq) {
                        handleChatRequest(chatReq);
                    } else {
                        System.out.println("[OrderingService] Unknown message from broker " + brokerId + ": " + msg);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                if (running) {
                    System.err.println("[OrderingService] Broker " + brokerId + " disconnected: " + e.getMessage());
                }
            } finally {
                removeBroker(brokerId);
            }
        }, "OrderingService-BrokerHandler-" + brokerId);

        handler.setDaemon(true);
        handler.start();
    }

    private void removeBroker(int brokerId) {
        synchronized (brokerConnections) {
            Socket socket = brokerConnections.remove(brokerId);
            brokerOutStreams.remove(brokerId);
            brokerInStreams.remove(brokerId);
            lastHeartbeats.remove(brokerId);

            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
        System.out.println("[OrderingService] Broker " + brokerId + " removed");
    }

    // =========================================================================
    // HEARTBEAT REAPER (for sequencer)
    // =========================================================================

    private void startHeartbeatReaper() {
        Thread reaper = new Thread(() -> {
            while (running) {
                try {
                    TimeUnit.SECONDS.sleep(5);

                    long now = System.currentTimeMillis();
                    synchronized (lastHeartbeats) {
                        for (Map.Entry<Integer, Long> entry : Map.copyOf(lastHeartbeats).entrySet()) {
                            if (now - entry.getValue() > 10000) { // 10 second timeout
                                System.out.println("[OrderingService] Broker " + entry.getKey() + " timed out");
                                removeBroker(entry.getKey());
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "OrderingService-HeartbeatReaper");

        reaper.setDaemon(true);
        reaper.start();
    }

    // =========================================================================
    // FOLLOWER LOGIC - Connect to sequencer
    // =========================================================================

    private void connectToSequencer() {
        try {
            String host = config.getSequencerHost();
            int port = config.getSequencerPort();

            System.out.println("[OrderingService] Connecting to sequencer at " + host + ":" + port);
            sequencerSocket = new Socket(host, port);

            sequencerOut = new ObjectOutputStream(sequencerSocket.getOutputStream());
            sequencerOut.flush();
            sequencerIn = new ObjectInputStream(sequencerSocket.getInputStream());

            // Send join message
            sequencerOut.writeObject(new BrokerJoinMessage(
                    config.getBrokerHost(),
                    config.getBrokerPort(),
                    localBrokerId
            ));
            sequencerOut.flush();

            // Wait for ACK with assigned broker ID
            Object response = sequencerIn.readObject();
            if (response instanceof BrokerJoinAck ack) {
                this.localBrokerId = ack.getBrokerId();
                System.out.println("[OrderingService] Received broker ID: " + ack.getBrokerId());

                // Notify callback of broker ID assignment
                if (callback != null) {
                    callback.onBrokerIdAssigned(ack.getBrokerId());
                    callback.onConnectionEstablished();
                }
            }

            // Start listening for messages from sequencer
            startSequencerInboundHandler();

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[OrderingService] Failed to connect to sequencer: " + e.getMessage());
        }
    }

    private void startSequencerInboundHandler() {
        Thread handler = new Thread(() -> {
            try {
                while (running) {
                    Object msg = sequencerIn.readObject();

                    if (msg instanceof ChatDeliverMessage deliver) {
                        notifyDelivery(deliver);
                    } else if (msg instanceof ChatReqAck ack) {
                        System.out.println("[OrderingService] Received ACK for " + ack.getLocalMsgId());
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                if (running) {
                    System.err.println("[OrderingService] Lost connection to sequencer: " + e.getMessage());
                    if (callback != null) {
                        callback.onConnectionLost();
                    }
                }
            }
        }, "OrderingService-SequencerInbound");

        handler.setDaemon(true);
        handler.start();
    }

    private void sendToSequencer(ChatReqMessage request) {
        if (sequencerOut == null) {
            System.err.println("[OrderingService] Not connected to sequencer");
            return;
        }

        try {
            sequencerOut.writeObject(request);
            sequencerOut.flush();
        } catch (IOException e) {
            System.err.println("[OrderingService] Failed to send to sequencer: " + e.getMessage());
        }
    }

    // =========================================================================
    // HEARTBEAT SENDING (for followers)
    // =========================================================================

    /**
     * Send a heartbeat to the sequencer (called by Broker's heartbeat loop).
     */
    public void sendHeartbeat(HeartbeatMessage heartbeat) {
        if (sequencerOut != null) {
            try {
                sequencerOut.writeObject(heartbeat);
                sequencerOut.flush();
            } catch (IOException e) {
                System.err.println("[OrderingService] Failed to send heartbeat: " + e.getMessage());
            }
        }
    }

    /**
     * Get the assigned broker ID after connecting to sequencer.
     * This is used by followers to update their broker ID.
     */
    public int getAssignedBrokerId() {
        // For sequencer, return local broker ID
        // For followers, this should be called after connection is established
        return localBrokerId;
    }
}

