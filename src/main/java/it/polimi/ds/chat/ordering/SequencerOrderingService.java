package it.polimi.ds.chat.ordering;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.*;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Sequencer-based implementation of OrderingService.
 * <p>
 * This implementation uses a single sequencer (leader) to assign
 * global sequence numbers to messages. All brokers send their
 * ChatReqMessages to the sequencer, which orders them and broadcasts
 * ChatDeliverMessages back to all brokers.
 * <p>
 * Architecture:
 * <ul>
 *     <li>One broker is designated as the sequencer (isSequencer=true in config)</li>
 *     <li>Non-sequencer brokers connect to the sequencer via TCP</li>
 *     <li>The sequencer assigns monotonically increasing sequence numbers</li>
 *     <li>Ordered messages are broadcast to all connected brokers</li>
 * </ul>
 * This will be replaced by RaftOrderingService for fault tolerance.
 */
public class SequencerOrderingService implements OrderingService {

    private final BrokerConfig config;
    private int localBrokerId;

    // Callback for broker events
    private OrderingServiceCallback callback;

    // Global sequence counter
    private long globalSeq = 0;

    // Callbacks to notify when messages are ready for delivery
    private final CopyOnWriteArrayList<Consumer<ChatDeliverMessage>> deliveryCallbacks = new CopyOnWriteArrayList<>();

    // TCP Connections
    private final Map<Integer, Socket> brokerConnections = new HashMap<>();
    private final Map<Integer, ObjectOutputStream> brokerOutStreams = new HashMap<>();
    private final Map<Integer, ObjectInputStream> brokerInStreams = new HashMap<>();
    private final Map<Integer, Long> lastHeartbeats = new HashMap<>();

    // // Follower connection to sequencer
    private Socket sequencerSocket;
    private ObjectOutputStream sequencerOut;
    private ObjectInputStream sequencerIn;

    // UDP Data Channel
    private DatagramSocket dataUdpSocket;
    private final int dataUdpPort;
    private static final int MAX_UDP_PACKET_SIZE = 65507;

    // Reliability layer: message history
    private final Map<Long, ChatDeliverMessage> messageHistory = new ConcurrentHashMap<>();
    private static final int HISTORY_SIZE = 1000;

    private volatile boolean running = false;
    private ServerSocket serverSocket;

    /**
     * Constructs a SequencerOrderingService with the given configuration and local broker ID.
     *
     * @param config        the broker configuration
     * @param localBrokerId the local broker's ID
     */
    public SequencerOrderingService(BrokerConfig config, int localBrokerId) {
        this.config = config;
        this.localBrokerId = localBrokerId;
        // Use a distinct port for data to avoid conflict with LanDiscovery (udpPort)
        this.dataUdpPort = config.getUdpPort() + 1;
    }

    /**
     * Sets a callback to be notified of ordering service events.
     *
     * @param callback the callback to set
     */
    public void setCallback(OrderingServiceCallback callback) {
        this.callback = callback;
    }

    /**
     * Gets the current broker ID (may be updated after connecting to sequencer).
     *
     * @return the broker ID
     */
    public int getBrokerId() {
        return localBrokerId;
    }

    /**
     * Starts the ordering service.
     * If this broker is the sequencer, starts the sequencer listener and heartbeat reaper.
     * Otherwise, connects to the sequencer as a follower.
     */
    @Override
    public void start() {
        running = true;

        // Initialize UDP Socket for Data
        try {
            this.dataUdpSocket = new DatagramSocket(null);
            this.dataUdpSocket.setReuseAddress(true);
            this.dataUdpSocket.bind(new InetSocketAddress(dataUdpPort));
            this.dataUdpSocket.setBroadcast(true);
            System.out.println("[OrderingService] UDP Data channel bound to port " + dataUdpPort);
        } catch (SocketException e) {
            System.err.println("[OrderingService] Failed to bind UDP Data socket: " + e.getMessage());
        }

        if (config.isSequencer()) {
            startSequencerListener();
            startHeartbeatReaper();
        } else {
            connectToSequencer();
            // Followers must listen for UDP broadcasts
            startUdpDataListener();
        }
    }

    /**
     * Stops the ordering service and closes all network connections.
     */
    @Override
    public void stop() {
        running = false;

        // Close server socket if sequencer
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }

        // Close sequencer connection if follower
        if (sequencerSocket != null) {
            try {
                sequencerSocket.close();
            } catch (IOException ignored) {
            }
        }

        // Close UDP
        if (dataUdpSocket != null) {
            dataUdpSocket.close();
        }

        // Close all broker connections if sequencer
        synchronized (brokerConnections) {
            for (Socket socket : brokerConnections.values()) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
            brokerConnections.clear();
            brokerOutStreams.clear();
            brokerInStreams.clear();
        }
    }

    /**
     * Proposes a chat request message for global ordering.
     * If sequencer, handles locally; otherwise, forwards to sequencer.
     *
     * @param request the chat request message
     */
    @Override
    public void propose(ChatReqMessage request) {
        if (config.isSequencer()) {
            handleChatRequest(request);
        } else {
            sendToSequencer(request);
        }
    }

    /**
     * Registers a callback to be notified when messages are ready for delivery.
     *
     * @param callback the delivery callback
     */
    @Override
    public void onDeliver(Consumer<ChatDeliverMessage> callback) {
        deliveryCallbacks.add(callback);
    }

    /**
     * Checks if this broker is the leader (sequencer).
     *
     * @return true if leader, false otherwise
     */
    @Override
    public boolean isLeader() {
        return config.isSequencer();
    }

    /**
     * Gets the leader's broker ID.
     *
     * @return leader broker ID
     */
    @Override
    public int getLeaderId() {
        return config.isSequencer() ? localBrokerId : 0; // Sequencer is always broker 0
    }

    /**
     * Called by the Broker when it detects a gap in sequence numbers.
     * Sends a NACK to the Sequencer via TCP.
     */
    public void requestRetransmission(long missingSeq) {
        if (config.isSequencer()) return;

        RetransmissionRequestMessage nack = new RetransmissionRequestMessage(missingSeq);

        if (sequencerOut != null) {
            try {
                sequencerOut.writeObject(nack);
                sequencerOut.flush();
            } catch (IOException e) {
                System.err.println("[OrderingService] Failed to send NACK: " + e.getMessage());
            }
        }
    }

    // =========================================================================
    // SEQUENCER LOGIC
    // =========================================================================

    /**
     * Handle an incoming chat request by assigning a sequence number
     * and delivering to all brokers.
     *
     * @param chatReq the chat request message
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

        // Save to history for NACKs
        addToHistory(seq, deliver);

        // Deliver locally, Broadcast UDP and ACK TCP
        notifyDelivery(deliver);
        broadcastViaUdp(deliver);
        sendToBroker(chatReq.getBrokerId(), new ChatReqAck(chatReq.getLocalMsgId(), seq));
    }

    /**
     * Adds a message to the history for potential retransmission.
     *
     * @param seq the sequence number
     * @param msg the chat deliver message
     */
    private void addToHistory(long seq, ChatDeliverMessage msg) {
        messageHistory.put(seq, msg);

        // Simple cleanup
        if (seq % 100 == 0) {
            long cutoff = seq - HISTORY_SIZE;
            messageHistory.keySet().removeIf(key -> key < cutoff);
        }
    }

    /**
     * Handles a retransmission request (NACK) from a broker.
     *
     * @param brokerId   the requesting broker ID
     * @param missingSeq the missing sequence number
     */
    private void handleRetransmissionRequest(int brokerId, long missingSeq) {
        ChatDeliverMessage msg = messageHistory.get(missingSeq);
        if (msg != null) {
            System.out.println("[Reliability] Resending seq = " + missingSeq + " to broker " + brokerId + " (TCP)");
            sendToBroker(brokerId, msg);
        } else {
            System.err.println("[Reliability] Cannot satisfy NACK for seq=" + missingSeq + " (not in history)");
        }
    }

    /**
     * Notifies all registered delivery callbacks with the given message.
     *
     * @param message the chat deliver message
     */
    private void notifyDelivery(ChatDeliverMessage message) {
        for (Consumer<ChatDeliverMessage> callback : deliveryCallbacks) {
            try {
                callback.accept(message);
            } catch (Exception e) {
                System.err.println("Error in delivery callback: " + e.getMessage());
            }
        }
    }

    /**
     * Serializes the message and broadcasts it to all network interfaces.
     */
    private void broadcastViaUdp(ChatDeliverMessage message) {
        if (dataUdpSocket == null) return;

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {

            out.writeObject(message);
            out.flush();
            byte[] data = bos.toByteArray();

            // Send to all broadcast addresses found on interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() || !networkInterface.isUp()) continue;

                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress broadcast = interfaceAddress.getBroadcast();
                    if (broadcast != null) {
                        try {
                            DatagramPacket packet = new DatagramPacket(data, data.length, broadcast, dataUdpPort);
                            dataUdpSocket.send(packet);
                        } catch (IOException ignored) {
                        }
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("[Sequencer] Failed to broadcast UDP packet: " + e.getMessage());
        }
    }

    /**
     * Sends a message to a specific broker by broker ID.
     *
     * @param brokerId the broker ID
     * @param message  the message to send
     */
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
    // FOLLOWER LOGIC (UDP LISTENER)
    // =========================================================================

    private void startUdpDataListener() {
        Thread udpThread = new Thread(() -> {
            byte[] buffer = new byte[MAX_UDP_PACKET_SIZE];
            System.out.println("[OrderingService] Started UDP Data Listener...");

            while (running) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    dataUdpSocket.receive(packet);

                    try (ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData(), 0, packet.getLength());
                         ObjectInputStream in = new ObjectInputStream(bis)) {

                        Object obj = in.readObject();
                        if (obj instanceof ChatDeliverMessage deliver) {
                            notifyDelivery(deliver);
                        } else {
                            System.err.println("[OrderingService] Received unknown UDP message: " + obj);
                        }
                    } catch (ClassNotFoundException e) {
                        System.err.println("[OrderingService] Failed to deserialize UDP message: " + e.getMessage());

                    }
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[OrderingService] UDP Receive error: " + e.getMessage());
                    }
                }
            }
        }, "OrderingService-UDPListener");

        udpThread.setDaemon(true);
        udpThread.start();
    }

    // =========================================================================
    // SEQUENCER NETWORK LISTENER
    // =========================================================================

    /**
     * Starts the TCP listener for incoming broker connections (sequencer only).
     */
    private void startSequencerListener() {
        Thread listenerThread = new Thread(() -> {
            try {
                int port = config.getSequencerPort();
                serverSocket = new ServerSocket(port);
                System.out.println("[OrderingService] Sequencer listening on port " + port);

                while (running) {
                    Socket brokerSocket = serverSocket.accept();
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

    /**
     * Handles a new broker connection to the sequencer.
     *
     * @param brokerSocket the socket of the connecting broker
     */
    private void handleNewBrokerConnection(Socket brokerSocket) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(brokerSocket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(brokerSocket.getInputStream());

            // First message must be BrokerJoinMessage
            Object obj = in.readObject();
            if (!(obj instanceof BrokerJoinMessage joinMsg)) {
                brokerSocket.close();
                return;
            }

            // Assign broker ID
            HandlerState handlerState = config.getHandlerState();
            int newBrokerId = handlerState.getNewBrokerId();
            System.out.println("[OrderingService] Assigned broker ID " + newBrokerId);

            // Send acknowledgment
            out.writeObject(new BrokerJoinAck(newBrokerId, globalSeq));
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
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Starts a thread to handle incoming messages from a connected broker.
     *
     * @param brokerId the broker ID
     * @param in       the input stream from the broker
     */
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
                    } else if (msg instanceof RetransmissionRequestMessage nack) {
                        handleRetransmissionRequest(brokerId, nack.getMissingSeq());
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                removeBroker(brokerId);
            } finally {
                removeBroker(brokerId);
            }
        }, "OrderingService-BrokerHandler-" + brokerId);

        handler.setDaemon(true);
        handler.start();
    }

    /**
     * Removes a broker from the sequencer's connection lists.
     *
     * @param brokerId the broker ID to remove
     */
    private void removeBroker(int brokerId) {
        synchronized (brokerConnections) {
            Socket socket = brokerConnections.remove(brokerId);
            brokerOutStreams.remove(brokerId);
            brokerInStreams.remove(brokerId);
            lastHeartbeats.remove(brokerId);
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
        System.out.println("[OrderingService] Broker " + brokerId + " removed");
    }

    /**
     * Starts the heartbeat reaper thread to remove brokers that have timed out.
     */
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

    /**
     * Connects this broker to the sequencer as a follower.
     */
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
                    callback.onBrokerIdAssigned(ack.getBrokerId(), ack.getCurrentSequenceNumber());
                    callback.onConnectionEstablished();
                }
            }

            // Start listening for messages from sequencer
            startSequencerInboundHandler();

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[OrderingService] Failed to connect to sequencer: " + e.getMessage());
        }
    }

    /**
     * Starts a thread to handle inbound messages from the sequencer.
     */
    private void startSequencerInboundHandler() {
        Thread handler = new Thread(() -> {
            try {
                while (running) {
                    Object msg = sequencerIn.readObject();

                    if (msg instanceof ChatReqAck ack) {
                        // Received confirmation that my request was ordered
                        // System.out.println("Ack for msg: " + ack.getLocalMsgId());
                    } else if (msg instanceof ChatDeliverMessage) {
                        // Just in case we receive it via TCP (legacy fallback support?)
                        notifyDelivery((ChatDeliverMessage) msg);
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

    /**
     * Sends a chat request message to the sequencer.
     *
     * @param request the chat request message
     */
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

    /**
     * Sends a heartbeat to the sequencer (called by Broker's heartbeat loop).
     *
     * @param heartbeat the heartbeat message
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
}