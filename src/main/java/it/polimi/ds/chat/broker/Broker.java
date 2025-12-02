package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.client.ClientHandler;
import it.polimi.ds.chat.messages.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Broker node in the replicated chat infrastructure.
 *
 * Responsibilities:
 * - Accept TCP connections from chat clients.
 * - If it is the sequencer: accept TCP connections from other brokers and
 *   order their messages (and assign brokerIds on join).
 * - If it is NOT the sequencer: connect to the sequencer, obtain a brokerId
 *   and send CHAT_REQ messages.
 * - Deliver ordered messages to its local clients.
 */
public class Broker implements Serializable{

    // Directory Service config (for now hardcoded)
    private static final String DIRECTORY_HOST = "localhost";
    private static final int DIRECTORY_PORT = 60000;
    private static final long HEARTBEAT_INTERVAL_MS = 3000;

    // Connection to directory service
    private transient Socket directorySocket;
    private transient ObjectOutputStream directoryOut;
    private final transient Object directoryLock = new Object();

    // Static configuration for this broker (ports, host, isSequencer, etc.)
    private final BrokerConfig config;

    // Runtime brokerId (may differ from initial config value for followers)
    private int brokerId;

    // All clients currently connected to this broker.
    private final List<ClientHandler> clients =
            Collections.synchronizedList(new ArrayList<>());

    // Local sequence counter used in fallback / local mode.
    private long nextSeq = 1;

    // Holds the global sequencing state when this broker acts as sequencer.
    private transient SequencerState sequencerState;

    // TCP connection from this broker to the sequencer (only if NOT sequencer).
    private transient Socket sequencerSocket;
    private transient ObjectOutputStream sequencerOut;
    private transient ObjectInputStream sequencerIn;

    // Local per-broker message counter to build unique localMsgId values.
    private long localMsgCounter = 0;

    /**
     * Construct a broker with the given configuration.
     * If this broker is configured as sequencer, immediately create the SequencerState.
     */
    public Broker(BrokerConfig config) {
        this.config = config;
        this.brokerId = config.getBrokerId(); // 0 for leader, -1 for followers at startup

        if(config.isSequencer()) {
            // The leader must have a HandlerState (AtomicInteger) to generate broker IDs
            if (config.getHandlerState() == null) {
                throw new IllegalArgumentException("Sequencer broker must have a HandleState");
            }
            sequencerState = new SequencerState(this);
        }
    }

    public int getBrokerId() {
        return brokerId;
    }

    /**
     * Start the broker:
     * - open TCP listener for clients,
     * - if sequencer: start a second TCP listener for brokers (CHAT_REQ),
     * - if non-sequencer: connect to the sequencer.
     */
    public void start() throws IOException {
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
     */
    public void onClientMessage(ClientMessage message) {
        if (config.isSequencer()) {
            // If this broker is the sequencer, handle the message directly
            String localMsgId = brokerId + "-" + (++localMsgCounter);
            ChatReqMessage chatReq = new ChatReqMessage(
                    localMsgId,
                    brokerId,
                    message.getUsername(),
                    message.getText()
            );

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

                    synchronized (directoryLock) {
                        directoryOut.writeObject(hb);
                        directoryOut.flush();
                    }

                    // System.out.println("Sent heartbeat: " + hb);
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
            onChatDeliver(chatDeliver.getSeq(), chatDeliver.getUsername(), chatDeliver.getText());
        } else {
            System.out.println("Received broker message from sequencer: " + message.getClass().getSimpleName());
        }
    }

    /**
     * Send a ChatReqMessage to the sequencer when a local client sends a chat message.
     */
    public void sendChatReqToSequencer(String username, String text, long timestamp) {
        if (sequencerOut == null) {
            System.err.println("No connection to sequencer; falling back to local broadcast.");
            broadcastToClients(username, text);
            sendAckToClient(username, timestamp);
            return;
        }

        String localMsgId = brokerId + "-" + (++localMsgCounter);
        ChatReqMessage msg = new ChatReqMessage(localMsgId, brokerId, username, text);

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

    /* =========================================================
       2) SEQUENCER SIDE: listen for brokers (JOIN + CHAT_REQ)
       ========================================================= */

    /**
     * Start a dedicated thread that listens for connections from other brokers.
     * First message on each connection is expected to be BrokerJoinMessage;
     * the sequencer responds with ASSIGN_ID using HandlerState (AtomicInteger),
     * then a SequencerHandler handles ChatReqMessage messages on the same socket.
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