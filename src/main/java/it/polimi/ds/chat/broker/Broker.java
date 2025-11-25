package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.client.ClientHandler;
import it.polimi.ds.chat.utilities.Protocol;

import java.io.IOException;
import java.io.PrintWriter;
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
 *   order their messages.
 * - If it is NOT the sequencer: connect to the sequencer and send CHAT_REQ messages.
 * - Deliver ordered messages to its local clients.
 */
public class Broker {
    // Static configuration for this broker (id, ports, isSequencer, etc.)
    private final BrokerConfig config;

    // All clients currently connected to this broker.
    // Wrapped in a synchronizedList because multiple threads (one per client)
    // will access it concurrently.
    private final List<ClientHandler> clients = Collections.synchronizedList((new ArrayList<>()));

    // Local sequence counter used to assign sequence numbers in broadcastToClients.
    // At the moment still used in fallback / local mode.
    private long nextSeq = 1;

    // Holds the global sequencing state when this broker acts as sequencer.
    private SequencerState sequencerState;

    // TCP connection from this broker to the sequencer (only used if this broker is NOT sequencer).
    private Socket sequencerSocket;
    private PrintWriter sequencerOut;

    // Local per-broker message counter to build unique localMsgId values.
    private long localMsgCounter = 0;

    /**
     * Construct a broker with the given configuration.
     * If this broker is configured as sequencer, immediately create the SequencerState.
     */
    public Broker(BrokerConfig config) {
        this.config = config;
        if(config.isSequencer()) {
            sequencerState = new SequencerState(this);
        }
    }

    /**
     * Start the broker:
     * - open TCP listener for clients,
     * - if sequencer: start a second TCP listener for brokers (CHAT_REQ),
     * - if non-sequencer: connect to the sequencer.
     */
    public void start() throws IOException {
        int port = config.getClientPort();
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker " + config.getBrokerId() + " listening for clients on port " + port);

        // If this broker is the sequencer, ensure sequencer state exists
        // and start the listener for CHAT_REQ from other brokers.
        if (config.isSequencer()) {
            if (sequencerState == null) {
                sequencerState = new SequencerState(this);
            }
            startSequencerListener();
        } else {
            // Non-sequencer: open a TCP connection to the sequencer
            // in order to send CHAT_REQ messages.
            connectToSequencer();
        }

        // Main loop: accept client TCP connections and spawn a ClientHandler for each.
        while(true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("New client connected from " + clientSocket.getRemoteSocketAddress());

            // One handler per client, running in its own thread.
            ClientHandler handler = new ClientHandler(clientSocket, this);
            clients.add(handler);

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
    }

    /**
     * Assign a sequence number to a message and deliver it to local clients.
     * Currently used in local/fallback mode; long term the sequencerState
     * should own global sequence assignment.
     */
    public void broadcastToClients(String sender, String text) {
        long seq;
        // Protect nextSeq with synchronized(this) because multiple threads
        // (client handlers) may call this concurrently.
        synchronized (this) {
            seq = nextSeq++;
        }

        // Once we have the seq, deliver to all local clients.
        onChatDeliver(seq, sender, text);
    }

    /**
     * Deliver a message that has already been assigned a global sequence number
     * to all locally connected clients.
     *
     * This is called by:
     * - broadcastToClients (local mode),
     * - SequencerState.handleChatFromBroker(...)
     * - In the future: UDP listener handling CHAT_DELIVER from the sequencer.
     */
    public void onChatDeliver(long seq, String sender, String text) {
        synchronized (clients) {
            for (ClientHandler handler : clients) {
                handler.sendMessageToClient(seq, sender, text);
            }
        }
    }

    /**
     * Entry point for messages sent by clients connected to THIS broker.
     * Decides how to handle them based on the role of the broker:
     * - If this broker is the sequencer, route the message to SequencerState
     *   so it can assign a global sequence number and deliver.
     * - If not, send a CHAT_REQ to the sequencer over TCP.
     */
    public void onClientMessage(String username, String text) {
        if(config.isSequencer()) {
            // This broker acts as the sequencer: let SequencerState assign seq.
            sequencerState.handleChatFromBroker(config.getBrokerId(), username, text);
        } else {
            // This broker is a follower: delegate ordering to the sequencer via CHAT_REQ.
            sendChatReqToSequencer(username, text);
        }
    }

    /**
     * Notify all local clients that someone left the chat.
     * Counts as a normal message with sender "[system]".
     */
    public void notifyLeave(String username) {
        broadcastToClients("[system]", username + " left the chat");
    }

    /**
     * Notify all local clients that someone joined the chat.
     * Counts as a normal message with sender "[system]".
     */
    public void notifyJoin(String username) {
        broadcastToClients("[system]", username + " joined the chat");
    }

    /**
     * Open a TCP connection to the sequencer broker.
     * This is used only if this broker is NOT the sequencer.
     * The resulting PrintWriter (sequencerOut) is used to send CHAT_REQ lines.
     */
    private void connectToSequencer() {
        try {
            System.out.println("Connecting to a sequencer at " + config.getSequencerHost() + ":" + config.getSequencerPort());
            sequencerSocket = new Socket(config.getSequencerHost(), config.getSequencerPort());

            // autoFlush = true so every println is immediately sent over the network
            sequencerOut = new PrintWriter(sequencerSocket.getOutputStream(), true);
            System.out.println("Connected to sequencer.");
        } catch (IOException e) {
            System.err.println("Failed to connect to sequencer: " + e.getMessage());
            // TODO: in a real system we might want to retry or shut down gracefully
        }
    }

    /**
     * Send a CHAT_REQ message to the sequencer when a local client sends a chat message.
     *
     * Format on the wire:
     *   CHAT_REQ <localMsgId> <brokerId> <username> <text>
     *
     * localMsgId is a per-broker unique id (e.g. "broker2-7").
     */
    public void sendChatReqToSequencer(String username, String text) {
        if (sequencerOut == null) {
            // If we don't have a connection to the sequencer, we can't enforce global order.
            // For now we fall back to local broadcast so clients still see something.
            System.err.println("No connection to sequencer; falling back to local broadcast.");
            broadcastToClients(username, text); // fallback for now
            return;
        }

        // Build a unique local message id, useful for logging/retries if needed.
        String localMsgId = config.getBrokerId() + "-" + (++localMsgCounter);

        // Build the CHAT_REQ line using the Protocol helper.
        String line = Protocol.chatReq(localMsgId, config.getBrokerId(), username, text);

        // Send it to the sequencer over TCP.
        sequencerOut.println(line);
    }

    /**
     * Start a dedicated thread that listens for CHAT_REQ connections from other brokers.
     * This is only started when this broker is the sequencer.
     *
     * For each incoming connection from a broker, we spawn a SequencerHandler
     * to read CHAT_REQ lines and feed them into SequencerState.
     */
    private void startSequencerListener() {
        new Thread(() -> {
            try {
                int port = config.getSequencerPort();
                ServerSocket serverSocket = new ServerSocket(port);
                System.out.println("Sequencer " + config.getBrokerId() + " listening for CHAT_REQ on port " + port);

                while (true) {
                    // Accept a TCP connection from a follower broker.
                    Socket brokerSocket = serverSocket.accept();
                    System.out.println("Sequencer accepted connection from broker: " + brokerSocket.getRemoteSocketAddress());

                    // Each connection is handled in a separate SequencerHandler thread.
                    SequencerHandler handler = new SequencerHandler(brokerSocket, sequencerState);

                    Thread t = new Thread(handler);
                    t.setDaemon(true);
                    t.start();
                }
            } catch (IOException e) {
                System.err.println("Sequencer listener failed: " + e.getMessage());
            }
        }, "SequencerListener-" + config.getBrokerId()).start();
    }
}