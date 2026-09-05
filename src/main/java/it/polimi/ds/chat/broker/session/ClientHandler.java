package it.polimi.ds.chat.broker.session;

import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.protocol.client.ClientJoinMessage;
import it.polimi.ds.chat.protocol.client.ClientQuitMessage;
import it.polimi.ds.chat.protocol.client.ClientMessage;
import it.polimi.ds.chat.protocol.client.ClientAckMessages;
import it.polimi.ds.chat.protocol.client.HeartbeatMessage;
import it.polimi.ds.chat.protocol.client.HeartbeatAckMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles a single client connection to the broker.
 * Responsible for reading commands from the client, parsing them,
 * and invoking the appropriate broker logic.
 */
public class ClientHandler implements Runnable {
    static final int DEFAULT_OUTBOUND_QUEUE_CAPACITY = 256;

    private final Socket socket;
    private final Broker broker;
    private final BlockingQueue<Object> outboundQueue;
    private final AtomicBoolean sessionClosed = new AtomicBoolean(false);
    private final AtomicBoolean joinAttempted = new AtomicBoolean(false);
    private final AtomicBoolean active = new AtomicBoolean(false);
    private volatile ObjectOutputStream out;
    private volatile Thread outboundThread;
    private String username = "anonymous";
    private String clientId;

    // Monotonic per-handler sequence number used as id for join/quit control messages.
    private final AtomicLong seqCounter = new AtomicLong(0);

    /**
     * Constructs a ClientHandler for a given client socket and broker.
     *
     * @param socket the client socket
     * @param broker the broker instance
     */
    public ClientHandler(Socket socket, Broker broker) {
        this(socket, broker, DEFAULT_OUTBOUND_QUEUE_CAPACITY);
    }

    ClientHandler(Socket socket, Broker broker, int outboundQueueCapacity) {
        this.socket = socket;
        this.broker = broker;
        if (outboundQueueCapacity <= 0) {
            throw new IllegalArgumentException("outboundQueueCapacity must be > 0");
        }
        this.outboundQueue = new ArrayBlockingQueue<>(outboundQueueCapacity);
    }

    /**
     * Returns the username associated with this client connection.
     *
     * @return the client's username
     */
    public String getUsername() {
        return username;
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * Sends an object (message or response) to the client.
     *
     * @param obj the object to send
     */
    public void sendLine(Object obj) {
        Objects.requireNonNull(obj, "obj");
        if (sessionClosed.get()) {
            return;
        }

        // Never make a Raft/application callback wait for client socket I/O. A
        // full bounded queue identifies this session as a slow consumer; only
        // this client is disconnected.
        if (!outboundQueue.offer(obj)) {
            System.err.println("Disconnecting slow client " + username
                    + ": outbound queue is full");
            closeSession();
        }
    }

    /**
     * Main loop for handling client communication.
     * Reads objects from the client, processes commands, and handles disconnects.
     */
    @Override
    public void run() {
        try (
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            startOutboundWorker(new ObjectOutputStream(socket.getOutputStream()));
            Object obj;
            while ((obj = in.readObject()) != null) {
                handleCommand(obj);
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        } finally {
            closeSession();
            broker.removeClient(this);
        }
    }

    void startOutboundWorker(ObjectOutputStream output) {
        Objects.requireNonNull(output, "output");
        synchronized (this) {
            if (outboundThread != null) {
                throw new IllegalStateException("outbound worker already started");
            }
            if (sessionClosed.get()) {
                try {
                    output.close();
                } catch (IOException ignored) {
                }
                return;
            }
            out = output;
            Thread worker = new Thread(this::outboundLoop,
                    "ClientOutbound-" + socket.getRemoteSocketAddress());
            worker.setDaemon(true);
            outboundThread = worker;
            worker.start();
        }
    }

    private void outboundLoop() {
        try {
            while (!sessionClosed.get()) {
                Object next = outboundQueue.take();
                ObjectOutputStream output = out;
                if (output == null) {
                    return;
                }
                output.writeObject(next);
                output.flush();
                output.reset();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            if (!sessionClosed.get()) {
                System.err.println("Failed to send object to client " + username
                        + ": " + e.getMessage());
            }
        } finally {
            closeSession();
        }
    }

    /** Isolates and closes this one client session. Safe to call repeatedly. */
    public void closeSession() {
        if (!sessionClosed.compareAndSet(false, true)) {
            return;
        }

        Thread worker = outboundThread;
        if (worker != null && worker != Thread.currentThread()) {
            worker.interrupt();
        }
        try {
            socket.close();
        } catch (IOException ignored) {
        }
        broker.removeClient(this);
    }

    public boolean isSessionClosed() {
        return sessionClosed.get();
    }

    /**
     * Completes JOIN activation from the ordering service's applied-boundary
     * callback. WELCOME is queued before Broker publishes this handler to its
     * fan-out set, so the first subsequent chat cannot overtake it.
     *
     * @return {@code true} if this live session was activated
     */
    public boolean activateAtDeliveryBoundary() {
        if (sessionClosed.get() || !active.compareAndSet(false, true)) {
            return false;
        }

        sendLine(ClientJoinMessage.welcome(username));
        if (sessionClosed.get()) {
            active.set(false);
            return false;
        }
        return true;
    }

    int pendingOutboundMessages() {
        return outboundQueue.size();
    }

    /**
     * Handles a single command or message object received from the client.
     *
     * @param obj the received object (String or HeartbeatMessage)
     */
    void handleCommand(Object obj) {
        if (sessionClosed.get()) return;
        if (obj instanceof String line) {
            Object msg = parseLineToMessage(line, username);
            if (msg instanceof ClientJoinMessage) {
                if (!joinAttempted.compareAndSet(false, true)) {
                    sendLine("ERROR JOIN already processed");
                    closeSession();
                    return;
                }
                username = ClientJoinMessage.parseJoin(line);
                clientId = ClientJoinMessage.parseClientId(line);
                if (!broker.activateClient(this)) {
                    closeSession();
                }
            } else if (msg instanceof ClientQuitMessage) {
                closeSession();
            } else if (msg instanceof ClientMessage) {
                ClientMessage clientMessage = (ClientMessage) msg;
                if (!active.get()) {
                    sendLine("ERROR JOIN required before MSG");
                    return;
                }
                if (clientId == null) {
                    clientId = clientMessage.getClientId();
                }
                if (broker.onClientMessage(clientMessage)) {
                    sendLine(ClientAckMessages.buildAck(
                            clientMessage.getClientId(),
                            clientMessage.getClientSeq()));
                } else {
                    // A live edge can still be unable to reach the Raft majority.
                    // Release the client to Directory failover; its FIFO head keeps
                    // the same identity because no definitive ACK was sent.
                    closeSession();
                }
            } else if (msg instanceof HeartbeatMessage) {
                HeartbeatMessage hb = (HeartbeatMessage) msg;
                HeartbeatAckMessage ack = new HeartbeatAckMessage(hb.getTimestamp(), broker.getBrokerId());
                sendLine(ack);
            } else {
                sendLine("ERROR Unknown command " + line);
            }
        } else if (obj instanceof HeartbeatMessage hb) {
            HeartbeatAckMessage ack = new HeartbeatAckMessage(hb.getTimestamp(), broker.getBrokerId());
            sendLine(ack);
        } else {
            sendLine("ERROR Unknown object command " + obj);
        }
    }

    /**
     * Parses a line of text from the client into a message object.
     *
     * @param line the input line from the client
     * @param currentUsername the username currently associated with this connection
     * @return the parsed message object, or null if unrecognized
     */
    private Object parseLineToMessage(String line, String currentUsername) {
        if (line == null) return null;
        if (ClientJoinMessage.isJoin(line)) {
            String parsed = ClientJoinMessage.parseJoin(line);
            String parsedClientId = ClientJoinMessage.parseClientId(line);
            return new ClientJoinMessage(parsed, parsedClientId, "", seqCounter.incrementAndGet());
        }
        if (ClientQuitMessage.isQuit(line)) {
            return new ClientQuitMessage(currentUsername, "", seqCounter.incrementAndGet());
        }
        if (ClientMessage.isMsg(line)) {
            return ClientMessage.fromClientLine(currentUsername, line);
        }
        if (HeartbeatMessage.isHeartbeatLine(line)) {
            long ts = HeartbeatMessage.parseTimestampFromWire(line);
            return new HeartbeatMessage(ts);
        }

        return null;
    }

    /**
     * Sends a chat message to the client with the given sequence number, sender, and text.
     *
     * @param seq the global sequence number of the message
     * @param sender the sender's username
     * @param text the message text
     */
    public void sendMessageToClient(long seq, String sender, String text) {
        if (out != null) {
            sendLine(ClientMessage.msgToClient(seq, sender, text));
        }
    }
}
