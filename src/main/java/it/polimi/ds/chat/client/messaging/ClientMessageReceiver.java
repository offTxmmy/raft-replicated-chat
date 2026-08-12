package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.client.connection.ClientConnectionGeneration;
import it.polimi.ds.chat.protocol.client.ClientAckMessages;
import it.polimi.ds.chat.protocol.client.HeartbeatAckMessage;
import it.polimi.ds.chat.protocol.client.HeartbeatMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Receives broker responses for exactly one connection generation. */
public class ClientMessageReceiver implements Runnable {

    @FunctionalInterface
    public interface ConnectionFailureHandler {
        void onConnectionFailure();
    }

    private final ObjectInputStream input;
    private final ClientMessageSender sender;
    private final String clientId;
    private final ClientHeartbeatManager heartbeatManager;
    private final BooleanSupplier generationActive;
    private final ConnectionFailureHandler failureHandler;
    private final AtomicBoolean failureNotified = new AtomicBoolean(false);

    private volatile boolean running = true;

    /** Legacy constructor retained for a standalone input stream. */
    public ClientMessageReceiver(ObjectInputStream input,
                                 ClientMessageSender sender,
                                 String clientId,
                                 ClientHeartbeatManager heartbeatManager) {
        this(
                input,
                sender,
                clientId,
                heartbeatManager,
                () -> true,
                null
        );
    }

    public ClientMessageReceiver(ClientConnectionGeneration generation,
                                 ClientMessageSender sender,
                                 String clientId,
                                 ClientHeartbeatManager heartbeatManager,
                                 ConnectionFailureHandler failureHandler) {
        this(
                Objects.requireNonNull(generation, "generation").getInputStream(),
                sender,
                clientId,
                heartbeatManager,
                generation::isActive,
                failureHandler
        );
    }

    ClientMessageReceiver(ObjectInputStream input,
                          ClientMessageSender sender,
                          String clientId,
                          ClientHeartbeatManager heartbeatManager,
                          BooleanSupplier generationActive,
                          ConnectionFailureHandler failureHandler) {
        this.input = Objects.requireNonNull(input, "input");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.heartbeatManager = heartbeatManager;
        this.generationActive = Objects.requireNonNull(
                generationActive,
                "generationActive"
        );
        this.failureHandler = failureHandler;
    }

    /** Stops processing; the generation owner closes the socket to unblock read. */
    public void stop() {
        running = false;
    }

    /** Legacy shutdown also closes the directly owned input stream. */
    public void shutdown() {
        stop();
        try {
            input.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void run() {
        try {
            while (running && generationActive.getAsBoolean()) {
                Object object = input.readObject();
                if (!running || !generationActive.getAsBoolean()) {
                    return;
                }
                handleObject(object);
            }
        } catch (IOException | ClassNotFoundException e) {
            if (running && generationActive.getAsBoolean()) {
                System.err.println("Connection error (receiver): " + e.getMessage());
                notifyFailureOnce();
            }
        } finally {
            running = false;
        }
    }

    private void handleObject(Object object) {
        if (object instanceof HeartbeatAckMessage ack) {
            if (heartbeatManager != null) {
                heartbeatManager.onHeartbeatAck(ack.getTimestamp());
            }
            return;
        }

        if (object instanceof HeartbeatMessage heartbeat) {
            if (heartbeatManager != null) {
                heartbeatManager.onHeartbeatAck(heartbeat.getTimestamp());
            }
            return;
        }

        if (!(object instanceof String line)) {
            return;
        }

        if (!ClientAckMessages.isAck(line)) {
            System.out.println(line);
            return;
        }

        try {
            String acknowledgedClientId = ClientAckMessages.parseClientId(line);
            long clientSeq = ClientAckMessages.parseClientSeq(line);
            if (clientId.equals(acknowledgedClientId)) {
                sender.handleAck(acknowledgedClientId, clientSeq);
            }
        } catch (RuntimeException e) {
            System.err.println("Invalid ACK: " + e.getMessage());
        }
    }

    private void notifyFailureOnce() {
        if (failureHandler != null
                && failureNotified.compareAndSet(false, true)) {
            failureHandler.onConnectionFailure();
        }
    }
}
