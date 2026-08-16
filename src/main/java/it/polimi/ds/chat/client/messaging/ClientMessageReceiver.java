package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.client.connection.ClientConnectionGeneration;
import it.polimi.ds.chat.protocol.client.ClientAckMessages;
import it.polimi.ds.chat.protocol.client.HeartbeatAckMessage;
import it.polimi.ds.chat.protocol.client.HeartbeatMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
    private final InboundDispatcher inboundDispatcher;
    private final ConnectionFailureHandler failureHandler;
    private final Runnable beforeDispatch;
    private final Consumer<String> chatOutput;
    private final AtomicBoolean failureNotified = new AtomicBoolean(false);

    private volatile boolean running = true;

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
                generation::dispatchInboundIfActive,
                failureHandler,
                () -> { },
                System.out::println
        );
    }

    ClientMessageReceiver(ObjectInputStream input,
                          ClientMessageSender sender,
                          String clientId,
                          ClientHeartbeatManager heartbeatManager,
                          BooleanSupplier generationActive,
                          InboundDispatcher inboundDispatcher,
                          ConnectionFailureHandler failureHandler,
                          Runnable beforeDispatch,
                          Consumer<String> chatOutput) {
        this.input = Objects.requireNonNull(input, "input");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.heartbeatManager = heartbeatManager;
        this.generationActive = Objects.requireNonNull(
                generationActive,
                "generationActive"
        );
        this.inboundDispatcher = Objects.requireNonNull(
                inboundDispatcher,
                "inboundDispatcher"
        );
        this.failureHandler = failureHandler;
        this.beforeDispatch = Objects.requireNonNull(
                beforeDispatch,
                "beforeDispatch"
        );
        this.chatOutput = Objects.requireNonNull(chatOutput, "chatOutput");
    }

    /** Stops processing; the generation owner closes the socket to unblock read. */
    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            while (running && generationActive.getAsBoolean()) {
                Object object = input.readObject();
                if (!running || !generationActive.getAsBoolean()) {
                    return;
                }
                beforeDispatch.run();
                inboundDispatcher.dispatch(() -> handleObject(object));
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
            chatOutput.accept(line);
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

    @FunctionalInterface
    interface InboundDispatcher {
        boolean dispatch(Runnable action);
    }
}
