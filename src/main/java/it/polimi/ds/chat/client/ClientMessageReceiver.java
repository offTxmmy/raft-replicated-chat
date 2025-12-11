package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientAckMessages;
import it.polimi.ds.chat.messages.HeartbeatMessage;
import it.polimi.ds.chat.messages.HeartbeatAckMessage;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Receives and processes messages from the broker for the chat client.
 * Handles incoming chat messages, ACKs, and heartbeat messages.
 */
public class ClientMessageReceiver implements Runnable {

    private final ObjectInputStream in;
    private final ClientMessageSender sender;
    private final String username;
    private final ClientHeartbeatManager heartbeatManager;

    private volatile boolean running = true;

    /**
     * Constructs a ClientMessageReceiver.
     *
     * @param in               the ObjectInputStream to receive messages from the broker
     * @param sender           the ClientMessageSender to handle ACKs
     * @param username         the username of the client
     * @param heartbeatManager the heartbeat manager to notify on heartbeat ACKs
     */
    public ClientMessageReceiver(ObjectInputStream in,
                                 ClientMessageSender sender,
                                 String username,
                                 ClientHeartbeatManager heartbeatManager) {
        this.in = in;
        this.sender = sender;
        this.username = username;
        this.heartbeatManager = heartbeatManager;
    }

    /**
     * Shuts down the receiver and closes the input stream.
     */
    public void shutdown() {
        running = false;
        try {
            in.close();
        } catch (IOException ignored) {
        }
    }

    /**
     * Main loop for receiving and processing messages from the broker.
     * Handles chat messages, ACKs, and heartbeat messages.
     */
    @Override
    public void run() {
        try {
            while (running) {
                Object obj = in.readObject();
                if (obj instanceof HeartbeatAckMessage) {
                    HeartbeatAckMessage ack = (HeartbeatAckMessage) obj;

                    //System.out.println("[HB] ACK ricevuto dal broker (ts=" + ack.getTimestamp() + ", brokerId=" + ack.getBrokerId() + ")");

                    if (heartbeatManager != null) {
                        heartbeatManager.onHeartbeatAck(ack.getTimestamp());
                    }
                    continue;
                }
                if (obj instanceof HeartbeatMessage) {
                    HeartbeatMessage hb = (HeartbeatMessage) obj;
                    if (heartbeatManager != null) {
                        heartbeatManager.onHeartbeatAck(hb.getTimestamp());
                    }
                    continue;
                }
                if (obj instanceof String) {
                    String line = (String) obj;
                    if (ClientAckMessages.isAck(line)) {
                        try {
                            long ts = ClientAckMessages.parseTimestamp(line);
                            String ackUser = ClientAckMessages.parseUsername(line);

                            if (username.equals(ackUser)) {
                                sender.handleAck(ts);
                            }
                        } catch (Exception e) {
                            System.err.println("Errore parsing ACK: " + e.getMessage());
                        }
                    } else {
                        System.out.println(line);
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            if (running) {
                System.err.println("Connection error (receiver): " + e.getMessage());
            }
        }
    }
}
