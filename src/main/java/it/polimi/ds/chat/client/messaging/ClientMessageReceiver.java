package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.protocol.client.ClientAckMessages;
import it.polimi.ds.chat.protocol.client.HeartbeatMessage;
import it.polimi.ds.chat.protocol.client.HeartbeatAckMessage;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Receives and processes messages from the broker for the chat client.
 * Handles incoming chat messages, ACKs, and heartbeat messages.
 */
public class ClientMessageReceiver implements Runnable {

    private final ObjectInputStream in;
    private final ClientMessageSender sender;
    private final String clientId;
    private final ClientHeartbeatManager heartbeatManager;

    private volatile boolean running = true;

    /**
     * Constructs a ClientMessageReceiver.
     *
     * @param in               the ObjectInputStream to receive messages from the broker
     * @param sender           the ClientMessageSender to handle ACKs
     * @param clientId         the stable id of this client process
     * @param heartbeatManager the heartbeat manager to notify on heartbeat ACKs
     */
    public ClientMessageReceiver(ObjectInputStream in,
                                 ClientMessageSender sender,
                                 String clientId,
                                 ClientHeartbeatManager heartbeatManager) {
        this.in = in;
        this.sender = sender;
        this.clientId = clientId;
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
                            String ackClientId = ClientAckMessages.parseClientId(line);
                            long clientSeq = ClientAckMessages.parseClientSeq(line);

                            if (clientId.equals(ackClientId)) {
                                sender.handleAck(ackClientId, clientSeq);
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
