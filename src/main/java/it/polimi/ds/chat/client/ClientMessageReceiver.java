package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientAckMessages;

import java.io.BufferedReader;
import java.io.IOException;


public class ClientMessageReceiver implements Runnable {

    private final BufferedReader in;
    private final ClientMessageSender sender;
    private final String username;
    private final ClientHeartbeatManager heartbeatManager;

    private volatile boolean running = true;


    public ClientMessageReceiver(BufferedReader in,
                                 ClientMessageSender sender,
                                 String username,
                                 ClientHeartbeatManager heartbeatManager) {
        this.in = in;
        this.sender = sender;
        this.username = username;
        this.heartbeatManager = heartbeatManager;
    }


    public void shutdown() {
        running = false;
        try {
            in.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void run() {
        try {
            String line;
            while (running && (line = in.readLine()) != null) {

                if (isHeartbeatAck(line)) {
                    try {
                        long ts = parseHeartbeatTimestamp(line);
                        if (heartbeatManager != null) {
                            heartbeatManager.onHeartbeatAck(ts);
                        }
                    } catch (Exception e) {
                        System.err.println("Errore parsing HEARTBEAT_ACK: " + e.getMessage());
                    }
                    // non stampo a video l'heartbeat
                    continue;
                }

                if (ClientAckMessages.isAck(line)) {
                    try {
                        long ts = ClientAckMessages.parseTimestamp(line);
                        String ackUser = ClientAckMessages.parseUsername(line);

                        if (username.equals(ackUser)) {
                            sender.handleAck(ts);
                        } else {
                            // System.out.println("[INFO] ACK per utente " + ackUser + ": " + ts);
                        }
                    } catch (Exception e) {
                        System.err.println("Errore parsing ACK: " + e.getMessage());
                    }
                } else {
                    System.out.println(line);
                }
            }
        } catch (IOException e) {
            if (running) {
                System.err.println("Connection error (receiver): " + e.getMessage());
            }
        }
    }


    private boolean isHeartbeatAck(String line) {
        return line != null && line.startsWith("HEARTBEAT_ACK ");
    }


    private long parseHeartbeatTimestamp(String line) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Formato HEARTBEAT_ACK non valido: " + line);
        }
        return Long.parseLong(parts[1]);
    }
}
