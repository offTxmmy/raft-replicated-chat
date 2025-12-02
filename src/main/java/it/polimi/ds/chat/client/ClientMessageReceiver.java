package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientAckMessages;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Legge continuamente dal server:
 *  - se è un ACK, lo passa al MessageSender
 *  - altrimenti stampa il messaggio a video
 */
public class ClientMessageReceiver implements Runnable {

    private final BufferedReader in;
    private final ClientMessageSender sender;
    private final String username;

    private volatile boolean running = true;

    public ClientMessageReceiver(BufferedReader in, ClientMessageSender sender, String username) {
        this.in = in;
        this.sender = sender;
        this.username = username;
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

                // Gestione ACK
                if (ClientAckMessages.isAck(line)) {
                    try {
                        long ts = ClientAckMessages.parseTimestamp(line);
                        String ackUser = ClientAckMessages.parseUsername(line);

                        if (username.equals(ackUser)) {
                            sender.handleAck(ts);
                        } else {
                            // ACK per un altro utente (se il server li broadcasta)
                            // Puoi ignorarlo o loggarlo
                            //System.out.println("[INFO] ACK per utente " + ackUser + ": " + ts);
                        }
                    } catch (Exception e) {
                        System.err.println("Errore parsing ACK: " + e.getMessage());
                    }
                } else {
                    // Qualsiasi altro messaggio lo mostriamo così com'è
                    System.out.println(line);
                }
            }
        } catch (IOException e) {
            if (running) {
                System.err.println("Connection error (receiver): " + e.getMessage());
            }
        }
    }
}
