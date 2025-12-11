package it.polimi.ds.chat.client;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles sending user messages to the server and retransmitting messages that have not yet been
 * acknowledged (ACKed).
 * Maintains a pending message queue and manages retransmission based on ACK timeouts.
 */
public class ClientMessageSender implements Runnable {

    /**
     * Output stream verso il broker corrente.
     * Può cambiare nel tempo in caso di riconnessione a un nuovo broker.
     */
    private volatile ObjectOutputStream out;

    private final long ackTimeoutMs;
    private final Map<Long, ClientPendingMessage> pendingMessages = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    /**
     * Constructs a ClientMessageSender.
     *
     * @param out          the ObjectOutputStream to send messages to the server
     * @param ackTimeoutMs the timeout in milliseconds to wait for an ACK before retransmitting
     */
    public ClientMessageSender(ObjectOutputStream out, long ackTimeoutMs) {
        this.out = out;
        this.ackTimeoutMs = ackTimeoutMs;
    }

    /**
     * Updates the ObjectOutputStream used to send messages.
     * <p>
     * Questo metodo viene chiamato dopo una riconnessione a un nuovo broker.
     * I messaggi pendenti rimangono nella mappa e saranno ritrasmessi usando il nuovo stream.
     *
     * @param newOut the new ObjectOutputStream to use
     */
    public synchronized void updateOutputStream(ObjectOutputStream newOut) {
        System.out.println("[SEND] Aggiornato ObjectOutputStream verso nuovo broker.");
        this.out = newOut;
    }

    /**
     * Sends a user message to the server, assigning a timestamp and adding it to the pending queue.
     *
     * @param text the message text to send
     */
    public void sendUserMessage(String text) {
        long timestamp = System.currentTimeMillis();
        String wireLine = buildMsgWire(timestamp, text);

        ClientPendingMessage pm = new ClientPendingMessage(timestamp, wireLine);
        pendingMessages.put(timestamp, pm);

        ObjectOutputStream currentOut = this.out;
        if (currentOut == null) {
            System.err.println("[SEND] Impossibile inviare: stream nullo (nessun broker connesso).");
            return;
        }

        try {
            currentOut.writeObject(wireLine);
            currentOut.flush();
        } catch (IOException e) {
            System.err.println("[SEND] Errore invio messaggio: " + e.getMessage());
        }
    }

    /**
     * Builds the wire format line for the server.
     * Format: "MSG &lt;timestamp&gt; &lt;text&gt;"
     *
     * @param timestamp the message timestamp
     * @param text      the message text
     * @return the formatted wire line
     */
    private String buildMsgWire(long timestamp, String text) {
        return "MSG " + timestamp + " " + text;
    }

    /**
     * Called by the MessageReceiver when a valid ACK is received.
     * Removes the acknowledged message from the pending queue.
     *
     * @param timestamp the timestamp of the acknowledged message
     */
    public void handleAck(long timestamp) {
        ClientPendingMessage removed = pendingMessages.remove(timestamp);
        if (removed == null) {
            System.out.println("[ACK] Ricevuto ACK per timestamp sconosciuto: " + timestamp);
        }
        // Se removed != null, il messaggio è stato confermato e rimosso dai pendenti.
    }

    /**
     * Shuts down the sender thread.
     */
    public void shutdown() {
        running = false;
    }

    /**
     * Main loop for retransmitting pending messages if ACKs are not received within the timeout.
     * Retransmits messages and manages the pending queue.
     */
    @Override
    public void run() {
        try {
            while (running) {
                long now = System.currentTimeMillis();

                for (ClientPendingMessage pm : pendingMessages.values()) {
                    long elapsed = now - pm.getLastSendTime();
                    if (elapsed >= ackTimeoutMs) {
                        ObjectOutputStream currentOut = this.out;
                        if (currentOut == null) {
                            System.err.println("[RETRY] Impossibile ritrasmettere: stream nullo.");
                            continue;
                        }

                        try {
                            currentOut.writeObject(pm.getWireLine());
                            currentOut.flush();
                            pm.updateLastSendTime();
                            System.out.println("[RETRY] Ritrasmesso messaggio con timestamp " + pm.getTimestamp());
                        } catch (IOException e) {
                            System.err.println("[RETRY] Errore ritrasmissione: " + e.getMessage());
                        }
                    }
                }

                try {
                    Thread.sleep(100); // intervallo di polling
                } catch (InterruptedException e) {
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            System.err.println("[SEND] Errore nel thread sender: " + e.getMessage());
        }
    }
}
