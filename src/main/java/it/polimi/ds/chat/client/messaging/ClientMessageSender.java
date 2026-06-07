package it.polimi.ds.chat.client.messaging;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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

    private final String clientId;
    private final long ackTimeoutMs;
    private final Map<Long, ClientPendingMessage> pendingMessages = new ConcurrentHashMap<>();

    // Monotonic per-client sequence number used as message id (replaces wall-clock timestamp).
    private final AtomicLong seqCounter = new AtomicLong(0);

    private volatile boolean running = true;

    /**
     * Constructs a ClientMessageSender.
     *
     * @param out          the ObjectOutputStream to send messages to the server
     * @param ackTimeoutMs the timeout in milliseconds to wait for an ACK before retransmitting
     */
    public ClientMessageSender(ObjectOutputStream out, long ackTimeoutMs) {
        this(out, ackTimeoutMs, UUID.randomUUID().toString());
    }

    public ClientMessageSender(ObjectOutputStream out, long ackTimeoutMs, String clientId) {
        this.out = out;
        this.ackTimeoutMs = ackTimeoutMs;
        this.clientId = clientId;
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
     * Sends a user message to the server, assigning a client sequence and adding it to the pending queue.
     *
     * @param text the message text to send
     */
    public void sendUserMessage(String text) {
        long clientSeq = seqCounter.incrementAndGet();
        String wireLine = buildMsgWire(clientSeq, text);

        ClientPendingMessage pm = new ClientPendingMessage(clientSeq, wireLine);
        pendingMessages.put(clientSeq, pm);

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
     * Format: "MSG &lt;clientId&gt; &lt;clientSeq&gt; &lt;text&gt;"
     *
     * @param clientSeq the message sequence for this client process
     * @param text      the message text
     * @return the formatted wire line
     */
    private String buildMsgWire(long clientSeq, String text) {
        return "MSG " + clientId + " " + clientSeq + " " + text;
    }

    /**
     * Called by the MessageReceiver when a valid ACK is received.
     * Removes the acknowledged message from the pending queue.
     *
     * @param clientId the id of the acknowledged client
     * @param clientSeq the sequence of the acknowledged message
     */
    public void handleAck(String clientId, long clientSeq) {
        if (!this.clientId.equals(clientId)) {
            System.out.println("[ACK] Ignorato ACK per clientId diverso: " + clientId);
            return;
        }

        ClientPendingMessage removed = pendingMessages.remove(clientSeq);
        if (removed == null) {
            System.out.println("[ACK] Ricevuto ACK per clientSeq sconosciuto: " + clientSeq);
        }
        // Se removed != null, il messaggio è stato confermato e rimosso dai pendenti.
    }

    /**
     * Legacy ACK handler kept for tests or older protocol paths.
     */
    public void handleAck(long clientSeq) {
        handleAck(clientId, clientSeq);
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
                            System.out.println("[RETRY] Ritrasmesso messaggio con clientSeq " + pm.getClientSeq());
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
