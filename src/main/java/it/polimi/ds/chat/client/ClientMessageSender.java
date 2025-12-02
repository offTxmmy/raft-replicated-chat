package it.polimi.ds.chat.client;

import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gestisce:
 *  - l'invio dei messaggi utente al server
 *  - la ritrasmissione dei messaggi non ancora ACKati
 */
public class ClientMessageSender implements Runnable {

    private final PrintWriter out;
    private final long ackTimeoutMs;
    private final Map<Long, ClientPendingMessage> pendingMessages = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    public ClientMessageSender(PrintWriter out, long ackTimeoutMs) {
        this.out = out;
        this.ackTimeoutMs = ackTimeoutMs;
    }

    /**
     * Invia un messaggio utente al server, assegnando un timestamp
     * e inserendolo tra i pendenti.
     */
    public void sendUserMessage(String text) {
        long timestamp = System.currentTimeMillis();
        String wireLine = buildMsgWire(timestamp, text);

        ClientPendingMessage pm = new ClientPendingMessage(timestamp, wireLine);
        pendingMessages.put(timestamp, pm);

        out.println(wireLine);
        //System.out.println("[SEND] (" + timestamp + ") " + text);
    }

    /**
     * Costruisce la linea per il server.
     * Formato deciso: "MSG <timestamp> <text>"
     */
    private String buildMsgWire(long timestamp, String text) {
        return "MSG " + timestamp + " " + text;
    }

    /**
     * Chiamato dal MessageReceiver quando arriva un ACK valido.
     */
    public void handleAck(long timestamp) {
        ClientPendingMessage removed = pendingMessages.remove(timestamp);
        if (removed != null) {
            //System.out.println("[ACK] Confermato messaggio con timestamp " + timestamp);
        } else {
            System.out.println("[ACK] Ricevuto ACK per timestamp sconosciuto: " + timestamp);
        }
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void run() {
        try {
            while (running) {
                long now = System.currentTimeMillis();

                for (ClientPendingMessage pm : pendingMessages.values()) {
                    long elapsed = now - pm.getLastSendTime();
                    if (elapsed >= ackTimeoutMs) {
                        // Ritrasmissione
                        out.println(pm.getWireLine());
                        pm.updateLastSendTime();
                        System.out.println("[RETRY] Ritrasmesso messaggio con timestamp " + pm.getTimestamp());
                    }
                }

                try {
                    Thread.sleep(100); // intervallo di polling
                } catch (InterruptedException e) {
                    // interrompiamo il thread
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            System.err.println("Errore nel MessageSender: " + e.getMessage());
        }
    }
}
