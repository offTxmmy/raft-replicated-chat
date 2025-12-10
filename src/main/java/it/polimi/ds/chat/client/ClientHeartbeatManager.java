package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.HeartbeatMessage;

import java.io.ObjectOutputStream;
import java.io.PrintWriter;

/**
 * Gestisce l'heartbeat lato client.
 *
 * - Invia HEARTBEAT <timestamp> al broker ogni HEARTBEAT_INTERVAL_MS.
 * - Si aspetta che il broker risponda con HEARTBEAT_ACK <stessoTimestamp>.
 * - Se vengono persi MAX_MISSED_HEARTBEATS consecutivi, invoca la callback
 *   di failure (es. per scatenare la riconnessione via DirectoryService).
 */
public class ClientHeartbeatManager implements Runnable {

    // Intervallo tra un heartbeat e l'altro
    private static final long HEARTBEAT_INTERVAL_MS = 1000L; // 1 secondo

    // Numero massimo di heartbeat consecutivi mancati
    private static final int MAX_MISSED_HEARTBEATS = 10;

    private final ObjectOutputStream out;
    private final HeartbeatFailureHandler failureHandler;

    private volatile boolean running = true;

    // contatore di heartbeat consecutivi non confermati
    private int consecutiveMissed = 0;


    public interface HeartbeatFailureHandler {
        void onHeartbeatFailure();
    }


    public ClientHeartbeatManager(ObjectOutputStream out, HeartbeatFailureHandler failureHandler) {
        this.out = out;
        this.failureHandler = failureHandler;
    }


    public void stop() {
        running = false;
    }


    public synchronized void onHeartbeatAck(long timestamp) {
        // System.out.println("[HB] ACK ricevuto per heartbeat " + timestamp);
        consecutiveMissed = 0;
    }

    @Override
    public void run() {
        try {
            while (running) {
                long ts = System.currentTimeMillis();

                HeartbeatMessage hb = new HeartbeatMessage(ts);

                out.writeObject(hb);
                boolean triggerFailure = false;
                synchronized (this) {
                    consecutiveMissed++;
                    if (consecutiveMissed >= MAX_MISSED_HEARTBEATS) {
                        triggerFailure = true;
                        running = false;
                    }
                }

                if (triggerFailure) {
                    System.err.println("[HB] Raggiunta soglia di " + MAX_MISSED_HEARTBEATS
                            + " heartbeat mancati. Broker sospetto CRASHATO.");
                    if (failureHandler != null) {
                        failureHandler.onHeartbeatFailure();
                    }
                    break;
                }

                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                } catch (InterruptedException e) {
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            System.err.println("[HB] Errore nel thread heartbeat: " + e.getMessage());
        }
    }
}
