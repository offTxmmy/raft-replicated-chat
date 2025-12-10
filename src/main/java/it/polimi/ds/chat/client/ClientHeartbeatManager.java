package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.HeartbeatMessage;

import java.io.ObjectOutputStream;
import java.io.PrintWriter;

/**
 * Manages the heartbeat mechanism on the client side.
 * <p>
 * Periodically sends a {@link HeartbeatMessage} to the broker and expects an ACK in response.
 * If a configured number of consecutive heartbeats are missed, triggers a failure callback.
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


    /**
     * Handler interface for heartbeat failure events.
     */
    public interface HeartbeatFailureHandler {
        /**
         * Called when the maximum number of missed heartbeats is reached.
         */
        void onHeartbeatFailure();
    }


    /**
     * Constructs a ClientHeartbeatManager.
     *
     * @param out            the ObjectOutputStream to send heartbeat messages to the broker
     * @param failureHandler the handler to invoke on heartbeat failure
     */
    public ClientHeartbeatManager(ObjectOutputStream out, HeartbeatFailureHandler failureHandler) {
        this.out = out;
        this.failureHandler = failureHandler;
    }


    /**
     * Stops the heartbeat manager.
     */
    public void stop() {
        running = false;
    }


    /**
     * Resets the missed heartbeat counter upon receiving an ACK for a heartbeat.
     *
     * @param timestamp the timestamp of the acknowledged heartbeat
     */
    public synchronized void onHeartbeatAck(long timestamp) {
        // System.out.println("[HB] ACK ricevuto per heartbeat " + timestamp);
        consecutiveMissed = 0;
    }

    /**
     * Main loop for sending heartbeat messages and monitoring ACKs.
     * Triggers the failure handler if too many heartbeats are missed.
     */
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
