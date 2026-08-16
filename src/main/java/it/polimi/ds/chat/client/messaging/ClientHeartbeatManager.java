package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.client.connection.ClientObjectWriter;
import it.polimi.ds.chat.protocol.client.HeartbeatMessage;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/** Sends heartbeats through the shared writer of one connection generation. */
public class ClientHeartbeatManager implements Runnable {

    private static final long HEARTBEAT_INTERVAL_MS = 1_000L;
    private static final int MAX_MISSED_HEARTBEATS = 10;

    private final ClientObjectWriter writer;
    private final BooleanSupplier generationActive;
    private final HeartbeatFailureHandler failureHandler;
    private final long heartbeatIntervalMs;
    private final int maxMissedHeartbeats;
    private final AtomicLong seqCounter = new AtomicLong(0L);
    private final AtomicBoolean failureNotified = new AtomicBoolean(false);

    private volatile boolean running = true;
    private int consecutiveMissed;

    public interface HeartbeatFailureHandler {
        void onHeartbeatFailure();
    }

    public ClientHeartbeatManager(ClientObjectWriter writer,
                                  BooleanSupplier generationActive,
                                  HeartbeatFailureHandler failureHandler) {
        this(
                writer,
                generationActive,
                failureHandler,
                HEARTBEAT_INTERVAL_MS,
                MAX_MISSED_HEARTBEATS
        );
    }

    ClientHeartbeatManager(ClientObjectWriter writer,
                           BooleanSupplier generationActive,
                           HeartbeatFailureHandler failureHandler,
                           long heartbeatIntervalMs,
                           int maxMissedHeartbeats) {
        if (heartbeatIntervalMs <= 0L || maxMissedHeartbeats <= 0) {
            throw new IllegalArgumentException(
                    "Heartbeat interval and missed threshold must be positive"
            );
        }
        this.writer = Objects.requireNonNull(writer, "writer");
        this.generationActive = Objects.requireNonNull(
                generationActive,
                "generationActive"
        );
        this.failureHandler = failureHandler;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.maxMissedHeartbeats = maxMissedHeartbeats;
    }

    public void stop() {
        running = false;
    }

    public synchronized void onHeartbeatAck(long timestamp) {
        if (running && generationActive.getAsBoolean()) {
            consecutiveMissed = 0;
        }
    }

    @Override
    public void run() {
        try {
            while (running && generationActive.getAsBoolean()) {
                long sequence = seqCounter.incrementAndGet();
                writer.send(new HeartbeatMessage(sequence));

                boolean failed;
                synchronized (this) {
                    consecutiveMissed++;
                    failed = consecutiveMissed >= maxMissedHeartbeats;
                    if (failed) {
                        running = false;
                    }
                }

                if (failed) {
                    notifyFailureOnce();
                    return;
                }

                Thread.sleep(heartbeatIntervalMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (running && generationActive.getAsBoolean()) {
                System.err.println("[HB] Heartbeat failed: " + e.getMessage());
                notifyFailureOnce();
            }
        } finally {
            running = false;
        }
    }

    private void notifyFailureOnce() {
        if (failureHandler != null
                && failureNotified.compareAndSet(false, true)) {
            failureHandler.onHeartbeatFailure();
        }
    }
}
