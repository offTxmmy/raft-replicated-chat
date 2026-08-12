package it.polimi.ds.chat.client;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs at most one eventual reconnect loop with bounded exponential backoff.
 */
final class ClientReconnectManager {

    @FunctionalInterface
    interface ReconnectAttempt {
        void reconnect() throws IOException;
    }

    @FunctionalInterface
    interface BackoffWaiter {
        void await(long delayMillis) throws InterruptedException;
    }

    private final ReconnectAttempt reconnectAttempt;
    private final BackoffWaiter backoffWaiter;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);
    private final AtomicBoolean reconnectRequested = new AtomicBoolean(false);
    private final Object workerLock = new Object();

    private Thread worker;

    ClientReconnectManager(ReconnectAttempt reconnectAttempt,
                           long initialBackoffMs,
                           long maxBackoffMs) {
        this(
                reconnectAttempt,
                initialBackoffMs,
                maxBackoffMs,
                Thread::sleep
        );
    }

    ClientReconnectManager(ReconnectAttempt reconnectAttempt,
                           long initialBackoffMs,
                           long maxBackoffMs,
                           BackoffWaiter backoffWaiter) {
        if (initialBackoffMs <= 0L || maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException(
                    "Reconnect backoff must be positive and bounded"
            );
        }
        this.reconnectAttempt = Objects.requireNonNull(
                reconnectAttempt,
                "reconnectAttempt"
        );
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.backoffWaiter = Objects.requireNonNull(
                backoffWaiter,
                "backoffWaiter"
        );
    }

    /** Starts a reconnect worker unless one is already active or shutdown began. */
    boolean requestReconnect() {
        if (!running.get()) {
            return false;
        }

        /*
         * A newly installed generation may fail just before the worker that
         * installed it leaves its reconnect loop. Remember that request even
         * while a worker is active, otherwise the client could remain
         * disconnected after this narrow hand-off race.
         */
        reconnectRequested.set(true);
        if (!reconnecting.compareAndSet(false, true)) {
            return true;
        }

        synchronized (workerLock) {
            if (!running.get()) {
                reconnectRequested.set(false);
                reconnecting.set(false);
                workerLock.notifyAll();
                return false;
            }

            worker = new Thread(this::runReconnectLoop, "ClientReconnect");
            worker.setDaemon(true);
            worker.start();
            return true;
        }
    }

    /** Cancels future attempts and interrupts an in-progress backoff wait. */
    void shutdown() {
        running.set(false);
        synchronized (workerLock) {
            if (worker != null) {
                worker.interrupt();
            }
            workerLock.notifyAll();
        }
    }

    boolean isReconnecting() {
        return reconnecting.get();
    }

    /** Test/support helper that waits for the worker to become idle. */
    boolean awaitIdle(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (workerLock) {
            while (reconnecting.get()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0L) {
                    return false;
                }
                long millis = Math.max(
                        1L,
                        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                remainingNanos
                        )
                );
                workerLock.wait(millis);
            }
            return true;
        }
    }

    private void runReconnectLoop() {
        try {
            do {
                reconnectRequested.set(false);
                long delay = initialBackoffMs;
                boolean connected = false;
                while (running.get() && !connected) {
                    try {
                        reconnectAttempt.reconnect();
                        connected = true;
                    } catch (IOException e) {
                        if (!running.get()) {
                            return;
                        }

                        try {
                            backoffWaiter.await(delay);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        delay = nextDelay(delay);
                    }
                }
            } while (running.get() && reconnectRequested.get());
        } finally {
            reconnecting.set(false);
            synchronized (workerLock) {
                if (worker == Thread.currentThread()) {
                    worker = null;
                }
                workerLock.notifyAll();
            }

            /* Close the check/reset race with a request arriving in finally. */
            if (running.get() && reconnectRequested.get()) {
                requestReconnect();
            }
        }
    }

    private long nextDelay(long currentDelay) {
        if (currentDelay >= maxBackoffMs) {
            return maxBackoffMs;
        }
        if (currentDelay > maxBackoffMs / 2L) {
            return maxBackoffMs;
        }
        return Math.min(maxBackoffMs, currentDelay * 2L);
    }
}
