package it.polimi.ds.chat.ordering.raft;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Production {@link RaftClock} backed by a {@link ScheduledExecutorService}.
 *
 * <p>Used by {@link RaftElectionManager} to schedule the election timeout
 * and the leader heartbeat tick. Tests typically use a fake clock instead.
 *
 * <p>Threads are daemon: a JVM shutdown does not wait on Raft timers.
 */
public final class DefaultRaftClock implements RaftClock {

    private final ScheduledExecutorService scheduler;

    public DefaultRaftClock() {
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raft-clock");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public RaftScheduledTask scheduleOnce(long delayMs, Runnable task) {
        ScheduledFuture<?> future = scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public RaftScheduledTask scheduleAtFixedRate(long initialDelayMs, long periodMs, Runnable task) {
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                task, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    /**
     * Shuts down the scheduler. After this call any subsequent {@code schedule*}
     * call will silently fail (returned tasks will never fire).
     */
    public void shutdown() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}