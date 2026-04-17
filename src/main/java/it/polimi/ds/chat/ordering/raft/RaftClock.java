package it.polimi.ds.chat.ordering.raft;

/**
 * Testable scheduling abstraction for Raft timers.
 * The election manager uses this interface instead of depending directly on
 * ScheduledExecutorService or Thread.sleep, so tests can drive timer behavior
 * deterministically.
 */
public interface RaftClock {

    /**
     * Schedules a one-shot task.
     *
     * @param delayMs delay before execution, in milliseconds
     * @param task task to execute when the timeout expires
     * @return cancellable task handle
     */
    RaftScheduledTask scheduleOnce(long delayMs, Runnable task);

    /**
     * Schedules a periodic task.
     *
     * @param initialDelayMs initial delay before the first execution
     * @param periodMs fixed period between successive executions
     * @param task task to execute periodically
     * @return cancellable task handle
     */
    RaftScheduledTask scheduleAtFixedRate(long initialDelayMs, long periodMs, Runnable task);
}
