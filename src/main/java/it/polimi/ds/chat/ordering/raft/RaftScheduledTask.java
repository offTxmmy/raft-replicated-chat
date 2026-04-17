package it.polimi.ds.chat.ordering.raft;

/**
 * Handle returned by {@link RaftClock} for a scheduled task.
 * The election manager uses it to cancel timeouts and heartbeat loops.
 */
public interface RaftScheduledTask {

    /**
     * Cancels the scheduled task.
     */
    void cancel();
}
