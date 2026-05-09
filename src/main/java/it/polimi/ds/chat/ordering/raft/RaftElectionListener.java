package it.polimi.ds.chat.ordering.raft;

/**
 * Optional outward callback for election-related events.
 * This keeps the election manager decoupled from broker wiring and replication
 * orchestration while still allowing upper layers to react to leadership
 * changes and heartbeat ticks.
 */
public interface RaftElectionListener {

    default void onLeaderElected(int leaderId, long term) {
        // no-op by default
    }

    default void onSteppedDown(long newTerm, int knownLeaderId) {
        // no-op by default
    }

    /**
     * Fired when valid leader activity is observed and the locally known leader id
     * changes as a result, while the local node remains (or becomes) a follower.
     * <p>This includes the transition from {@link RaftNode#NO_LEADER} to a real
     * leader id and any change of leader id within or across terms. It is NOT
     * fired on heartbeat-style repetitions of the same leader id.
     * <p>Upper layers (e.g. the ordering service) can combine this event with
     * {@link #onLeaderElected(int, long)} and {@link #onSteppedDown(long, int)}
     * to expose a unified "leader changed" notification to clients.
     */
    default void onLeaderObserved(int leaderId, long term) {
        // no-op by default
    }

    default void onHeartbeatRoundDue(long term) {
        // no-op by default
    }
}
