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

    default void onHeartbeatRoundDue(long term) {
        // no-op by default
    }
}
