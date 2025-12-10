package it.polimi.ds.chat.utilities;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * VectorClock implements a vector clock for tracking causal dependencies in distributed systems.
 * Each entry maps a broker/client id to its logical timestamp.
 */
public class VectorClock implements Serializable {
    private Map<Integer, Integer> clock; // key: broker/client id, value timestamp

    /**
     * Constructs an empty vector clock.
     */
    public VectorClock() {
        clock = new HashMap<>();
    }

    /**
     * Constructs a deep copy of another vector clock.
     *
     * @param other the vector clock to copy
     */
    public VectorClock(VectorClock other) {
        this.clock = new HashMap<>(other.clock);
    }

    /**
     * Increments the local timestamp for the given broker.
     *
     * @param brokerId the broker/client id whose timestamp to increment
     */
    public void increment(int brokerId) {
        clock.put(brokerId, clock.getOrDefault(brokerId, 0) +1);
    }

    /**
     * Updates this vector clock by merging with a received vector clock.
     * For each entry, keeps the maximum timestamp.
     *
     * @param receivedClock the received vector clock
     */
    public void update(VectorClock receivedClock) {
        for (Map.Entry<Integer, Integer> entry: receivedClock.clock.entrySet()) {
            int brokerId = entry.getKey();
            int receivedTimestamp = entry.getValue();
            clock.put(brokerId, Math.max(clock.getOrDefault(brokerId, 0), receivedTimestamp));
        }
    }

    /**
     * Determines if this vector clock happens-before another vector clock.
     *
     * @param other the other vector clock to compare to
     * @return true if this clock happens-before the other, false otherwise
     */
    public boolean happensBefore(VectorClock other) {
        boolean strictlyLess = false;
        for (Map.Entry<Integer, Integer> entry : clock.entrySet()) {
            int brokerId = entry.getKey();
            int local = entry.getValue();
            int remote = other.clock.getOrDefault(brokerId, 0);
            if (local > remote) {
                return false;
            }
            if (local < remote) {
                strictlyLess = true;
            }
        }
        for (Map.Entry<Integer, Integer> entry : other.clock.entrySet()) {
            int brokerId = entry.getKey();
            if (!clock.containsKey(brokerId) && entry.getValue() > 0) {
                strictlyLess = true;
            }
        }
        return strictlyLess;
    }

    /**
     * Returns a string representation of the vector clock.
     *
     * @return string representation of the clock
     */
    @Override
    public String toString() {
        return clock.toString();
    }

    /**
     * Gets the timestamp for a particular broker/client.
     *
     * @param brokerId the broker/client id
     * @return the timestamp for the given id, or 0 if not present
     */
    public int getTimeStamp(int brokerId) {
        return clock.getOrDefault(brokerId, 0);
    }

    /**
     * Returns an unmodifiable view of the vector clock map.
     *
     * @return the vector clock as a map
     */
    public Map<Integer, Integer> getClock() {
        return Collections.unmodifiableMap(clock);
    }
}