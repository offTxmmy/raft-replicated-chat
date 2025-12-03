package it.polimi.ds.chat.utilities;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class VectorClock implements Serializable {
    private Map<Integer, Integer> clock; // key: broker/client id, value timestamp

    public VectorClock() {
        clock = new HashMap<>();
    }

    public VectorClock(VectorClock other) {
        this.clock = new HashMap<>(other.clock);
    }

    // Increment the local timestamp for the current broker
    public void increment(int brokerId) {
        clock.put(brokerId, clock.getOrDefault(brokerId, 0) +1);
    }

    // Update the vector clock based on a received clock
    public void update(VectorClock receivedClock) {
        for (Map.Entry<Integer, Integer> entry: receivedClock.clock.entrySet()) {
            int brokerId = entry.getKey();
            int receivedTimestamp = entry.getValue();
            clock.put(brokerId, Math.max(clock.getOrDefault(brokerId, 0), receivedTimestamp));
        }
    }

    // Compare if this clock happens-before the received clock
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

    @Override
    public String toString() {
        return clock.toString();
    }

    // Get the timestamp for a particular broker (for comparison)
    public int getTimeStamp(int brokerId) {
        return clock.getOrDefault(brokerId, 0);
    }

    // Get the vector clock map
    public Map<Integer, Integer> getClock() {
        return Collections.unmodifiableMap(clock);
    }
}