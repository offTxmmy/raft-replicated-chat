package it.polimi.ds.chat.broker;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains state for assigning new broker IDs in a thread-safe manner.
 * Used by the sequencer to generate unique broker IDs.
 */
public class HandlerState implements Serializable {
    private final AtomicInteger newBrokerId = new AtomicInteger(0);

    /**
     * Constructs a HandlerState instance.
     */
    public HandlerState () {

    }

    /**
     * Returns the next available broker ID and increments the internal counter.
     *
     * @return the next broker ID
     */
    public int getNewBrokerId() {
        return newBrokerId.getAndIncrement();
    }
}
