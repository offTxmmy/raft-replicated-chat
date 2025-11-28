package it.polimi.ds.chat.broker;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class HandlerState implements Serializable {
    private final AtomicInteger newBrokerId = new AtomicInteger(0);

    public HandlerState () {

    }

    public int getNewBrokerId() {
        return newBrokerId.getAndIncrement();
    }
}
