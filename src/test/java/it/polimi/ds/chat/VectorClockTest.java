package it.polimi.ds.chat;

import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.Test;

import static org.junit.Assert.*;

public class VectorClockTest {

    @Test
    public void happensBeforeTrueWhenAllEntriesLessOrEqualAndOneStrictlyLess() {
        VectorClock earlier = new VectorClock();
        earlier.increment(1); // {1:1}

        VectorClock later = new VectorClock(earlier);
        later.increment(1); // {1:2}

        assertTrue("Clock with lower timestamp should happen before the higher one", earlier.happensBefore(later));
        assertFalse("Clock with higher timestamp should not happen before the lower one", later.happensBefore(earlier));
    }

    @Test
    public void happensBeforeFalseForConcurrentEvents() {
        VectorClock clockA = new VectorClock();
        clockA.increment(1);
        clockA.increment(1); // {1:2}

        VectorClock clockB = new VectorClock();
        clockB.increment(2);
        clockB.increment(2); // {2:2}

        assertFalse("Concurrent clocks should not be ordered", clockA.happensBefore(clockB));
        assertFalse("Concurrent clocks should not be ordered", clockB.happensBefore(clockA));
    }

    @Test
    public void happensBeforeFalseWhenClocksEqual() {
        VectorClock first = new VectorClock();
        first.increment(1);
        first.increment(2);

        VectorClock copy = new VectorClock(first);

        assertFalse("Identical clocks do not have a happens-before relationship", first.happensBefore(copy));
        assertFalse("Identical clocks do not have a happens-before relationship", copy.happensBefore(first));
    }

    @Test
    public void updateMergesMaxTimestampsFromReceivedClock() {
        VectorClock local = new VectorClock();
        local.increment(1); // {1:1}
        local.increment(2); // {1:1, 2:1}

        VectorClock incoming = new VectorClock();
        incoming.increment(2);
        incoming.increment(2); // {2:2}
        incoming.increment(3); // {2:2, 3:1}

        local.update(incoming);

        assertEquals("Local clock should keep its own timestamp for broker 1", 1, local.getTimeStamp(1));
        assertEquals("Update should take the max timestamp for broker 2", 2, local.getTimeStamp(2));
        assertEquals("Update should include new broker entries", 1, local.getTimeStamp(3));
    }
}
