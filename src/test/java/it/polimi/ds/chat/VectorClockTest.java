package it.polimi.ds.chat;

import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VectorClockTest {

    @Test
    public void happensBeforeTrueWhenAllEntriesLessOrEqualAndOneStrictlyLess() {
        VectorClock earlier = new VectorClock();
        earlier.increment(1); // {1:1}

        VectorClock later = new VectorClock(earlier);
        later.increment(1); // {1:2}

        assertTrue(earlier.happensBefore(later),
                "Clock with lower timestamp should happen before the higher one");
        assertFalse(later.happensBefore(earlier),
                "Clock with higher timestamp should not happen before the lower one");
    }

    @Test
    public void happensBeforeFalseForConcurrentEvents() {
        VectorClock clockA = new VectorClock();
        clockA.increment(1);
        clockA.increment(1); // {1:2}

        VectorClock clockB = new VectorClock();
        clockB.increment(2);
        clockB.increment(2); // {2:2}

        assertFalse(clockA.happensBefore(clockB), "Concurrent clocks should not be ordered");
        assertFalse(clockB.happensBefore(clockA), "Concurrent clocks should not be ordered");
    }

    @Test
    public void happensBeforeFalseWhenClocksEqual() {
        VectorClock first = new VectorClock();
        first.increment(1);
        first.increment(2);

        VectorClock copy = new VectorClock(first);

        assertFalse(first.happensBefore(copy),
                "Identical clocks do not have a happens-before relationship");
        assertFalse(copy.happensBefore(first),
                "Identical clocks do not have a happens-before relationship");
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

        assertEquals(1, local.getTimeStamp(1),
                "Local clock should keep its own timestamp for broker 1");
        assertEquals(2, local.getTimeStamp(2),
                "Update should take the max timestamp for broker 2");
        assertEquals(1, local.getTimeStamp(3),
                "Update should include new broker entries");
    }
}
