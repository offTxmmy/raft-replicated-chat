package it.polimi.ds.chat.ordering.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaftPeerReplicationStateTest {

    @Test
    void shouldTrackNextAndMatchIndexes() {
        RaftPeerReplicationState state = new RaftPeerReplicationState(5L);

        assertEquals(5L, state.getNextIndex());
        assertEquals(0L, state.getMatchIndex());

        state.decrementNextIndex();
        assertEquals(4L, state.getNextIndex());

        state.updateMatchIndex(3L);
        assertEquals(3L, state.getMatchIndex());

        state.updateMatchIndex(2L);
        assertEquals(3L, state.getMatchIndex());
    }

    @Test
    void decrementShouldNotGoBelowOne() {
        RaftPeerReplicationState state = new RaftPeerReplicationState(1L);

        state.decrementNextIndex();

        assertEquals(1L, state.getNextIndex());
    }
}
