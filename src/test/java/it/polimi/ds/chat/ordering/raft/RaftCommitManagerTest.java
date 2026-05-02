package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.ChatCommand;
import it.polimi.ds.chat.utilities.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftCommitManagerTest {

    @Test
    void advanceCommitIndexShouldApplyEntriesInOrder() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));

        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));

        commitManager.advanceCommitIndex(2L);

        assertEquals(List.of(1L, 2L), applied);
        assertEquals(2L, commitManager.getCommitIndex());
        assertEquals(2L, commitManager.getLastApplied());
    }

    @Test
    void updateCommitIndexFromLeaderShouldCapToLogTip() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(2L, command("c"));

        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));

        commitManager.updateCommitIndexFromLeader(10L);

        assertEquals(List.of(1L, 2L, 3L), applied);
        assertEquals(3L, commitManager.getCommitIndex());
    }

    @Test
    void tryAdvanceCommitIndexShouldRespectCurrentTermRule() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(2L, command("c"));

        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));

        long commitIndex = commitManager.tryAdvanceCommitIndex(List.of(2L, 2L, 2L), 2, 2L);

        assertEquals(0L, commitIndex);
        assertTrue(applied.isEmpty());
    }

    @Test
    void tryAdvanceCommitIndexShouldCommitWhenMajorityAndCurrentTermMatch() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(2L, command("c"));

        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));

        long commitIndex = commitManager.tryAdvanceCommitIndex(List.of(3L, 3L, 1L), 2, 2L);

        assertEquals(3L, commitIndex);
        assertEquals(List.of(1L, 2L, 3L), applied);
        assertEquals(3L, commitManager.getLastApplied());
    }

    private ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }
}
