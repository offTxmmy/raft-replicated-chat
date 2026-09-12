package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.ChatCommand;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    @Test
    void restoredCommitProgressShouldResumeFromPersistedState() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(2L, command("c"));

        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(
                log,
                entry -> applied.add(entry.getIndex()),
                3L,
                1L,
                (commitIndex, lastApplied) -> { }
        );

        assertEquals(List.of(2L, 3L), applied);
        assertEquals(3L, commitManager.getCommitIndex());
        assertEquals(3L, commitManager.getLastApplied());
    }

    @Test
    void failedApplicationMustPropagateWithoutAdvancingLastApplied() {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        RaftCommitManager commitManager = new RaftCommitManager(
                log,
                entry -> { throw new IllegalStateException("delivery failed"); }
        );

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> commitManager.advanceCommitIndex(1L)
        );

        assertEquals("delivery failed", failure.getMessage());
        assertEquals(1L, commitManager.getCommitIndex());
        assertEquals(0L, commitManager.getLastApplied(),
                "a failed callback must not be recorded as applied");
    }

    @Test
    void localBoundaryWaitsForAnInProgressApplication() throws Exception {
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        CountDownLatch applyEntered = new CountDownLatch(1);
        CountDownLatch releaseApply = new CountDownLatch(1);
        CountDownLatch boundaryEntered = new CountDownLatch(1);
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {
            applyEntered.countDown();
            try {
                assertTrue(releaseApply.await(2, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });

        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            var apply = workers.submit(() -> commitManager.advanceCommitIndex(1L));
            assertTrue(applyEntered.await(1, TimeUnit.SECONDS));
            var boundary = workers.submit(() -> commitManager.executeAtApplyBoundary(
                    boundaryEntered::countDown));

            assertFalse(boundaryEntered.await(100, TimeUnit.MILLISECONDS),
                    "boundary crossed while committed apply was still running");
            releaseApply.countDown();
            apply.get(1, TimeUnit.SECONDS);
            boundary.get(1, TimeUnit.SECONDS);
            assertEquals(0L, boundaryEntered.getCount());
        } finally {
            releaseApply.countDown();
            workers.shutdownNow();
        }
    }

    private ChatCommand command(String clientId) {
        return new ChatCommand("alice", clientId, 1L, "msg-" + clientId);
    }
}
