package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the section 2 (Replication / AppendEntries / Log) fixes
 * documented in docs/CODE_ISSUES_BY_GROUP.md.
 *
 * Each test exercises one fix in isolation so a regression points to the exact
 * issue that came back.
 */
class Section2FixesTest {

    // -------------------------------------------------------------------------
    // P0 — Follower must not overstate matchIndex on empty AppendEntries.
    // -------------------------------------------------------------------------

    @Test
    void emptyAppendEntriesShouldAckOnlyPrevLogIndexNotFollowerTail() {
        // Follower already holds 3 entries from a previous (stale) leader.
        // The new leader sends a heartbeat with prevLogIndex=1 / prevLogTerm=1.
        // Pre-fix bug: follower replies matchIndex=3 (its lastLogIndex),
        // tricking the leader into believing the stale tail is replicated.
        // Post-fix: follower replies matchIndex=1 + 0 entries = 1.
        RaftNode node = followerNodeWithTerm(1, 1L);
        RaftLog log = new RaftLog();
        log.append(1L, command("a"));
        log.append(1L, command("stale-b"));
        log.append(1L, command("stale-c"));

        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = newManager(node, log, commitManager, new RecordingObserver());

        AppendEntriesRequestMessage heartbeat = new AppendEntriesRequestMessage(
                1L,
                2,
                1L,
                1L,
                List.of(),
                0L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(heartbeat);

        assertTrue(response.isSuccess());
        assertEquals(1L, response.getMatchIndex(),
                "matchIndex must reflect only what THIS RPC confirmed, not the follower's tail");
    }

    @Test
    void appendEntriesWithNewEntriesShouldAckPrevPlusEntriesSize() {
        // Empty log; leader sends prevLogIndex=0 + 2 new entries.
        // Follower must ack matchIndex=2 (prev + size), not lastLogIndex.
        RaftNode node = followerNodeWithTerm(1, 1L);
        RaftLog log = new RaftLog();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> {});
        RaftReplicationManager manager = newManager(node, log, commitManager, new RecordingObserver());

        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                1L,
                2,
                0L,
                0L,
                List.of(
                        new RaftLogEntry(1L, 1L, command("a")),
                        new RaftLogEntry(2L, 1L, command("b"))
                ),
                0L
        );

        AppendEntriesResponseMessage response = manager.handleAppendEntries(request);

        assertTrue(response.isSuccess());
        assertEquals(2L, response.getMatchIndex());
    }

    // -------------------------------------------------------------------------
    // P1 — Single-node Raft cluster must commit without peers.
    // -------------------------------------------------------------------------

    @Test
    void singleNodeClusterShouldCommitImmediatelyAfterAppend() {
        // Cluster of size 1 (only node 1). majority = 1, no peers.
        // Pre-fix: commit never advanced because no AppendEntries response ever
        // arrived. Post-fix: appendCommandAsLeader triggers advanceCommit.
        RaftNode node = leaderNode(1);
        RaftLog log = new RaftLog();
        List<Long> applied = new ArrayList<>();
        RaftCommitManager commitManager = new RaftCommitManager(log, entry -> applied.add(entry.getIndex()));

        RaftReplicationManager manager = new RaftReplicationManager(
                1,
                Set.of(1),
                node,
                log,
                commitManager,
                new RecordingSender(),
                null
        );
        manager.start();
        manager.onLeaderElected(1, node.getCurrentTerm());

        RaftLogEntry entry = manager.appendCommandAsLeader(command("solo"));

        assertNotNull(entry);
        assertEquals(1L, entry.getIndex());
        assertEquals(1L, commitManager.getCommitIndex(),
                "Single-node leader must commit its own appends without waiting for peers");
        assertEquals(List.of(1L), applied);
    }

    // -------------------------------------------------------------------------
    // P1 — RaftLog.truncateFrom must never drop committed entries.
    // -------------------------------------------------------------------------

    @Test
    void truncateFromShouldRefuseToDropCommittedEntries() {
        // Commit index advances to 2; trying to truncate at index 2 must fail.
        AtomicLong commitIndex = new AtomicLong(0L);
        RaftLog log = new RaftLog();
        log.setCommitIndexSupplier(commitIndex::get);

        log.append(1L, command("a"));
        log.append(1L, command("b"));
        log.append(1L, command("c"));

        commitIndex.set(2L);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> log.truncateFrom(2L));
        assertTrue(ex.getMessage().contains("committed"));

        // Truncating strictly above the committed prefix still works.
        log.truncateFrom(3L);
        assertEquals(2L, log.lastLogIndex());
    }

    // -------------------------------------------------------------------------
    // P1 — RaftLog.append must roll back in-memory state if persistence fails.
    // -------------------------------------------------------------------------

    @Test
    void appendShouldRollbackInMemoryWhenPersistenceFails() {
        FailingPersistence persistence = new FailingPersistence();
        RaftLog log = new RaftLog(persistence);

        // First append succeeds.
        persistence.failNext = false;
        log.append(1L, command("ok"));
        assertEquals(1L, log.lastLogIndex());

        // Second append: persistence throws. In-memory must not retain the entry.
        persistence.failNext = true;
        assertThrows(RuntimeException.class, () -> log.append(1L, command("boom")));

        assertEquals(1L, log.lastLogIndex(),
                "In-memory log must be rolled back when persistence rejects the append");
        assertEquals("ok", log.getEntry(1L).getCommand().getLocalMsgId());
    }

    // -------------------------------------------------------------------------
    // P0 — Truncation hook must fire for every entry removed by a conflict.
    // Used by RaftOrderingService to keep the dedup set in sync with the log.
    // -------------------------------------------------------------------------

    @Test
    void truncationHookShouldFireForEveryRemovedEntryOnConflict() {
        RaftLog log = new RaftLog();
        List<String> removedIds = new ArrayList<>();
        log.setTruncationHook(entry -> {
            if (entry.getCommand() != null) {
                removedIds.add(entry.getCommand().getLocalMsgId());
            }
        });

        log.append(1L, command("keep"));
        log.append(1L, command("drop-1"));
        log.append(1L, command("drop-2"));

        // Incoming entries from a new leader conflict at index 2 (term 2 != 1).
        boolean applied = log.appendEntries(1L, 1L, List.of(
                new RaftLogEntry(2L, 2L, command("replacement"))
        ));

        assertTrue(applied);
        assertEquals(List.of("drop-1", "drop-2"), removedIds);
        assertEquals(2L, log.lastLogIndex());
        assertEquals("replacement", log.getEntry(2L).getCommand().getLocalMsgId());
    }

    // -------------------------------------------------------------------------
    // P2 — VectorClock must declare a serialVersionUID so previously written
    // log files survive innocent class evolution.
    // -------------------------------------------------------------------------

    @Test
    void vectorClockShouldDeclareSerialVersionUID() throws Exception {
        java.io.ObjectStreamClass desc = java.io.ObjectStreamClass.lookup(VectorClock.class);
        assertNotNull(desc);
        assertEquals(1L, desc.getSerialVersionUID(),
                "VectorClock must declare an explicit serialVersionUID");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private RaftReplicationManager newManager(RaftNode node,
                                              RaftLog log,
                                              RaftCommitManager commitManager,
                                              RaftLeaderActivityObserver observer) {
        RaftReplicationManager manager = new RaftReplicationManager(
                node.getNodeId(),
                Set.of(node.getNodeId(), 2, 3),
                node,
                log,
                commitManager,
                new RecordingSender(),
                observer
        );
        manager.start();
        return manager;
    }

    private RaftNode leaderNode(int nodeId) {
        RaftNode node = new RaftNode(nodeId);
        node.startElection();
        node.becomeLeader();
        return node;
    }

    private RaftNode followerNodeWithTerm(int nodeId, long term) {
        RaftNode node = new RaftNode(nodeId);
        if (term > 0L) {
            node.startElection();
            node.becomeFollower(term, RaftNode.NO_LEADER);
        }
        return node;
    }

    private ChatCommand command(String localMsgId) {
        return new ChatCommand(localMsgId, 1, "alice", "msg-" + localMsgId, new VectorClock());
    }

    private static final class FailingPersistence implements RaftPersistence {
        boolean failNext = false;

        @Override
        public void persistTermAndVote(long currentTerm, Integer votedFor) {
            // not used here
        }

        @Override
        public void appendLogEntry(RaftLogEntry entry) {
            if (failNext) {
                throw new RuntimeException("simulated persistence failure");
            }
        }
    }

    private static final class RecordingSender implements RaftAppendEntriesSender {
        private final Map<Integer, List<AppendEntriesRequestMessage>> sent = new HashMap<>();

        @Override
        public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
            sent.computeIfAbsent(peerId, k -> new ArrayList<>()).add(request);
        }

        @Override
        public void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
            RaftAppendEntriesSender.super.broadcastAppendEntries(request, peerIds);
        }
    }

    private static final class RecordingObserver implements RaftLeaderActivityObserver {
        int calls;

        @Override
        public void onValidLeaderActivityObserved(long term, int leaderId) {
            calls++;
        }
    }
}