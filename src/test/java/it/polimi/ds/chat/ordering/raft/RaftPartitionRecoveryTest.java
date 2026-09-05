package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.common.clock.VectorClock;
import it.polimi.ds.chat.protocol.raft.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Component integration: production election, replication, log and commit paths,
 * with deterministic asynchronous message omission and a virtual scheduler.
 * Socket/serialization behavior is covered separately by transport integration tests.
 */
class RaftPartitionRecoveryTest {
    @Test
    void repeatedLiveFollowerIsolationKeepsMajorityStableAndCatchesUpWithoutRestart() {
        Cluster cluster = new Cluster();
        cluster.advance(1000);
        assertEquals(2, cluster.singleLeader());
        long stableTerm = cluster.nodes[2].node.getCurrentTerm();
        cluster.propose(2, "before");
        cluster.advance(100);
        cluster.assertLogsAndCommitsEqual();

        for (int cycle = 0; cycle < 3; cycle++) {
            Stack isolated = cluster.nodes[0];
            long committedBefore = isolated.commit.getCommitIndex();
            int electionsBefore = isolated.persistedTerms.size();
            cluster.isolated = 0;
            cluster.propose(2, "during-" + cycle);
            cluster.advance(15_000); // 100 local timeouts, process stays alive.

            assertEquals(stableTerm, isolated.node.getCurrentTerm());
            assertEquals(electionsBefore, isolated.persistedTerms.size(), "isolated timeouts must not persist a term/vote");
            assertFalse(isolated.node.isLeader());
            assertNull(isolated.replication.appendCommandAsLeader(command("cannot-commit-alone")));
            assertEquals(committedBefore, isolated.commit.getCommitIndex());
            assertEquals(2, cluster.singleLeader());
            assertEquals(stableTerm, cluster.nodes[2].node.getCurrentTerm());
            assertTrue(cluster.nodes[2].commit.getCommitIndex() > committedBefore);
            assertEquals(cluster.nodes[2].commit.getCommitIndex(), cluster.nodes[1].commit.getCommitIndex());

            cluster.isolated = -1;
            // Deliver a rejoin PreVote before catch-up to exercise the healthy
            // majority's leader-contact guard, rather than relying on lucky ordering.
            isolated.election.onElectionTimeoutFired();
            cluster.advance(1000);
            assertEquals(2, cluster.singleLeader());
            assertEquals(stableTerm, cluster.nodes[2].node.getCurrentTerm());
            cluster.assertLogsAndCommitsEqual();
            cluster.propose(2, "after-" + cycle);
            cluster.advance(1000);
            cluster.assertLogsAndCommitsEqual();
        }
        assertEquals(1, cluster.nodes[2].leaderships);
        assertEquals(0, cluster.nodes[0].leaderships);
        assertEquals(0, cluster.nodes[1].leaderships);
    }

    @Test
    void isolatedOldLeaderCannotCommitAndRejoinsNewMajorityLeaderWithLogRepair() {
        Cluster cluster = new Cluster();
        cluster.advance(1000);
        assertEquals(2, cluster.singleLeader());
        cluster.propose(2, "committed-before");
        cluster.advance(100);
        long oldCommit = cluster.nodes[2].commit.getCommitIndex();
        cluster.isolated = 2;
        cluster.propose(2, "uncommitted-old-leader");
        cluster.advance(3000);
        assertEquals(oldCommit, cluster.nodes[2].commit.getCommitIndex());
        int newLeader = cluster.nodes[0].node.isLeader() ? 0 : 1;
        assertTrue(cluster.nodes[newLeader].node.isLeader());
        long newTerm = cluster.nodes[newLeader].node.getCurrentTerm();
        assertTrue(newTerm > cluster.nodes[2].node.getCurrentTerm());
        cluster.propose(newLeader, "new-majority-command");
        cluster.advance(1000);
        assertTrue(cluster.nodes[newLeader].commit.getCommitIndex() > oldCommit);

        cluster.isolated = -1;
        cluster.advance(3000);
        assertEquals(newLeader, cluster.singleLeader());
        assertEquals(newTerm, cluster.nodes[newLeader].node.getCurrentTerm());
        cluster.assertLogsAndCommitsEqual();
        assertTrue(cluster.nodes[2].applied.stream().noneMatch(e -> e.getCommand() != null
                && e.getCommand().getLocalMsgId().equals("uncommitted-old-leader")));
        cluster.propose(newLeader, "after-leader-rejoin");
        cluster.advance(1000);
        cluster.assertLogsAndCommitsEqual();
    }

    private static ChatCommand command(String id) {
        return new ChatCommand(id, 0, "alice", id, new VectorClock());
    }

    private static class Cluster {
        final PriorityQueue<Event> queue = new PriorityQueue<>();
        final Stack[] nodes = new Stack[3];
        long now;
        long sequence;
        int isolated = -1;

        Cluster() {
            for (int i = 0; i < 3; i++) nodes[i] = new Stack(this, i);
            for (Stack node : nodes) { node.replication.start(); node.election.start(); }
        }

        Event schedule(long delay, long period, Runnable action) {
            Event event = new Event(now + delay, ++sequence, period, action);
            queue.add(event);
            return event;
        }

        void send(int from, int to, Runnable delivery) {
            if (from == isolated || to == isolated) return;
            schedule(1, 0, () -> {
                if (from != isolated && to != isolated) delivery.run();
            });
        }

        void advance(long millis) {
            long end = now + millis;
            int executed = 0;
            while (!queue.isEmpty() && queue.peek().time <= end) {
                Event event = queue.remove();
                now = event.time;
                if (event.cancelled) continue;
                assertTrue(++executed < 100_000, "virtual event storm");
                event.action.run();
                if (!event.cancelled && event.period > 0) {
                    event.time += event.period;
                    queue.add(event);
                }
            }
            now = end;
        }

        int singleLeader() {
            List<Integer> leaders = new ArrayList<>();
            for (Stack stack : nodes) if (stack.node.isLeader()) leaders.add(stack.node.getNodeId());
            assertEquals(1, leaders.size(), "leaders=" + leaders);
            return leaders.get(0);
        }

        void propose(int leader, String id) {
            assertNotNull(nodes[leader].replication.appendCommandAsLeader(command(id)));
        }

        void assertLogsAndCommitsEqual() {
            Stack expected = nodes[0];
            for (Stack actual : nodes) {
                assertEquals(expected.log.lastLogIndex(), actual.log.lastLogIndex());
                assertEquals(expected.commit.getCommitIndex(), actual.commit.getCommitIndex());
                assertEquals(actual.log.lastLogIndex(), actual.commit.getCommitIndex());
                assertEquals(expected.applied.size(), actual.applied.size());
                for (int i = 0; i < expected.applied.size(); i++) {
                    RaftLogEntry a = expected.applied.get(i);
                    RaftLogEntry b = actual.applied.get(i);
                    assertEquals(a.getIndex(), b.getIndex());
                    assertEquals(a.getTerm(), b.getTerm());
                    assertEquals(a.getCommand() == null ? null : a.getCommand().getLocalMsgId(),
                            b.getCommand() == null ? null : b.getCommand().getLocalMsgId());
                }
            }
        }
    }

    private static class Stack implements RaftClock, RaftVoteRequestSender, RaftPreVoteRequestSender, RaftAppendEntriesSender {
        final Cluster cluster;
        final int id;
        final List<Long> persistedTerms = new ArrayList<>();
        final List<RaftLogEntry> applied = new ArrayList<>();
        final RaftNode node;
        final RaftLog log = new RaftLog();
        final RaftCommitManager commit = new RaftCommitManager(log, applied::add);
        final RaftElectionManager election;
        final RaftReplicationManager replication;
        int leaderships;

        Stack(Cluster cluster, int id) {
            this.cluster = cluster;
            this.id = id;
            node = new RaftNode(id, (term, vote) -> persistedTerms.add(term));
            replication = new RaftReplicationManager(id, Set.of(0, 1, 2), node, log, commit, this,
                    this::leaderActivity, this::higherTerm);
            long timeout = id == 2 ? 100 : 150 + 50L * id;
            election = new RaftElectionManager(id, Set.of(0, 1, 2), timeout, timeout, 20,
                    node, log, this, this, this, new RaftElectionListener() {
                @Override public void onLeaderElected(int leader, long term) {
                    leaderships++;
                    replication.onLeaderElected(leader, term);
                    replication.appendCommandAsLeader(null);
                }
                @Override public void onSteppedDown(long term, int leader) { replication.onSteppedDown(term, leader); }
                @Override public void onHeartbeatRoundDue(long term) { replication.onHeartbeatRoundDue(term); }
            });
        }

        void leaderActivity(long term, int leader) { election.onValidLeaderActivityObserved(term, leader); }
        void higherTerm(long term) { election.onHigherTermObserved(term); }
        @Override public long nanoTime() { return TimeUnit.MILLISECONDS.toNanos(cluster.now); }
        @Override public RaftScheduledTask scheduleOnce(long delay, Runnable action) { return cluster.schedule(delay, 0, action); }
        @Override public RaftScheduledTask scheduleAtFixedRate(long delay, long period, Runnable action) { return cluster.schedule(delay, period, action); }

        @Override public void sendPreVote(int peer, PreVoteRequestMessage request) {
            cluster.send(id, peer, () -> {
                PreVoteResponseMessage response = cluster.nodes[peer].election.onPreVoteRequest(request);
                cluster.send(peer, id, () -> election.onPreVoteResponse(response));
            });
        }
        @Override public void sendRequestVote(int peer, RequestVoteRequestMessage request) {
            cluster.send(id, peer, () -> {
                RequestVoteResponseMessage response = cluster.nodes[peer].election.onRequestVoteRequest(request);
                cluster.send(peer, id, () -> election.onRequestVoteResponse(response));
            });
        }
        @Override public void sendAppendEntries(int peer, AppendEntriesRequestMessage request) {
            cluster.send(id, peer, () -> {
                AppendEntriesResponseMessage response = cluster.nodes[peer].replication.handleAppendEntries(request);
                cluster.send(peer, id, () -> replication.handleAppendEntriesResponse(peer, response));
            });
        }
    }

    private static class Event implements RaftScheduledTask, Comparable<Event> {
        long time;
        final long sequence;
        final long period;
        final Runnable action;
        boolean cancelled;
        Event(long time, long sequence, long period, Runnable action) {
            this.time = time; this.sequence = sequence; this.period = period; this.action = action;
        }
        @Override public void cancel() { cancelled = true; }
        @Override public int compareTo(Event other) {
            int timeOrder = Long.compare(time, other.time);
            return timeOrder != 0 ? timeOrder : Long.compare(sequence, other.sequence);
        }
    }
}
