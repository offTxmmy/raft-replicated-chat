# Comprehensive Report: migration to Raft for consensus and replication

## 1) Refactoring objective

The current architecture uses a centralized `SequencerOrderingService` (single point of failure).
To achieve fault tolerance and distributed consensus, it is recommended to introduce a
`RaftOrderingService` implementation behind the existing `OrderingService` interface.

This keeps the `Broker` almost unchanged at API level (`propose`, `onDeliver`, `isLeader`, `getLeaderId`), while changing only the internal ordering/replication logic.

---

## 2) Classes to add (concrete proposal)

Below is a proposal aligned with the current codebase, with suggested packages and responsibilities.

## 2.1 Raft core (package `it.polimi.ds.chat.ordering.raft`)

1. **`RaftOrderingService`** (implements `OrderingService`)
   - Entry point from `Broker`.
   - Receives `ChatReqMessage` via `propose(...)`.
   - If leader: appends to log and starts `AppendEntries` to followers.
   - If follower: forwards to leader (or responds with internal redirect).
   - Exposes `start/stop`, `isLeader`, `getLeaderId`.

2. **`RaftNode`**
   - Volatile Raft node state: `currentTerm`, `votedFor`, `role` (FOLLOWER/CANDIDATE/LEADER), timeouts.
   - State transition management and election timer reset.

3. **`RaftLog`**
   - Replicated entry handling: append, match by `(index, term)`, truncate on conflict.
   - Access to `lastLogIndex`, `lastLogTerm`.
   - Mapping from log entries to `ChatDeliverMessage` at commit time.

4. **`RaftCommitManager`**
   - Tracks `commitIndex` and `lastApplied`.
   - Leader side: computes commit progress from follower `matchIndex` (majority).
   - Node side: applies committed entries in order and triggers delivery callback.

5. **`RaftPeerReplicationState`**
   - Leader-side structure with `nextIndex` and `matchIndex` per peer.
   - Required for decremental retry on conflict.

6. **`RaftElectionManager`**
   - Orchestrates election timeout and heartbeat timeout.
   - Starts elections, collects votes, declares majority win.

7. **`RaftRpcServer`**
   - Endpoint for incoming Raft RPCs (`RequestVote`, `AppendEntries`).
   - Can reuse existing TCP channel or use a dedicated port.

8. **`RaftRpcClient`**
   - Sends RPCs to peers using `PeerRegistry`.
   - Retry, timeout, and peer-down handling.

9. **`RaftPersistence`** (interface) + **`FileRaftPersistence`**
   - Minimal persistence required by Raft:
     - `currentTerm`
     - `votedFor`
     - log entries
   - `FileRaftPersistence` stores data on local disk.

10. **`RaftStateMachineAdapter`**
    - Translates committed entries into application events (`ChatDeliverMessage`) toward `Broker`.
    - Isolates Raft from chat-specific logic.

## 2.2 Raft data models (package `it.polimi.ds.chat.messages.raft`)

11. **`RaftLogEntry`**
    - Minimum fields: `index`, `term`, `command` (chat payload here).

12. **`RequestVoteRequest` / `RequestVoteResponse`**
13. **`AppendEntriesRequest` / `AppendEntriesResponse`**
14. **`InstallSnapshotRequest` / `InstallSnapshotResponse`** *(phase 2)*

> Note: snapshots are optional for MVP, but useful for scalability and slow-node recovery.

## 2.3 Utility/infrastructure

15. **`RaftClock`**
    - Timer scheduling wrapper (testable, mockable).

16. **`RaftMetrics`**
    - Diagnostic counters: election count, term changes, append retry, follower lag.

---

## 3) Classes to modify

1. **`Broker`**
   - In `initializeOrderingService()`, choose implementation via config (`sequencer` vs `raft`).
   - External interaction with the rest of the system remains unchanged.

2. **`BrokerConfig`**
   - New parameters:
     - `orderingMode` (`SEQUENCER` / `RAFT`)
     - `raftElectionTimeoutMinMs`
     - `raftElectionTimeoutMaxMs`
     - `raftHeartbeatIntervalMs`
     - `raftRpcPort`
     - `raftStorageDir`

3. **`PeerRegistry`**
   - Already suitable for quorum and peer discovery.
   - Possible extension with health/latency metadata to optimize retries.

4. **`OrderingServiceCallback`** *(optional)*
   - Additional useful event: `onLeaderChanged(newLeaderId)`.

5. **`messages` package**
   - Introduce serializable Raft RPC command/DTO classes.

---

## 4) Implementation roadmap by milestone

### Milestone M1 (working Raft MVP)
- Election + heartbeat.
- AppendEntries with single entry and majority commit.
- Ordered delivery to brokers through existing callback.
- Basic persistence (`term`, `votedFor`, log).
- Core unit tests + 1 three-node integration test.

### Milestone M2 (robustness)
- Complete `nextIndex` backtracking on conflicts.
- Follower down/up recovery with catch-up.
- Client proposal redirect when node is not leader.
- More integration tests for failure scenarios.

### Milestone M3 (hardening)
- Snapshot/install snapshot.
- Metrics + structured logging.
- Timeout tuning and long-running stability tests.

---

## 5) Work split for a 3-person team

## Person A – **Consensus Core Owner**

**Main classes:**
- `RaftNode`
- `RaftElectionManager`
- `RequestVote*`
- term/role/majority logic

**Deliverables:**
- Stable election without split-brain.
- Internal API for state transitions.
- Unit tests for election and voting rules.

**Estimated workload:**
- **~35% of total**
- High complexity (concurrency + timing).

---

## Person B – **Replication & Commit Owner**

**Main classes:**
- `RaftLog`
- `RaftPeerReplicationState`
- `RaftCommitManager`
- `AppendEntries*`

**Deliverables:**
- Reliable leader→follower replication.
- Majority commit and ordered apply.
- Conflict handling and progressive retry.

**Estimated workload:**
- **~40% of total**
- Most critical part for global order correctness.

---

## Person C – **Integration, Persistence & QA Owner**

**Main classes:**
- `RaftOrderingService`
- `RaftRpcServer` / `RaftRpcClient`
- `RaftPersistence` + `FileRaftPersistence`
- changes to `Broker`, `BrokerConfig`, integration tests

**Deliverables:**
- Integration with existing `OrderingService` API.
- Minimal crash-safe persistence.
- End-to-end tests (3 nodes, leader failover, recovery).

**Estimated workload:**
- **~25% of total**
- Significant wiring and validation effort.

---

## 6) Effort estimate (person-days)

- M1: **10–14 person-days**
- M2: **6–9 person-days**
- M3: **4–6 person-days**

Total: **20–29 person-days** (depends on testing quality and required robustness level).

For a 3-person part-time course team: typically **3–5 weeks**.

---

## 7) Risk/complexity matrix by component

- Election timeout tuning: **high risk** (leader flapping).
- Log conflict resolution: **high risk** (subtle safety bugs).
- Crash recovery persistence: **medium-high risk**.
- RPC network layer (timeout/retry): **medium risk**.
- Broker integration refactor: **medium-low risk**.

---

## 8) Recommended test strategy

1. **Raft rule unit tests**
   - Vote granted/denied by term and log freshness.
   - AppendEntries reject on prevLog mismatch.

2. **3-node integration tests**
   - Single leader election.
   - Replication of N messages with identical order on all nodes.

3. **Failure tests**
   - Kill leader → new election → commit continues.
   - Rejoin old leader node as follower.

4. **Persistence tests**
   - Node restart with coherent persisted state.

---

## 9) Suggested execution sequence (week-by-week)

- **Week 1:** class skeletons + base election + RPC DTOs.
- **Week 2:** append/majority commit + apply callback.
- **Week 3:** persistence + failover tests.
- **Week 4:** hardening, tuning, final documentation.

---

## 10) Operational conclusion

To minimize risk and maximize parallelism in a 3-person team:
- separate **consensus**, **replication**, and **integration/persistence** clearly;
- lock contracts between classes early (`RaftNode`, `RaftLog`, RPC DTOs, apply callback);
- keep `Broker` dependent only on `OrderingService`, so sequencer/raft switch is possible without impact on the rest of the system.

This structure enables delivery of a correct Raft MVP and then iterative hardening without rewriting the architecture.
