# Comprehensive Report: migration to Raft for consensus and replication

## 1) Refactoring objective

The current architecture uses a centralized `SequencerOrderingService`, which is a single point of failure.
To achieve fault tolerance and distributed consensus, the system should introduce a
`RaftOrderingService` implementation behind the existing `OrderingService` interface.

This keeps the `Broker` almost unchanged at API level (`propose`, `onDeliver`, `isLeader`, `getLeaderId`), while replacing only the internal ordering and replication logic.

The main architectural goal is:

- keep `Broker` dependent only on `OrderingService`
- replace the centralized sequencer with Raft for total order and fault tolerance
- preserve clear separation between:
    - **consensus/election**
    - **replication/commit**
    - **integration/network/persistence**

---

## 2) Agreed implementation principles

The team agreed to keep the first Raft version as a **working MVP**:

- fixed broker IDs
- static cluster membership
- randomized election timeout
- heartbeats implemented as empty `AppendEntries`
- no dynamic membership reconfiguration in the first version
- no snapshots in the first version

The objective of the MVP is to deliver a **correct and testable static-membership Raft**, then harden or extend it later.

---

## 3) Locked team contracts (must be treated as project rules)

These contracts are now part of the agreed architecture and should guide all implementations.

### Contract A — Cluster membership and quorum (LOCKED)

For the first Raft version, **voting membership is static**.

#### Rule
The set of Raft voting brokers is defined **in configuration at startup**.
All brokers in that configured set are voting members.

#### Consequences
- quorum and majority are computed from the **static configured voter set**
- runtime discovery must **not** define who can vote
- runtime discovery must **not** define cluster size
- runtime discovery may still be used only to help nodes **locate/contact peers**

#### Why
Raft requires a stable definition of voters. If majority depended on the peers discovered at runtime,
different nodes could observe different cluster sizes and compute different quorums, creating unsafe elections.

#### Future extension
Dynamic membership will be added later, but only through **Raft-controlled configuration changes**
(not through raw discovery). A good future approach is:
- discovery detects candidate nodes
- a leader proposes membership changes through the Raft log
- optionally, new nodes first join as non-voting learners and become voters only after catch-up

---

### Contract B — Election ↔ Log metadata (LOCKED)

The election logic depends on the log only through **minimal read-only metadata**.

#### Rule
The election layer must be able to read the **last log index** and **last log term** of the local log,
without depending on log internals.

#### Recommended shape
Preferred design:
- expose one small **log summary snapshot** containing:
    - `lastLogIndex`
    - `lastLogTerm`

If the project keeps the current `RaftLogMetadata` interface, then it must still guarantee that:
- `lastLogIndex()` and `lastLogTerm()` represent a **consistent snapshot of the same logical state**

#### Empty log convention
For an empty log:
- `lastLogIndex = 0`
- `lastLogTerm = 0`

#### Semantic rule
These values refer to the **last local log entry currently present**, not to the last committed entry.
This is critical because `RequestVote` freshness must compare the **candidate log tip**, not the commit index.

#### Dependency boundary
The election module must **not** depend on:
- append/truncate internals
- entry storage details
- conflict resolution internals
- commit index or apply logic

It only needs the metadata required by Raft vote freshness.

---

### Contract C — Election ↔ RPC/network layer (LOCKED)

The boundary between election logic and networking must be **event/message-based**, not transport-based.

#### Rule
The election logic must only reason in terms of Raft events/messages.
It must not know TCP, sockets, serialization, retry implementation, or discovery details.

#### What election consumes
The election layer should receive only the following kinds of inputs:
- incoming `RequestVoteRequest`
- incoming `RequestVoteResponse`
- notification that **valid leader activity** has been observed

Important: the third input is not “a raw heartbeat arrived”, but rather:
- “a valid `AppendEntries` / leader activity has been accepted for the current or higher term”

This keeps validation details outside the pure election logic.

#### What election emits
The election layer should produce only the following kinds of outputs/events:
- request to send `RequestVote` to peers
- notification that leadership has been acquired
- optionally notification that the node stepped down / leader changed

#### Dependency boundary
Election must **not** know:
- host/port resolution details
- transport retry policies
- TCP/UDP choices
- message serialization
- runtime discovery details

Networking must transport messages; election decides consensus state transitions.

---

### Contract D — Election ↔ timer/clock abstraction (LOCKED)

The election logic must not be tied directly to real scheduling primitives such as `Thread.sleep` or `ScheduledExecutorService`.

#### Rule
The project should use a small testable abstraction named `RaftClock`.

#### Responsibility split
`RaftElectionManager` owns:
- election timeout policy
- timeout randomization
- reset/rearm behavior
- what to do when a timeout fires

`RaftClock` owns only:
- scheduling a callback in the future
- cancelling a scheduled callback

#### Timer policy
- only **one active election timeout** should exist at a time
- leader activity resets that timeout
- timeout randomization belongs to election logic, not to `RaftClock`

#### Testing rule
Election tests should use a **fake/manual clock** whenever possible.
This allows deterministic tests for timeout start, reset, expiry, and repeated elections.

---

## 4) Classes to add (concrete proposal)

Below is the proposed class structure aligned with the current codebase and the 4 locked contracts.

## 4.1 Raft core (package `it.polimi.ds.chat.ordering.raft`)

1. **`RaftOrderingService`** (implements `OrderingService`)
    - Entry point from `Broker`.
    - Receives `ChatReqMessage` via `propose(...)`.
    - If leader: appends to log and starts `AppendEntries` to followers.
    - If follower: forwards to leader (or responds with internal redirect).
    - Exposes `start/stop`, `isLeader`, `getLeaderId`.

2. **`RaftNode`**
    - Holds node-local Raft election state:
        - `nodeId`
        - `currentTerm`
        - `votedFor`
        - `leaderId`
        - `role` (`FOLLOWER`, `CANDIDATE`, `LEADER`)
    - Owns only **state + state transitions**.
    - Must remain independent from transport and replication.

3. **`RaftLog`**
    - Stores replicated Raft entries.
    - Supports append, lookup, conflict detection, and truncation.
    - Exposes log metadata needed by elections through `RaftLogMetadata` or an equivalent immutable summary.

4. **`RaftCommitManager`**
    - Tracks `commitIndex` and `lastApplied`.
    - Leader side: computes majority commit progress from followers’ `matchIndex`.
    - Node side: applies committed entries in order and triggers delivery callback.

5. **`RaftPeerReplicationState`**
    - Leader-side per-peer state:
        - `nextIndex`
        - `matchIndex`
    - Supports conflict backtracking during replication.

6. **`RaftElectionManager`**
    - Owns election orchestration only.
    - Starts and resets election timeouts.
    - Starts elections after timeout expiry.
    - Builds `RequestVoteRequest` using:
        - local term from `RaftNode`
        - self ID
        - local log metadata from Contract B
    - Collects and validates vote responses.
    - Promotes node to leader after majority.
    - Steps down on higher-term responses or valid leader activity.
    - Uses `RaftClock` (Contract D).
    - Talks to networking only through Contract C.

7. **`RaftRpcServer`**
    - Endpoint for incoming Raft RPCs (`RequestVote`, `AppendEntries`).
    - Deserializes network messages and forwards them to the correct local Raft component.

8. **`RaftRpcClient`**
    - Sends Raft RPCs to peers.
    - Resolves peer addresses and handles transport-level timeout/retry policy.
    - Must not contain consensus decisions.

9. **`RaftPersistence`** (interface) + **`FileRaftPersistence`**
    - Minimal persistence required by Raft:
        - `currentTerm`
        - `votedFor`
        - log entries
    - `FileRaftPersistence` stores data on local disk.

10. **`RaftStateMachineAdapter`**
    - Maps committed log entries into application events (`ChatDeliverMessage`) toward `Broker`.
    - Keeps Raft logic separated from chat-specific delivery logic.

## 4.2 Raft data models (package `it.polimi.ds.chat.messages.raft`)

11. **`RaftLogEntry`**
    - Minimum fields:
        - `index`
        - `term`
        - `command`

12. **`RequestVoteRequest` / `RequestVoteResponse`**
    - Already part of Person A’s work.

13. **`AppendEntriesRequest` / `AppendEntriesResponse`**

14. **`InstallSnapshotRequest` / `InstallSnapshotResponse`** *(phase 2 / post-MVP)*

> Snapshots are not required for the first working version.

## 4.3 Utility/infrastructure

15. **`RaftClock`**
    - Timer/scheduling abstraction used by election logic.
    - Must be testable and mockable.

16. **`RaftMetrics`** *(optional for post-MVP hardening)*
    - Diagnostic counters: election count, term changes, append retry, follower lag.

---

## 5) Classes to modify

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
        - **static Raft voter configuration** (required by Contract A)
            - example: list/map of broker IDs and Raft RPC endpoints

3. **`PeerRegistry`**
    - Can still help with peer discovery / addressing.
    - Must **not** be treated as the source of truth for Raft quorum in the MVP.
    - Quorum must come from the static configured voter set.

4. **`OrderingServiceCallback`** *(optional but useful)*
    - Additional possible event: `onLeaderChanged(newLeaderId)`.

5. **`messages` package**
    - Introduce serializable Raft RPC DTO classes.

---

## 6) Clear ownership boundaries

This section should be treated as the practical implementation guide for the 3-person team.

### `RaftNode` owns
- local election state
- state transitions
- term updates
- vote recording rules
- role changes

### `RaftElectionManager` owns
- election timeout scheduling/reset
- election start on timeout
- vote request broadcast orchestration
- vote response handling
- majority win detection
- step-down on higher term or valid leader activity

### `RaftElectionManager` must not own
- AppendEntries replication details
- commit logic
- socket/TCP details
- persistence internals
- broker integration wiring

### `RaftLog` / replication side owns
- actual log entry storage
- append/truncate/conflict resolution
- prevLog matching
- follower catch-up
- majority commit tracking

### RPC/network side owns
- message serialization/deserialization
- actual send/receive operations
- connection setup
- transport timeout/retry policy
- endpoint resolution

---

## 7) Updated implementation roadmap by milestone

### Milestone M1 (working static-membership Raft MVP)
- Contract A implemented: static voter set from config.
- Leader election with randomized election timeout.
- Heartbeats as empty `AppendEntries`.
- `RequestVote` correctness using log freshness.
- `AppendEntries` with single entry and majority commit.
- Ordered delivery to brokers through existing callback.
- Basic persistence (`term`, `votedFor`, log).
- Core unit tests + one 3-node integration test.

### Milestone M2 (robustness)
- Complete `nextIndex` backtracking on conflicts.
- Follower down/up recovery with catch-up.
- Client proposal redirect when node is not leader.
- More integration tests for failure scenarios.

### Milestone M3 (hardening)
- Snapshot/install snapshot.
- Metrics and structured logging.
- Timeout tuning and long-running stability tests.

### Post-MVP extension (optional)
- Learner/non-voter join flow.
- Membership reconfiguration through committed Raft config changes.
- Dynamic membership with safe quorum transition.

---

## 8) Work split for a 3-person team

## Person A – **Consensus Core Owner**

**Main classes:**
- `RaftNode`
- `RaftElectionManager`
- `RequestVote*`
- term/role/majority logic

**Responsibilities:**
- maintain correct node state transitions
- implement election orchestration under Contracts A, B, C, D
- ensure no split-brain under static membership
- define/use timer abstraction for deterministic tests

**Current completed scope (already done):**
- `RaftRole`
- `RaftNode`
- `RequestVoteRequest` / `RequestVoteResponse`
- vote handling logic with log freshness checks
- unit tests for node state transitions and vote rules

**Next scope:**
- `RaftElectionManager`
- election timeout reset/start logic
- vote collection for current term only
- step-down on higher term / valid leader activity
- unit tests for election orchestration

**Estimated workload:**
- **~35% of total**
- High complexity (concurrency + timing)

---

## Person B – **Replication & Commit Owner**

**Main classes:**
- `RaftLog`
- `RaftPeerReplicationState`
- `RaftCommitManager`
- `AppendEntries*`

**Responsibilities:**
- implement replicated log behavior
- expose election-safe log metadata under Contract B
- ensure `lastLogIndex` / `lastLogTerm` semantics are correct
- support majority commit and ordered apply
- implement conflict handling and progressive retry

**Important contract obligations:**
- expose a consistent snapshot of the local log tip
- use empty-log convention `(0,0)`
- do not force election to depend on log internals

**Estimated workload:**
- **~40% of total**
- Most critical part for global order correctness

---

## Person C – **Integration, Persistence & QA Owner**

**Main classes:**
- `RaftOrderingService`
- `RaftRpcServer` / `RaftRpcClient`
- `RaftPersistence` + `FileRaftPersistence`
- changes to `Broker`, `BrokerConfig`, integration tests

**Responsibilities:**
- integrate Raft behind the existing `OrderingService` API
- wire static voter membership from config (Contract A)
- keep RPC/network layer separate from election logic (Contract C)
- provide scheduling/runtime glue compatible with `RaftClock` (Contract D) if needed
- implement end-to-end tests (3 nodes, leader failover, recovery)

**Important contract obligations:**
- do not compute quorum from `PeerRegistry` discovery state
- do not leak sockets/TCP details into election logic
- treat discovery as addressing/support, not as Raft membership authority

**Estimated workload:**
- **~25% of total**
- Significant wiring and validation effort

---

## 9) Effort estimate (person-days)

- M1: **10–14 person-days**
- M2: **6–9 person-days**
- M3: **4–6 person-days**

Total: **20–29 person-days** (depends on testing quality and required robustness level).

For a 3-person part-time course team: typically **3–5 weeks**.

---

## 10) Risk/complexity matrix by component

- Election timeout tuning: **high risk** (leader flapping)
- Log conflict resolution: **high risk** (subtle safety bugs)
- Crash recovery persistence: **medium-high risk**
- RPC network layer (timeout/retry): **medium risk**
- Broker integration refactor: **medium-low risk**
- Dynamic membership: **deferred risk** (explicitly postponed after MVP)

---

## 11) Recommended test strategy

### A. Raft rule unit tests
- vote granted/denied by term and log freshness
- stale term rejection
- same-term vote reuse rules
- follower transition behavior on equal/higher term
- AppendEntries reject on `prevLog` mismatch

### B. ElectionManager unit tests
- follower timeout starts election
- self-vote is counted
- vote requests include current term and local log metadata
- majority vote makes node leader
- duplicate votes are ignored
- stale responses are ignored
- higher-term response forces step-down
- valid leader activity resets timeout
- candidate steps down on valid leader activity
- split vote causes no leader yet, then later new election can begin

### C. 3-node integration tests
- single leader election
- replication of N messages with identical order on all nodes
- leader heartbeat prevents follower elections

### D. Failure tests
- kill leader → new election → commit continues
- rejoin old leader node as follower

### E. Persistence tests
- node restart with coherent persisted state

---

## 12) Suggested execution sequence (week-by-week)

- **Week 1:** finalize contracts + class skeletons + base election + RPC DTOs
- **Week 2:** `RaftElectionManager` + append/majority commit + apply callback
- **Week 3:** persistence + failover tests
- **Week 4:** hardening, tuning, final documentation

---

## 13) Operational conclusion

To minimize risk and maximize parallelism in a 3-person team:

- separate **consensus**, **replication**, and **integration/persistence** clearly
- treat the 4 contracts in this document as binding design rules
- compute quorum only from the **static configured Raft voter set** in the MVP
- keep election dependent only on:
    - local node state (`RaftNode`)
    - local log metadata (Contract B)
    - message/event callbacks (Contract C)
    - timer abstraction (Contract D)
- keep `Broker` dependent only on `OrderingService`, so sequencer/raft switch is possible without impact on the rest of the system

This structure enables delivery of a correct static-membership Raft MVP and then iterative hardening or extension without rewriting the architecture.
