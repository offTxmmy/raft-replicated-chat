# Replicated Chat Infrastructure — Project Specification & Design Document

> Reference document for the Distributed Systems course project (A.Y. 2025/2026).
> This file is the **single source of truth** for requirements, assumptions, architecture,
> component responsibilities and progress tracking. All design decisions must be
> consistent with this document. When the design evolves, update this file first.

---

## 1. Problem statement (from the course brief)

A set of **brokers** connected on the same LAN cooperatively replicate a chat service
for **clients** running on the Internet (not necessarily on the same LAN).

- Each client connects to **one broker** to send messages to other clients (connected to
  any broker) and to receive messages sent by others.
- Brokers must deliver messages to clients such that **every client receives messages in
  the same order**, also respecting the **causal** relationship between messages.
- Brokers **do not store messages**: clients receive messages only while connected.

### 1.1 Assumptions (given by the spec)

- **No byzantine behavior.** Components follow the protocol, they only fail by crashing.
- **Clients, brokers, and network links may fail.** Crash-recovery model.
- **No network partitions happen.** The broker LAN is never split into disjoint
  subnetworks that keep running independently.
- **Link-layer broadcast is available among brokers** (they share a LAN) and should be
  exploited where appropriate.

### 1.2 What the spec implicitly requires (derived)

- **Fault tolerance:** a single broker crash must not stop the chat service.
- **Consistency:** the global delivery order seen by any two clients must be identical,
  and must extend the happens-before order induced by causal dependencies among messages.
- **Availability under crash-recover:** a crashed broker that restarts must be able to
  rejoin the replicated service without compromising safety.
- **Client-facing liveness:** when clients are connected, messages they send must
  eventually be delivered to all connected clients (within the no-partition assumption).

### 1.3 Explicitly out of scope

- Message persistence for offline clients.
- Message history / replay.
- Security / authentication / confidentiality.
- Byzantine fault tolerance.
- Operation under network partitions.
- Dynamic cluster membership (see §4.6 for the discussion).

---

## 2. Key design decisions

The following decisions shape the whole implementation.

### 2.1 Consensus protocol: Raft

We replace the previous centralized `SequencerOrderingService` (single point of failure)
with a **Raft**-based replication layer exposed behind the existing `OrderingService`
interface. Rationale:

- Raft provides a **total order** over client requests via a replicated log — matches
  the spec requirement of identical delivery order across clients.
- Raft gives **crash-recover fault tolerance** with a majority quorum — matches the
  assumption that crashes occur but partitions do not.
- Raft is well documented, well understood, and teachable.

### 2.2 Cluster membership: static for MVP, bootstrap-discoverable

- The **voting set of brokers is static** for the purpose of computing Raft quorum.
  Each broker knows, at startup, the set of broker IDs that form the cluster.
- The **IP:port endpoints** of those brokers are discovered at runtime via LAN
  broadcast (see §4.5), taking advantage of the spec hint about link-layer broadcast.
- **Dynamic membership changes are not implemented** in the MVP: see §4.6 for
  justification and future-extension notes.

### 2.3 Causal order: client-side metadata, broker-side preservation

- Each client attaches **causal metadata** (e.g., a vector clock or a per-client
  sequence number with causal dependencies) to its messages.
- Raft enforces the **total order** of delivery across all brokers.
- The client-side delivery layer uses the causal metadata to **hold a message until its
  causal predecessors have been delivered**, then releases it to the user.
- Because Raft's total order extends any causal order present in the input, causal
  consistency is preserved for free at the delivery layer.

### 2.4 Transport: mix of TCP and UDP broadcast

- **Client ↔ broker:** TCP (clients are on the Internet, no LAN broadcast available).
- **Broker ↔ broker control traffic (RequestVote, AppendEntries):** designed to be
  carriable over UDP multicast/broadcast on the LAN to exploit the spec hint. Vote
  responses travel as unicast because the receiver identity matters.
- **Broker discovery on LAN:** UDP broadcast beacon (see §4.5).

### 2.5 Persistence: minimum required by Raft

Each broker persists on local disk:

- `currentTerm`
- `votedFor`
- log entries (as produced by the replication layer)

No additional durable storage is required. Client session state is volatile by design
(the spec states brokers do not store messages).

---

## 3. High-level architecture

```
 +--------+       TCP        +----------+      UDP broadcast/multicast     +----------+
 | Client | <--------------> |  Broker  | <------------------------------> |  Broker  |
 +--------+                  |  (Raft)  |         (RequestVote /           |  (Raft)  |
                             +----------+          AppendEntries /         +----------+
                                  ^                 discovery beacon)            ^
                                  |                                              |
                                  +----------------- LAN -----------------------+
```

### 3.1 Layered responsibilities inside a broker

```
  +-----------------------------------------------------------+
  |                      Client-facing layer                  |
  |  ClientHandler, ClientMessageSender/Receiver, Directory   |
  +-----------------------------------------------------------+
  |                      Broker core                           |
  |                        Broker.java                         |
  +-----------------------------------------------------------+
  |                 OrderingService (interface)                |
  |     implemented by: SequencerOrderingService (legacy)      |
  |                     RaftOrderingService   (target)         |
  +-----------------------------------------------------------+
  |        Raft core (election + replication + commit)         |
  |  RaftNode, RaftElectionManager, RaftLog, RaftReplication   |
  |  Manager, RaftCommitManager, RaftPersistence               |
  +-----------------------------------------------------------+
  |        Raft RPC / transport & peer discovery layer         |
  |  RaftRpcServer, RaftRpcClient, LAN discovery beacon        |
  +-----------------------------------------------------------+
  |                 Timer abstraction (RaftClock)              |
  +-----------------------------------------------------------+
```

### 3.2 External view

- A client sees a logical chat service, reachable via any broker.
- If the broker the client is connected to is not the current Raft leader, the broker
  either forwards the client request to the leader or redirects the client to it
  (see §4.4).
- On leader change, the broker notifies its connected clients so they can update their
  target. The handoff is transparent to the user.

---

## 4. Functional specification

### 4.1 Client lifecycle

1. Client starts, learns a list of candidate brokers (from a Directory service and/or
   LAN hints surfaced by a local agent).
2. Client opens a TCP connection to one broker and performs a `ClientJoinMessage`
   handshake.
3. While connected, the client can:
   - send chat messages (`ChatReqMessage`) carrying causal metadata;
   - receive ordered messages (`ChatDeliverMessage`) in the globally agreed order.
4. On broker crash, the client detects the disconnection and connects to another
   broker. Any message sent and not yet acknowledged must be retried against the new
   broker (at-least-once delivery from the client side; deduplication is based on
   `(clientId, clientSeq)` — see §4.3).
5. On explicit quit, the client sends `ClientQuitMessage` and closes.

### 4.2 Broker lifecycle

1. Broker starts with a configuration file declaring:
   - its own `brokerId`,
   - the static set of voter broker IDs,
   - ports (client TCP, Raft RPC, UDP discovery),
   - storage directory for Raft persistence,
   - election/heartbeat timeouts.
2. Broker loads persisted Raft state (`currentTerm`, `votedFor`, log).
3. Broker starts the LAN discovery beacon (see §4.5) to resolve peer endpoints.
4. Broker starts the Raft RPC server and the election manager as **follower** with a
   full randomized election timeout.
5. From this point on, standard Raft behavior applies: elections, heartbeats,
   replication, commit, apply.
6. On graceful shutdown, the broker stops accepting new client messages, flushes its
   persistence, and closes transport resources.

### 4.3 Message flow (happy path)

1. Client C1 (attached to broker B_f, follower) sends `ChatReqMessage(m)` with causal
   metadata.
2. B_f forwards `m` to the current Raft leader B_l (or replies with a redirect telling
   C1 to contact B_l directly — see §4.4).
3. B_l calls `OrderingService.propose(m)`, which appends an entry to its Raft log.
4. B_l replicates the entry via `AppendEntries` to followers. Once a majority has
   acknowledged, the entry is marked **committed**.
5. Every broker (including B_l) independently applies committed entries in log-index
   order, producing `ChatDeliverMessage` events that carry the globally agreed order.
6. Each broker forwards `ChatDeliverMessage` to its locally connected clients over TCP.
7. Each client buffers the message until its causal predecessors have also been
   delivered, then releases it to the application.

### 4.4 Leader change and client redirect

- While there is no leader (during an election), the ordering service rejects
  `propose` calls with a "no leader, retry" semantics. Clients back off and retry.
- When a broker learns of a new leader (either by winning the election or by receiving
  valid `AppendEntries` from a higher-term leader), it emits an `onLeaderChanged`
  event up to the client-facing layer.
- Connected clients are notified of the new leader and reconnect to it. Outstanding
  un-acked messages are retried and deduplicated by `(clientId, clientSeq)` at the
  leader.

### 4.5 Peer discovery on LAN (address resolution, not membership)

- Each broker periodically broadcasts a UDP beacon `{brokerId, rpcEndpoint, epoch}`
  on a well-known LAN multicast address/port.
- Each broker listens on the same address and populates a local `PeerRegistry`
  mapping `brokerId -> rpcEndpoint`.
- **Discovery only resolves addresses.** It does not add or remove voters. The voter
  set is the static one declared in configuration. An unexpected broker ID heard on
  the LAN is ignored for the purpose of Raft. This preserves a safe and stable quorum.

### 4.6 Dynamic cluster membership — design note

The spec does not explicitly require dynamic membership: it speaks of "a set of
brokers" and only admits crash-recovery failures. Implementing full Raft
reconfiguration (joint consensus or single-server changes) is a significant effort
with high bug risk that would interact directly with safety.

**Decision:** the MVP uses a static voter set. Broker crash-and-rejoin is fully
supported and tested. Dynamic membership is **deliberately deferred** as a future
extension. The architecture is prepared for it:

- config changes would be represented as special Raft log entries,
- new brokers would first join as non-voting learners,
- quorum would be computed from the most recent config entry in the log.

This is documented here so that we can defend the choice at the oral exam and describe
the extension path without implementing it.

---

## 5. Non-functional requirements

- **Safety over liveness.** A correctness violation (e.g., two committed entries with
  the same index but different commands) is unacceptable; brief unavailability during
  an election is acceptable.
- **Short election gap.** Election timeouts must be tuned so that leader failover
  completes quickly (target: well under one second), because brokers do not store
  messages and a prolonged leaderless interval directly degrades user experience.
- **Deterministic tests.** Core Raft logic must be testable without real time, sockets
  or threads (via `RaftClock` and injectable sender seams).
- **Clear module boundaries.** Election, replication, transport and persistence must
  be independently replaceable and independently testable.

---

## 6. Architectural contracts (binding design rules)

These contracts are part of the architecture. Any change to them must update this
document first.

### Contract A — Cluster membership and quorum (static voter set)

- The set of Raft voters is defined **in configuration at startup**.
- Quorum and majority are computed from that static voter set.
- Runtime peer discovery resolves addresses only; it does **not** affect membership.

### Contract B — Election ↔ Log metadata (minimal read-only coupling)

- The election layer accesses the log only through a small, consistent snapshot:
  `(lastLogIndex, lastLogTerm)`.
- Empty log convention: `(0, 0)`.
- These values refer to the last local log entry present, not to the last committed
  entry.
- Election logic must not depend on append/truncate internals, conflict resolution, or
  commit logic.

### Contract C — Election ↔ RPC/network (event/message based)

- The election layer consumes only:
  - incoming `RequestVoteRequest`,
  - incoming `RequestVoteResponse`,
  - a notification that valid leader activity has been observed.
- The election layer emits only:
  - a request to send `RequestVote` to peers,
  - a notification that leadership has been acquired,
  - a notification of step-down / leader change.
- Transport details (TCP vs UDP, retries, serialization, endpoint resolution) live in
  the RPC layer.

### Contract D — Election ↔ Timer (testable clock abstraction)

- The election layer uses `RaftClock` to schedule and cancel callbacks.
- Only **one** active election timeout exists at a time; valid leader activity resets
  it.
- Timeout randomization is election-side concern, not clock-side.

---

## 7. Component inventory

### 7.1 Raft core (`it.polimi.ds.chat.ordering.raft`)

- `RaftOrderingService` — implements `OrderingService`; entry point from `Broker`.
- `RaftNode` — per-node state: `currentTerm`, `votedFor`, `role`, `leaderId`.
- `RaftRole` — FOLLOWER / CANDIDATE / LEADER.
- `RaftElectionManager` — election orchestration (timeouts, vote collection,
  promotion, step-down).
- `RaftElectionListener` — outward events from election (`onLeaderElected`,
  `onSteppedDown`, `onHeartbeatRoundDue`).
- `RaftVoteRequestSender` — transport seam for outgoing `RequestVote`.
- `RaftLeaderActivityObserver` — hook to feed valid-leader-activity signals to the
  election manager.
- `RaftLog` — replicated log with append, conflict handling, metadata snapshot.
- `RaftLogMetadata` — `(lastLogIndex, lastLogTerm)` snapshot.
- `RaftPeerReplicationState` — per-follower `nextIndex` / `matchIndex`.
- `RaftReplicationManager` — leader-side replication driver + follower-side
  `AppendEntries` handler.
- `RaftAppendEntriesSender` — transport seam for outgoing `AppendEntries`.
- `RaftCommitManager` — commit-index tracking and ordered apply.
- `RaftClock` / `RaftScheduledTask` — timer abstraction.
- `RaftPersistence` (interface) + `FileRaftPersistence` — durable `currentTerm`,
  `votedFor`, log.
- `RaftStateMachineAdapter` — maps committed entries to `ChatDeliverMessage`.

### 7.2 Raft RPC data models (`it.polimi.ds.chat.messages.raft`)

- `RaftLogEntry` — `(index, term, command)`.
- `RequestVoteRequest` / `RequestVoteResponse`.
- `AppendEntriesRequest` / `AppendEntriesResponse`.

### 7.3 Transport & discovery

- `RaftRpcServer` — listens for incoming Raft RPCs and dispatches to the correct
  local component.
- `RaftRpcClient` — sends RPCs to peers, handles transport-level retries.
- `LanDiscoveryService` — UDP broadcast beacon for peer address resolution.
- `PeerRegistry` — in-memory `brokerId -> endpoint` map. Not a source of truth for
  quorum.

### 7.4 Existing broker/client components (kept)

- `Broker`, `BrokerConfig`, `BrokerMain`.
- `ClientConnection`, `ClientHandler`, `ClientMessageSender/Receiver`,
  `ClientDirectory`, etc.
- `SequencerOrderingService` — kept temporarily behind a configuration switch during
  migration; removed once Raft is fully integrated.

---

## 8. Work split and responsibilities

### Person A — Consensus core (leader election)

- `RaftNode`, `RaftElectionManager`, `RaftRole`, `RequestVote*`.
- State transitions, term management, vote freshness rules.
- Election orchestration under Contracts A–D.
- Persistence hook for `currentTerm` / `votedFor` (once `RaftPersistence` lands).
- Emission of `onLeaderChanged` up to the ordering service.
- Unit tests for election logic with a fake clock.

### Person B — Replication and commit

- `RaftLog`, `RaftPeerReplicationState`, `RaftCommitManager`,
  `RaftReplicationManager`, `AppendEntries*`.
- Log append, conflict detection, truncation.
- Majority commit tracking and ordered apply.
- Follower catch-up and `nextIndex` backtracking.
- Unit tests for log and replication rules.

### Person C — Integration, transport, persistence, QA

- `RaftOrderingService`, `RaftRpcServer`, `RaftRpcClient`.
- `RaftPersistence` + `FileRaftPersistence`.
- LAN discovery beacon and `PeerRegistry` adaptation.
- Client-facing plumbing for leader redirect (`onLeaderChanged` → client notify).
- `Broker` / `BrokerConfig` changes to select Raft as ordering mode and wire the
  static voter set.
- End-to-end and failure-mode integration tests.

---

## 9. Testing strategy

### 9.1 Unit tests (deterministic, no real I/O)

- Raft rule tests: vote grant/deny by term and log freshness, same-term vote reuse,
  follower transition on higher term, `AppendEntries` reject on `prevLog` mismatch.
- Election manager tests with fake clock: follower timeout starts election, self-vote
  counted, majority makes leader, duplicates ignored, stale responses ignored,
  higher-term step-down, split vote followed by new election, valid leader activity
  resets timeout.
- Log and commit tests: append, conflict truncation, majority commit, ordered apply.

### 9.2 Integration tests (multi-node, in-process or multi-JVM)

- 3-node cluster elects a unique leader.
- `N` messages proposed under load are delivered in the same order on every node.
- Leader heartbeats prevent spurious follower elections.
- Causal ordering preserved end-to-end at the client layer.

### 9.3 Failure tests

- Kill the leader → new leader elected → commit continues.
- Kill a follower → leader keeps committing → follower catches up on restart.
- Restart any role with persisted state → no double voting, no safety violation.
- Simulated packet loss on `RequestVote` → a new election eventually succeeds.
- Simulated duplicate `RequestVote` (broadcast + retry) → idempotent handling.

### 9.4 Persistence tests

- Node votes, crashes before persisting → must not re-vote differently after restart.
- Node restarts with consistent `(term, votedFor, log)`.

---

## 10. Milestones and progress tracking

Update the status checkboxes in this section as work lands. This section is the
"where are we" overview.

### M0 — Legacy baseline (done)

- [x] Centralized `SequencerOrderingService` working end-to-end with clients.
- [x] Client-side causal metadata plumbing in place.

### M1 — Raft MVP (static voter set)

Consensus core (Person A):

- [x] `RaftRole`, `RaftNode`, vote freshness checks.
- [x] `RequestVoteRequest` / `RequestVoteResponse`.
- [x] `RaftElectionManager` with full election orchestration (timeouts, vote
      collection, promotion, step-down, heartbeat scheduling).
- [x] `RaftClock` / `RaftScheduledTask` abstraction.
- [x] `RaftVoteRequestSender` seam (Contract C).
- [x] `RaftElectionListener` outward callback.
- [x] Unit tests for election manager (split vote, higher-term step-down, leader
      activity reset, etc.).
- [ ] Persistence hook for `currentTerm` / `votedFor` wired to `RaftPersistence`.
- [ ] `onLeaderChanged` event propagated through `OrderingServiceCallback`.
- [ ] Correct exposure of `leaderId == null` during CANDIDATE state, so
      `OrderingService.propose` can reject cleanly.

Replication and commit (Person B):

- [x] `RaftLog` with append, conflict handling, `snapshotMetadata()`.
- [x] `RaftCommitManager` with ordered apply and current-term commit rule.
- [x] `RaftPeerReplicationState`.
- [x] `AppendEntriesRequest` / `AppendEntriesResponse` DTOs.
- [x] `RaftReplicationManager` (leader-side driver + follower-side handler).
- [x] `RaftCoreTest` wiring election and replication.
- [ ] Conflict-optimized backtracking hints (term/index hints) — M2.

Integration, transport, persistence, QA (Person C):

- [x] `RaftOrderingService` wired behind `OrderingService`.
- [x] `BrokerConfig` extended with Raft parameters and static voter set.
- [x] `Broker.initializeOrderingService()` selects Raft via config.
- [x] `RaftRpcServer` / `RaftRpcClient` for Raft RPC and follower proposal
      forwarding.
- [x] Client retry deduplication in Raft using `(username, MSG timestamp)`.
- [ ] LAN discovery beacon + `PeerRegistry` adapter for address resolution only.
- [ ] `RaftPersistence` + `FileRaftPersistence` implementation.
- [ ] Client-facing leader redirect triggered by `onLeaderChanged`.
- [ ] 3-node integration test (single leader, replicated delivery order).

### M2 — Robustness

- [ ] Conflict-optimized `nextIndex` backtracking.
- [ ] Follower crash/restart with full catch-up (integration test).
- [ ] Leader crash → re-election within target timeout (integration test).
- [ ] Deduplication of client retries by `(clientId, clientSeq)` on the leader.
- [ ] Message-loss tolerance on vote and append RPCs (broadcast path).
- [ ] End-to-end causal + total order validation under load.

### M3 — Hardening and polish

- [ ] Structured logging for role changes, term changes, elections.
- [ ] `RaftMetrics` (election count, term changes, append retries, follower lag).
- [ ] Timeout tuning with measured election latency.
- [ ] Long-running stability test (minutes to hours).
- [ ] Final documentation and user-facing README for running a 3-node demo.

### Post-MVP (not planned, documented for the oral exam)

- [ ] Dynamic membership via single-server changes (Ongaro §4.3).
- [ ] Snapshots / `InstallSnapshot`.
- [ ] Learner (non-voting) join flow before promotion to voter.

---

## 11. How we defend our choices at the oral exam

- **Why Raft:** total order + crash-recover fault tolerance, matches spec assumptions.
- **Why static voter set:** the spec defines "a set of brokers" and admits only
  crash-recover failures. A static voter set gives a provably safe quorum without
  requiring joint consensus, and frees engineering effort for end-to-end correctness.
- **Why LAN broadcast for Raft RPCs:** the spec explicitly suggests exploiting
  link-layer broadcast. One packet reaches all peers, reducing election latency
  linearly in cluster size.
- **Why no Pre-Vote / CheckQuorum:** the spec assumes no network partitions, which
  removes the main scenarios those mechanisms address. Omitting them keeps the
  election logic small and verifiable.
- **Why no dynamic membership:** not required by the spec, high implementation cost
  and safety risk. Extension path is documented (§4.6).
- **Why brokers do not persist chat messages:** the spec explicitly states so.
  Durability is limited to the Raft state required for safety.

---

## 12. Glossary

- **Broker:** a server node participating in the replicated chat service.
- **Client:** an external user agent connecting to a broker over the Internet.
- **Voter / voting member:** a broker whose vote counts toward Raft quorum.
- **Learner:** a non-voting broker that receives log entries but does not count in
  quorum. Not used in the MVP.
- **Term:** monotonically increasing logical time unit in Raft; at most one leader per
  term.
- **Quorum:** majority of the static voter set, i.e. `floor(N/2) + 1`.
- **Commit:** a log entry is committed when it is stored on a quorum of voters and the
  current-term commit rule is satisfied; it will then be applied on every broker.
- **Delivery:** the act of handing a committed message to connected clients in global
  order, after its causal predecessors have themselves been delivered.
