# Person A — Leader Election: remaining work plan

> Reference: `PROJECT_SPECIFICATION.md` (single source of truth).
> Scope: everything that Person A still owns to bring the leader-election layer to
> the final, defendable state required by the project specification.
> Update the checkboxes as work lands.

---

## 0. Current state (baseline)

Already implemented and tested:

- `RaftRole`, `RaftNode` with term/role/leader state and full `RequestVote` handling
  (term checks, double-vote prevention, log-freshness check via `RaftLogMetadata`).
- `RaftElectionManager` with election timeout lifecycle, randomized timeouts,
  election start, vote collection, majority promotion, step-down on higher term and
  on valid leader activity, heartbeat scheduling (delegated via listener).
- `RaftClock` / `RaftScheduledTask` testable timer abstraction.
- `RaftVoteRequestSender` transport seam (Contract C).
- `RaftElectionListener` outward events: `onLeaderElected`, `onSteppedDown`,
  `onHeartbeatRoundDue`.
- 32 unit tests in `RaftElectionManagerTest` plus shared `RaftCoreTest` wiring.

Not yet done (this document tracks them).

---

## 1. Hard requirements for the final version

These are the items that must land before the project can be considered finished
from the leader-election side. Each item is small in code, but each has a precise
correctness reason.

### 1.1 Persistence hook for `currentTerm` and `votedFor`

- **Why:** Raft safety requires that a vote, once granted, survives a crash. Without
  durable `votedFor`, a node could vote for candidate X, crash, restart, and vote for
  candidate Y in the same term, breaking the at-most-one-leader-per-term invariant.
  Same for `currentTerm`: a stale term after restart enables split brain.
- **Where:** `RaftNode` is the only authoritative writer of `currentTerm` and
  `votedFor`. The persistence call must happen **before** the result of the mutation
  becomes observable outside the node (i.e., before sending an RPC response that
  reflects the new state).
- **Design:**
  - Add a small interface `RaftPersistence` (Person C will provide a real
    implementation; in unit tests we use an in-memory fake):
    ```
    void persistTermAndVote(long currentTerm, Integer votedFor);
    ```
  - Inject it into `RaftNode` (constructor) — it can default to a no-op for tests
    that do not care about persistence.
  - Persist **synchronously** at every state mutation that changes
    `(currentTerm, votedFor)`:
    - inside `startElection()` after the term bump and self-vote;
    - inside `becomeFollower()` when the term advances or when `votedFor` is cleared;
    - inside `stepDownIfHigherTerm()` after the term advances;
    - inside `handleRequestVote()` immediately after granting the vote (i.e., before
      the response is returned to the caller).
  - On startup, the constructor (or a static factory) loads the persisted snapshot
    and seeds `currentTerm` / `votedFor` accordingly.
- **Tests:**
  - `RaftNode` persists exactly once per mutation and exactly with the new values.
  - A node that grants a vote, is recreated from the persisted state, and receives a
    second `RequestVote` for the same term from a different candidate: must reject.
  - A node that increments its term (e.g., after `stepDownIfHigherTerm`), is
    recreated from the persisted state: must not accept a `RequestVote` from a
    candidate whose term equals the old (now stale) term.

### 1.2 Propagation of leader-change events to `OrderingService`

- **Why:** the spec requires that clients connected to a follower can still send
  messages and that, on leader change, they are redirected to the new leader. The
  ordering service is where the broker-facing API lives; it needs to know who the
  leader is.
- **What:**
  - Confirm that the existing `RaftElectionListener.onLeaderElected` and
    `onSteppedDown` cover the full set of "leader changed" cases:
    - local node became leader → `onLeaderElected`;
    - local node stepped down because of higher term or leader activity →
      `onSteppedDown` already fires;
    - local node, while remaining follower, learned a new leader id within the same
      term → **currently not signalled**. Add a fourth event:
      ```
      void onLeaderObserved(int leaderId, long term);
      ```
      fired from `onValidLeaderActivityObserved` whenever the locally known leader
      id changes (including the transition from `NO_LEADER` to a real id).
  - Wire those three callbacks (`onLeaderElected`, `onSteppedDown`,
    `onLeaderObserved`) into the `OrderingServiceCallback` that Person C exposes,
    surfacing them as a single `onLeaderChanged(newLeaderId)` API.
- **Tests:**
  - Every transition that changes the locally known leader id triggers exactly one
    leader-change notification with the right value.
  - No spurious notification when the leader id is unchanged across heartbeats in
    the same term.

### 1.3 Correct exposure of "no leader" during a candidate phase

- **Why:** while a node is CANDIDATE and there is no acknowledged leader, the
  ordering service must reject `propose(...)` with a clean "no leader, retry"
  semantics so the client can back off. If the broker silently accepts proposals
  during the leaderless gap, total order is at risk.
- **What:**
  - `RaftNode.startElection()` already sets `leaderId = NO_LEADER`. Confirm that
    every code path that returns from CANDIDATE without electing this node also
    leaves `leaderId == NO_LEADER` until a real leader is observed.
  - Provide a small unit test that walks through the full state machine
    (`FOLLOWER → CANDIDATE → FOLLOWER` after losing election) and asserts
    `getLeaderId() == NO_LEADER` at each intermediate point.
- **Tests:**
  - During CANDIDATE state, `RaftNode.getLeaderId() == NO_LEADER`.
  - After step-down with no leader observed yet, `getLeaderId() == NO_LEADER`.
  - After `onValidLeaderActivityObserved`, `getLeaderId()` reflects the new leader.

### 1.4 Idempotence of vote handling under broadcast/duplicate delivery

- **Why:** the project will deliver `RequestVote` over LAN UDP broadcast, where
  duplicates are normal (broadcast + retry on no response). Granting two different
  votes in the same term would break safety; counting the same voter twice on the
  candidate side would falsely satisfy majority in odd corner cases.
- **What:**
  - The current `RaftNode.handleRequestVote` already returns the same decision for
    repeated requests in the same term from the same candidate (because `votedFor`
    is preserved). Add an explicit unit test for that.
  - The current `RaftElectionManager.onRequestVoteResponse` already adds voters to a
    set, so a duplicate response from the same voter is ignored. Add an explicit
    unit test for that.
- **Tests:**
  - Same-term duplicate `RequestVote` from the same candidate → same decision; no
    extra persistence write beyond the first one.
  - Same-term duplicate `RequestVoteResponse` from the same voter → counted once;
    no double promotion.

### 1.5 Tolerance to lost vote requests/responses

- **Why:** under UDP broadcast, packets can be lost. Election liveness must not
  depend on every vote-request reaching every voter on the first try.
- **What:** the existing logic re-arms the election timeout at the end of
  `onElectionTimeoutFired`, so a candidate that did not reach majority will start a
  new election after a fresh randomized timeout. Verify this with a dedicated test
  that simulates losing all `RequestVote` messages and asserts that, after enough
  timeouts, a new election starts at a higher term.
- **Tests:**
  - With a `RaftVoteRequestSender` that drops all messages, the candidate keeps
    starting new elections (term increases), without entering an inconsistent state.

### 1.6 Crash-and-rejoin semantics from the election layer

- **Why:** the spec admits crashes; rejoining brokers must not destabilize the
  cluster. A restarted node must come up as FOLLOWER with a full election timeout
  and accept the first valid heartbeat from the current leader without triggering an
  election.
- **What:**
  - `RaftElectionManager.start()` already arms a randomized election timeout. A
    persisted-state-loaded `RaftNode` will start as FOLLOWER (initial role).
    Add a test that simulates: persist `(term=7, votedFor=null)`, restart the
    election manager, deliver an `AppendEntries` (modeled as
    `onValidLeaderActivityObserved(7, leaderId)`) before the timeout, and assert
    no election was started.
- **Tests:**
  - Restart with persisted state + early heartbeat → no spurious election.
  - Restart with persisted state + no heartbeat → election starts at the next
    randomized timeout, with `term = persistedTerm + 1`.

---

## 2. Coordination items with Person B and Person C

These are not implementation tasks for Person A but are agreement points that must
be in place before the items in §1 can be fully wired.

- **With Person B:** the `RaftLogMetadata` snapshot consumed in `handleRequestVote`
  and `onElectionTimeoutFired` must remain a consistent `(lastLogIndex, lastLogTerm)`
  view (Contract B). This is already respected by `RaftLog.snapshotMetadata()`. No
  action expected from us; we depend on it.
- **With Person C:**
  - the `RaftPersistence` interface required by §1.1 must be provided by Person C's
    `FileRaftPersistence`. Agree on its method signatures and on whether persistence
    is synchronous (preferred) or buffered with `fsync` boundaries.
  - the wire format for `RequestVote` over LAN UDP broadcast: Person C controls
    serialization and the broadcast mechanism; Person A only consumes the
    `RaftVoteRequestSender` seam. Agree on a header that includes the candidate id
    so the local node can ignore self-echoes of its own broadcast.
  - propagation of `onLeaderElected` / `onSteppedDown` / `onLeaderObserved` into
    `OrderingServiceCallback.onLeaderChanged(...)` lives in `RaftOrderingService`,
    owned by Person C. Person A only emits the events.

---

## 3. Optional improvements (recommend only if M1 is fully done)

These are explicitly **not required** by the project specification and should only
be considered if all items in §1 are done and the team has time to spare. They are
listed here so we can decide collectively, not so that Person A picks them up
unilaterally.

- **Pre-Vote / CheckQuorum:** classical Raft hardening against partition-induced
  term inflation. The spec assumes no partitions, so the value is low. Document the
  decision to skip them in `PROJECT_SPECIFICATION.md` §11 (already done).
- **Leader lease:** mitigates stale-leader reads. Not relevant for this project
  because brokers do not serve reads from the log; they only deliver committed
  messages.
- **Election metrics:** counters for elections started, terms observed, votes
  granted/denied, time-to-leader. Useful for the demo, low effort.

---

## 4. Test plan summary (deliverables for Person A)

A consolidated checklist of tests that must exist for the leader-election layer to
be considered final.

### 4.1 RaftNode unit tests

- [x] Term/role transitions (`startElection`, `becomeLeader`, `becomeFollower`,
      `stepDownIfHigherTerm`).
- [x] `handleRequestVote` rules: stale term, log freshness, double-vote prevention.
- [x] Persistence call site is invoked exactly when expected (§1.1).
- [x] After restart from persisted state, vote rules behave correctly (§1.1).
- [x] `getLeaderId()` consistency across full state machine walks (§1.3).
- [x] Idempotent decision for duplicate same-term `RequestVote` (§1.4).

### 4.2 RaftElectionManager unit tests

- [x] Follower timeout starts election, self-vote counted, majority makes leader.
- [x] Higher-term response forces step-down.
- [x] Valid leader activity resets the timeout.
- [x] Candidate steps down on valid leader activity.
- [x] Split vote → new election starts at the next timeout.
- [x] Lossy `RaftVoteRequestSender` → repeated elections at increasing terms (§1.5).
- [x] Same-voter duplicate response counted once (§1.4).
- [x] `onLeaderObserved` fires exactly when the known leader id changes (§1.2).
- [x] Restart-with-persisted-state followed by early heartbeat does not trigger an
      election (§1.6).
- [x] Restart-with-persisted-state without heartbeat starts a fresh election with
      `term = persistedTerm + 1` (§1.6).

### 4.3 Integration touchpoint (shared with Person C)

- [ ] In a 3-node in-process cluster, killing the leader causes a single new leader
      to be elected within the configured timeout window. (Person A drives the
      election-side asserts; Person C provides the multi-node harness.)

---

## 5. Suggested execution order

1. §1.3 — exposure of `NO_LEADER` during candidate phase. Smallest, tightens an
   existing invariant, immediately useful for §1.2.
2. §1.4 — explicit idempotence tests on `RaftNode` and `RaftElectionManager`. Pure
   tests, no production code change expected.
3. §1.5 — lossy-sender test for vote requests. Pure test on top of the existing
   manager.
4. §1.1 — persistence hook in `RaftNode`. This is the largest item and the only one
   that depends on Person C delivering `RaftPersistence`. Use an in-memory fake in
   the meantime so tests can land independently.
5. §1.2 — add `onLeaderObserved` event and the corresponding tests; coordinate with
   Person C for the wiring into `RaftOrderingService`.
6. §1.6 — crash-and-rejoin tests, executed once §1.1 lands.
7. §4.3 — integration test, once Person C's harness is available.

This order minimizes blocking on other team members and lands the safety-critical
persistence change with full test coverage in place.
