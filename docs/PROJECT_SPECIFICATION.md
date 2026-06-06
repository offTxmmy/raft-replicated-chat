# Replicated Chat Infrastructure - Final Design Notes

Updated on 2026-06-03.

This document is the main reference for the project presentation. It describes the
requirements, the implemented architecture, the guarantees we claim, the guarantees
we deliberately do not claim, and the remaining checks before the group manual
testing session.

---

## 1. Course specification

The project asks for a replicated chat infrastructure:

- brokers are connected on the same LAN;
- clients are on the Internet and connect to one broker at a time;
- a client can send messages through any broker;
- clients connected to any broker must receive messages in the same order;
- delivery must also respect causal relationships;
- brokers do not provide message history for disconnected clients;
- clients, brokers and network links may fail;
- no Byzantine behavior and no network partitions are assumed;
- LAN broadcast/multicast is available among brokers and should be exploited where it
  is useful.

The implementation is a real Java distributed application.

---

## 2. Main design choices

### 2.1 Raft for total order

The production ordering layer is based on Raft. Every chat message that must be
globally delivered is proposed as a Raft log command. A command is delivered only
after it is committed and applied in log-index order.

This gives the property required by the specification: all brokers apply the same
committed log prefix, therefore all connected clients observe the same message order
for the messages delivered by their broker.

The old centralized sequencer is not the production path.

### 2.2 Static voting membership

The Raft voting set is configured statically at broker startup through
`RaftConfig.getVoters()`. Quorum is computed only from this static set.
The static topology is provided to the `DirectoryService` at startup as a
`votersCSV`; each broker fetches the same voter map from the Directory through a
one-shot `GetClusterRequestMessage`.

We chose static membership because it gives a stable majority definition. If brokers
could join the voting set dynamically without replicated configuration entries, two
brokers could compute different quorums and Raft safety would no longer be
defensible. Correct dynamic membership requires the Raft reconfiguration protocol
using configuration entries, learners and controlled promotion to voter. That is a
valid future extension, but it is not required by the assignment and would increase
the safety risk before the demo.

Crash and restart of configured voters is in scope. Adding a new voting broker while
the system is running is out of scope.

### 2.3 LAN broadcast where it is useful

Broker-to-broker traffic uses a hybrid transport:

- `RequestVote` requests use UDP LAN broadcast;
- empty `AppendEntries` heartbeats use UDP LAN broadcast;
- `AppendEntries` carrying log entries use TCP unicast;
- follower-to-leader proposal forwarding uses TCP;
- client-to-broker communication uses TCP.

This is the tradeoff we will justify at the oral exam. Broadcast is useful for
small messages addressed to all brokers, especially elections and heartbeat rounds.
TCP remains better for log-entry replication because it avoids UDP fragmentation,
manual ACK/NACK handling, selective retransmission and payload-size problems.

### 2.4 Broker discovery is not part of membership

`LanDiscoveryService` and `PeerRegistry` are not the source of Raft membership or
quorum and are not part of the reliable demo path for broker cluster setup.

The current reliable demo path is based on the static voter endpoints configured
in the Directory at startup. LAN is still used where it is useful, especially for
Raft broadcast control traffic, but not to dynamically discover or change the
broker voting set.

### 2.5 No application-level message storage

The assignment says that brokers do not store messages for clients. Our
interpretation is:

- disconnected clients do not receive history;
- there is no offline replay feature;
- client sessions are volatile;
- the Raft log is technical consensus storage, not application history.

Raft persistence is still necessary for safety: a broker must persist term, vote and
log state so that crash-recovery does not break election or replication rules.

---

## 3. Architecture

```text
Client TCP
    |
    v
+-------------------------+
| Broker                  |
|                         |
| Client handlers         |
| Broker core             |
| OrderingService         |
| RaftOrderingService     |
| Raft core               |
| Hybrid Raft transport   |
+-------------------------+
    |             |
    | TCP         | UDP LAN broadcast
    | entries     | votes / heartbeats
    v             v
 other brokers on the same LAN
```

### Broker components

- `Broker` manages connected clients and calls the ordering layer.
- `RaftOrderingService` implements the `OrderingService` interface.
- `RaftNode` stores local Raft role, term, vote and leader information.
- `RaftElectionManager` handles election timeouts, votes and leader transitions.
- `RaftReplicationManager` handles `AppendEntries`, conflict handling and follower
  catch-up.
- `RaftLog` stores ordered log entries.
- `RaftCommitManager` advances commit index and applies committed entries in order.
- `FileRaftPersistence` persists the Raft state needed for crash-recovery.
- `RaftRpcServer` and `RaftRpcClient` handle TCP Raft traffic and proposal forwarding.
- `RaftUdpBroadcastTransport` and `RaftHybridTransport` implement the LAN broadcast
  part of the Raft transport.
- `LanDiscoveryService` and `PeerRegistry` are discovery helpers, not consensus
  membership.

---

## 4. Message flow

### 4.1 Client joins

1. The client chooses a broker, usually through the directory service.
2. The client opens a TCP connection to that broker.
3. The broker keeps the client in its local connected-client set.
4. The client receives only messages delivered while it remains connected.

Join and leave notifications are local system messages in the current application
layer. They must not be presented as globally ordered Raft chat commands unless we
explicitly change the implementation.

### 4.2 Chat message on the leader

1. A client sends a `ChatReqMessage` to its connected broker.
2. If that broker is the Raft leader, it proposes the command directly.
3. The leader appends the command to its log.
4. The leader replicates it to followers with `AppendEntries` over TCP.
5. After a majority acknowledges, the command is committed.
6. Every broker applies committed entries in log order.
7. Each broker delivers the resulting `ChatDeliverMessage` to its locally connected
   clients.

### 4.3 Chat message on a follower

If the connected broker is a follower, the current implementation forwards the
proposal to the known leader over TCP. Client redirect is not the primary mechanism
implemented today.

If there is no known leader during an election, the proposal is rejected with a retry
semantics. The client/broker flow must retry after the cluster elects a leader.

### 4.4 Causal order

The system combines:

- Raft total order for all committed chat commands;
- vector-clock metadata attached by the broker/application layer;
- a hold-back delivery rule that avoids delivering a message before its causal
  predecessors.

Retry of the same client message reuses the same broker-side command identity and
causal metadata through a broker-side cache keyed by `(username, clientTimestamp)`.
The remaining hardening topic is concurrency between different client messages
originating from the same broker.

### 4.5 Deduplication

The effective retry key currently used by the Raft ordering path is based on the
username and the client message timestamp. This must be documented honestly.

A stronger future design would use an explicit `(clientId, clientSeq)` pair. That is
cleaner because it does not rely on timestamps and is easier to reason about during
retries, crashes and reconnects.

---

## 5. Guarantees we claim

Under the assumptions of no Byzantine behavior and no partitions:

- at most one Raft leader can commit entries for a term;
- a committed log entry is preserved by future leaders;
- all brokers apply committed entries in the same log order;
- clients connected to brokers receive committed chat messages in that same order;
- causal delivery is preserved when causal metadata has no gaps;
- a configured broker can crash and restart without intentionally losing Raft safety
  state.

---

## 6. Guarantees we do not claim

- No offline message history for disconnected clients.
- No Byzantine fault tolerance.
- No operation under network partitions.
- No dynamic voting membership during execution.
- No application-level exactly-once guarantee based on a true `clientSeq` yet.
- No guarantee that auxiliary LAN discovery alone can configure a full Raft cluster.
- No full-UDP reliable log replication.

These are limitations, not contradictions with the assignment. They are design choices
that must be justified clearly.

---

## 7. Testing status

Automated tests have passed previously with:

```powershell
mvn test
```

Observed result:

```text
Tests run: 170, Failures: 0, Errors: 0, Skipped: 0
```

Packaging without tests has also passed with:

```powershell
mvn -q -DskipTests package
```

The generated jar does not currently expose a `Main-Class`, so the practical runbook
uses:

```powershell
java -cp target/classes ...
```

The next required step is manual group testing with real processes, clients and
broker failures. The checklist is in `PRE_GROUP_MANUAL_TESTING_TODO.md`.

---

## 8. Remaining work before the presentation

### Must fix or explicitly validate

- Manual validation of concurrent messages from the same broker with vector-clock
  metadata.
- Manual 3-broker demo with clients attached to different brokers.
- Manual test of follower proposal forwarding.
- Manual leader crash and new election.
- Manual restart of the crashed broker with persisted Raft state.

### Should document in the slides

- Static membership is a safety choice.
- LAN broadcast is used for votes and heartbeats, not for large log payloads.
- TCP is used for client traffic and payload-bearing Raft replication.
- Directory service is a client/broker helper, not part of consensus.
- Raft log persistence is technical storage, not offline chat history.

---

## 9. Oral defense answers

### Why Raft?

Raft gives a replicated log. The committed log order is the global delivery order, so
the same committed prefix is applied by all brokers.

### Why static membership?

Because quorum safety depends on all nodes using the same voting set. Dynamic
membership is possible in Raft, but only with replicated configuration changes and
careful promotion of new voters. The assignment does not require that, so the safer
choice is a static voting set with crash-recovery.

### Why broadcast and TCP together?

Broadcast is efficient and natural for small messages sent to all brokers, such as
votes and heartbeats. TCP is more appropriate for log entries because payloads can be
larger and need reliable ordered delivery.

### Why is the Raft log not a violation of "brokers do not store messages"?

The Raft log is internal consensus state required for safety. It is not exposed as
chat history, and disconnected clients do not receive old messages after reconnecting.

### What happens when a client is connected to a follower?

The follower forwards the proposal to the current leader. If no leader is known during
an election, the proposal is retried after a leader is elected.

---

## 10. Glossary

- Broker: server participating in the replicated chat service.
- Client: external user process connected to one broker.
- Voter: broker whose vote counts toward Raft quorum.
- Term: Raft logical epoch.
- Quorum: majority of the static voter set.
- Commit: point where a log entry is durably accepted by a majority and can be
  applied.
- Delivery: forwarding an applied chat command to currently connected clients.
