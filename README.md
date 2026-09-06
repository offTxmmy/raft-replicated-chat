# Replicated Chat Infrastructure

A fault-tolerant, totally-ordered group chat built on a hand-written **Raft**
consensus core. Developed for the *Distributed Systems* course
(Politecnico di Milano, A.Y. 2025–2026).

Clients connect to any broker in a replicated cluster and exchange messages.
Every connected client observes **the same messages in the same order**, and the
order **respects causality**. The cluster keeps working while a minority of
brokers, individual links, or clients fail.

---

## Table of Contents

- [Requirements Realized](#requirements-realized)
- [Architecture](#architecture)
- [Ordering Guarantees](#ordering-guarantees)
- [Transport Strategy](#transport-strategy)
- [Project Layout](#project-layout)
- [Build](#build)
- [Running the System](#running-the-system)
- [Configuration Reference](#configuration-reference)
- [Testing](#testing)
- [Fault Model & Scope](#fault-model--scope)
- [Documentation](#documentation)

---

## Requirements Realized

The system implements the *Replicated Chat Infrastructure* assignment:

| # | Requirement | How it is met |
|---|-------------|---------------|
| **P1** | Brokers share a LAN and exploit link-level broadcast. | Hybrid Raft transport: `RequestVote` and empty heartbeats travel over **UDP LAN broadcast**; log replication travels over **TCP unicast**. |
| **P2** | A client connects to one broker and talks to clients on other brokers; clients may be off-LAN. | Client↔Broker and Client↔Directory links are point-to-point **TCP**, routable beyond the broker LAN. |
| **P3** | All connected clients receive messages in the same order. | A single committed Raft log plus deterministic, in-order application yields one client-visible **total order**. |
| **P4** | The order respects causality. | A single in-flight FIFO message per client extends per-client program order; the Raft total order preserves happens-before. |
| **P5** | Brokers do not store messages; clients only receive while connected. | No inbox, history, or replay. A client becomes a recipient only after an internal Raft-ordered **JOIN fence** is applied. |
| **F1** | Clients, brokers, and links may fail. | Raft quorum tolerates a minority of broker failures; clients reconnect with backoff, endpoint quarantine, and connection generations. |

Non-goals (explicitly out of scope): network partitions, Byzantine faults,
dynamic membership, authentication/TLS, and broker crash-recovery of the same
identity within a run. See [Fault Model & Scope](#fault-model--scope).

---

## Architecture

Four cooperating components communicate over Java sockets with Java
serialization.

```
                 ┌─────────────────────┐
                 │  Directory Service  │   static voter set, broker selection,
                 │ (broker + client    │   client registry, heartbeats
                 │  listeners)         │
                 └─────────┬───────────┘
        voter set / select │  register / heartbeat
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐        ┌────▼────┐        ┌────▼────┐
   │ Broker0 │◄──────►│ Broker1 │◄──────►│ Broker2 │   Raft cluster
   │ (Raft)  │  Raft  │ (Raft)  │  Raft  │ (Raft)  │   (UDP broadcast + TCP)
   └────┬────┘        └────┬────┘        └────┬────┘
        │ TCP              │ TCP              │ TCP
     ┌──▼──┐            ┌──▼──┐            ┌──▼──┐
     │Client│           │Client│           │Client│
     └─────┘            └─────┘            └─────┘
```

- **DirectoryService** — distributes the static voter set to brokers, registers
  broker/client endpoints and heartbeats, and selects a broker for each client.
  It does **not** participate in consensus.
- **Broker** — holds local client sessions, builds chat proposals, receives
  committed Raft entries, runs the hold-back queue and delivery, and reports
  heartbeats to the Directory.
- **RaftOrderingService** — the consensus core: election, log replication,
  commit management, transport, persistence, and proposal deduplication.
- **Client** — discovers a broker through the Directory, keeps one broker
  connection, sends `JOIN`/chat messages, and handles ACKs, heartbeats, retries,
  and reconnection.

### Message flow (steady state)

1. The client assigns `(clientId, clientSeq)` and sends a message.
2. The receiving broker builds a `ChatReqMessage` (identity + vector clock).
   If it is not the leader, it forwards the proposal to the leader over TCP.
3. The leader appends a `ChatCommand` to the Raft log.
4. Followers replicate; the leader advances the commit index only on a
   majority, under the current-term rule.
5. The commit manager applies entries in order and produces a delivery.
6. The broker passes the delivery through a per-session hold-back queue to its
   local clients — **excluding** the originating client.
7. The producer receives an ACK once the message is committed.

---

## Ordering Guarantees

- **Total order.** A single committed log plus in-order application gives a
  total order over commands. The implementation maintains a dense, deterministic
  *application sequence* distinct from the raw Raft index — no-op entries and
  JOIN fences occupy the log without creating client-visible gaps.
- **Causal order.** A client keeps a single FIFO message in flight until its
  commit ACK, so a later message can never overtake an earlier one from the same
  client. Deduplication makes retrying the in-flight head safe across leader
  changes, and vector-clock metadata is merged on the ready prefix before
  visibility.

---

## Transport Strategy

The transport is chosen per message pattern rather than broadcasting everything.

| Message | Transport | Rationale |
|---------|-----------|-----------|
| `RequestVote` request | UDP LAN broadcast | Small, naturally one-to-many; loss is recovered by later election rounds. |
| `RequestVote` response | UDP unicast | Reply to one specific candidate. |
| `AppendEntries` (empty heartbeat) | UDP broadcast when identical for all peers | Small, common; peer-specific state falls back to unicast. |
| `AppendEntries` (with payload) | TCP unicast | Depends on each follower's `nextIndex`; needs reliability and larger payloads. |
| `AppendEntries` response | UDP unicast | Small reply to the leader; treated as possibly duplicated/reordered. |
| Forward client proposal | TCP unicast | Synchronous follower→leader request/response. |
| Client↔Broker, Client↔Directory | TCP | Point-to-point, routable off-LAN. |

A development-only `raft-local` mode sends **every** Raft RPC over TCP unicast so
that multiple brokers can run on a single host.

---

## Project Layout

```
src/main/java/it/polimi/ds/chat/
├── directory/          DirectoryService: voter distribution, registry, selection
├── broker/             Broker core, config, and client-session handling
├── client/             Client runtime, connection, discovery, messaging, reconnect
├── ordering/
│   ├── api/            OrderingService abstraction
│   └── raft/           Raft node, election, replication, commit, transport, persistence
├── protocol/           Serializable wire messages (broker, chat, client, directory, raft)
└── common/             Vector clock, hold-back queue, socket helpers

src/test/java/          Unit, component, persistence, transport, and end-to-end tests
docs/                   Specification, audit, testing tracker, migration notes
```

---

## Build

Requirements: **JDK 16+** and **Maven 3.9+**.

```bash
mvn clean package
```

This compiles the project, runs the test suite, and produces the class output
under `target/`.

---

## Running the System

Start each component in its own terminal. The examples below run three brokers
and two clients on a single host using the `raft-local` transport mode.

### 1. Directory Service

```bash
mvn -q exec:java -Dexec.mainClass=it.polimi.ds.chat.directory.DirectoryService \
  -Dexec.args="0@localhost:50100:50000,1@localhost:50101:50001,2@localhost:50102:50002"
```

The voter CSV format is `id@host:rpcPort[:clientPort]`. When `clientPort` is
omitted it defaults to `50000 + id`. The Directory listens on port `60000` for
brokers and `60001` for clients by default.

### 2. Brokers (one per voter id)

```bash
# node 0
mvn -q exec:java -Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain \
  -Dexec.args="raft-local 0 50100"
# node 1
mvn -q exec:java -Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain \
  -Dexec.args="raft-local 1 50101"
# node 2
mvn -q exec:java -Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain \
  -Dexec.args="raft-local 2 50102"
```

Each broker fetches the static voter set from the Directory at startup. For a
real multi-host LAN deployment, use the default `raft` mode and real host
addresses in the voter CSV:

```bash
mvn -q exec:java -Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain \
  -Dexec.args="raft 0 50100"
```

### 3. Clients

```bash
mvn -q exec:java -Dexec.mainClass=it.polimi.ds.chat.client.ClientMain \
  -Dexec.args="localhost 60001"
```

Enter a username when prompted, then type messages at the `>` prompt.
Type `/quit` (or press Ctrl-D) to leave.

> **Note:** Running the compiled classes with a plain `java -cp target/classes …`
> command also works, but the `jline` client dependencies must be on the
> classpath. The `mvn exec:java` invocations above resolve them automatically.

---

## Configuration Reference

### DirectoryService

```
DirectoryService <votersCSV> [brokerPort] [clientPort]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `votersCSV` | — | `id@host:rpcPort[:clientPort],…` static voter set. |
| `brokerPort` | `60000` | Listener for broker registration/heartbeats. |
| `clientPort` | `60001` | Listener for client broker-selection requests. |

### BrokerMain

```
raft       <nodeId> <rpcPort> [clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes] [directoryHost] [directoryPort]
raft-local <nodeId> <rpcPort> [clientPort] [directoryHost] [directoryPort]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `nodeId` | — | Must appear in the voter set. |
| `rpcPort` | — | Must match the voter endpoint for `nodeId`. |
| `clientPort` | voter's client port | Client listener port. |
| `raftBroadcastPort` | `7100` | UDP broadcast port (`raft` mode). |
| `clusterId` | `default-raft-cluster` | UDP envelope cluster filter. |
| `udpMaxPayloadBytes` | `1400` | Max UDP datagram payload. |
| `directoryHost` / `directoryPort` | `localhost` / `60000` | Directory endpoint. |

Raft state is persisted per node under `raft-data/n<nodeId>/`.

### ClientMain

```
ClientMain [directoryHost] [directoryPort]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `directoryHost` | `localhost` | Directory host. |
| `directoryPort` | `60001` | Directory client listener port. |

---

## Testing

```bash
mvn test
```

The suite spans unit and component tests, filesystem persistence, TCP/UDP Raft
integration, client socket/reconnect behaviour, Directory lifecycle, and a full
end-to-end path exercising Directory, Broker, Raft, the hold-back queue, and
real client sockets — including leader kill, re-election, and reconnection.

> The automated evidence is same-host/loopback. Multi-host LAN behaviour,
> firewall traversal, and physical UDP broadcast must be validated manually on
> real hardware.

---

## Fault Model & Scope

**In scope**

- Crash-stop failure of a minority of brokers; the majority keeps making
  progress and re-elects a leader.
- Client and link failures, handled by reconnection with backoff, endpoint
  quarantine, and connection generations.
- No message storage: clients receive only while connected, enforced by the
  Raft-ordered JOIN fence.

**Out of scope**

- Network partitions and Byzantine faults.
- Dynamic membership (the voter set is static).
- Authentication, TLS, and production hardening.
- Broker crash-recovery under the same identity within a single run — the
  current delivery scope is **crash-stop**. A crashed node should not be
  restarted with the same identity in the same execution.

---

## Documentation

Additional design and verification material lives under [`docs/`](docs/):

- [`PROJECT_SPECIFICATION.md`](docs/PROJECT_SPECIFICATION.md) — requirement
  compliance baseline and design decisions.