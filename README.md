# Replicated Chat Infrastructure

A fault-tolerant group chat backed by a hand-written Raft implementation. Clients may connect to any broker; committed messages are replicated and delivered in one deterministic order across the cluster.

The project originated in the Distributed Systems course at Politecnico di Milano (A.Y. 2025–2026) and has since been cleaned up as a standalone engineering project.

## Why Raft?

A chat message may enter through any broker, but every broker needs to agree on the same order before exposing it. Raft supplies the replicated log, leader election, majority commit, and recovery rules needed for that agreement. The implementation is intentionally built from Java sockets rather than a consensus library so that election, replication, persistence, and failure handling remain visible in the code.

## Architecture

```text
                            client discovery
                     +-------------------------+
                     |    Directory Service    |
                     | live endpoints + load   |
                     +------------+------------+
                                  |
             register / heartbeat | TCP
                                  |
       +--------------------------+--------------------------+
       |                          |                          |
  +----+-----+   Raft RPCs   +----+-----+   Raft RPCs   +----+-----+
  | Broker 0 |<------------->| Broker 1 |<------------->| Broker 2 |
  |  + Raft  |  TCP + UDP    |  + Raft  |  TCP + UDP    |  + Raft  |
  +----+-----+               +----+-----+               +----+-----+
       | TCP                      | TCP                      | TCP
    clients                    clients                    clients
```

- **Directory Service** tracks broker leases and local client counts, then returns the least-loaded live endpoint to clients. It is not part of consensus or Raft bootstrap.
- **Broker** owns local TCP sessions and fans out committed deliveries. Slow clients have bounded outbound queues and are disconnected without blocking Raft application.
- **Raft ordering service** implements pre-vote, election, log replication, majority commit, persistent state, state-machine application, and retry deduplication.
- **Client runtime** discovers an endpoint, completes JOIN, keeps exactly one chat message in flight, and reconnects without discarding the pending head.

Raft membership is static and supplied directly, identically, to every broker. The Directory may be restarted or unavailable without stopping consensus or existing sessions; only new discovery is unavailable during that interval.

## Message flow

1. After a local JOIN boundary, the client sends a message identified by `(clientId, clientSeq)`.
2. If the edge broker is a follower, it forwards the proposal to the known leader over TCP.
3. The leader appends a `ChatCommand` and replicates it with `AppendEntries`.
4. A majority match advances `commitIndex`, subject to Raft's current-term commit rule.
5. Every broker applies committed entries sequentially by Raft log index and directly fans the delivery out to its active local sessions, excluding the sender.
6. The leader returns success after commit; the edge broker ACKs the originating client. A retry keeps the same logical identity and is deduplicated.

The follower-to-leader response is deliberately synchronous. It makes the client ACK mean “committed by Raft” without adding a second edge-broker acknowledgement protocol. Local delivery on the edge may follow shortly after that ACK, but global ordering and retry identity are already fixed.

## Guarantees

- **Total order:** every delivered chat command follows the committed Raft log. `ChatDeliverMessage.seq` is the Raft index, so gaps caused by internal no-op entries are expected and harmless.
- **Causal order:** a client keeps one FIFO proposal in flight. A message created in response to an observed delivery can only be proposed after that predecessor is committed and locally applied; Raft leader completeness keeps the predecessor before the response after leader changes. No separate vector clock is required.
- **Commit-before-ACK:** a proposal is acknowledged only after majority commit, never merely after append.
- **Retry safety:** `(clientId, clientSeq)` is the single application identity used for pending proposals and deduplication.
- **Connected-only delivery:** JOIN is a local action serialized with state-machine application. It needs no quorum, and a new session cannot see entries applied before its boundary. There is no inbox or history replay.
- **Failure isolation:** one slow or broken client cannot block delivery to other sessions. A failed state-machine callback is fatal to that broker; it is never silently recorded as applied.
- **Crash recovery:** Raft term, vote, log, and commit/application progress are durable. A restarted broker reconstructs deduplication state without replaying previously applied chat history.

These guarantees assume non-Byzantine processes and a stable, identically configured voter set. Progress requires a reachable majority.

## Transport

Production `raft` mode combines LAN broadcast with reliable unicast:

- pre-vote and vote rounds use UDP broadcast and TCP in parallel;
- common empty heartbeats use UDP broadcast plus periodic TCP probes;
- payload-bearing or follower-specific `AppendEntries` use TCP;
- forwarded client proposals use TCP request/response.

UDP packets may be lost, duplicated, or reordered; Raft terms, indexes, and TCP fallback preserve safety and eventual progress. `raft-local` sends all Raft traffic over TCP and is the convenient same-machine development mode.

## Repository layout

```text
src/main/java/it/polimi/ds/chat/
├── broker/             local sessions, fan-out, broker bootstrap
├── client/             discovery, FIFO sender, reconnect lifecycle
├── directory/          broker lease registry and endpoint selection
├── ordering/
│   ├── api/            application-facing ordering contract
│   └── raft/           election, replication, transport, persistence
├── protocol/           serializable client, Directory, and Raft messages
└── common/             shared socket utilities

src/test/java/          unit, deterministic fault, socket, and end-to-end tests
docs/DESIGN.md          invariants and reviewed design decisions
```

## Build and test

Requirements: JDK 17+ and Maven 3.9+.

```bash
mvn clean package
```

For the full test suite without packaging:

```bash
mvn test
```

Tests cover election safety, log repair, current-term commit, higher-term step-down, stale responses, dropped UDP, majority loss and healing, durable restart/corruption, late commit and retry, JOIN races, slow/broken clients, Directory restart, and real-socket end-to-end failover.

## Run three brokers locally

Start each command in its own terminal. The same voter CSV must be passed to every broker.

### 1. Directory

```bash
mvn -q exec:java "-Dexec.mainClass=it.polimi.ds.chat.directory.DirectoryService" "-Dexec.args=60000 60001"
```

### 2. Brokers

```bash
# broker 0
mvn -q exec:java "-Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain" "-Dexec.args=raft-local 0 0@127.0.0.1:50100:50000,1@127.0.0.1:50101:50001,2@127.0.0.1:50102:50002 127.0.0.1 60000"

# broker 1
mvn -q exec:java "-Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain" "-Dexec.args=raft-local 1 0@127.0.0.1:50100:50000,1@127.0.0.1:50101:50001,2@127.0.0.1:50102:50002 127.0.0.1 60000"

# broker 2
mvn -q exec:java "-Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain" "-Dexec.args=raft-local 2 0@127.0.0.1:50100:50000,1@127.0.0.1:50101:50001,2@127.0.0.1:50102:50002 127.0.0.1 60000"
```

The voter format is `id@host:raftRpcPort[:clientPort]`. If `clientPort` is omitted, it defaults to `50000 + id`. Persistent state is written below `raft-data/n<id>/`.

### 3. Clients

```bash
mvn -q exec:java "-Dexec.mainClass=it.polimi.ds.chat.client.ClientMain" "-Dexec.args=127.0.0.1 60001"
```

Enter a username, type messages, and use `/quit` to leave.

## Run on a LAN

Use stable LAN addresses in the shared voter CSV and start each broker in hybrid mode:

```bash
mvn -q exec:java "-Dexec.mainClass=it.polimi.ds.chat.broker.core.BrokerMain" "-Dexec.args=raft 0 0@192.168.1.10:50100:50000,1@192.168.1.11:50100:50000,2@192.168.1.12:50100:50000 7100 office-chat 1400 192.168.1.20 60000"
```

Allow broker RPC/client TCP ports and the shared UDP broadcast port through host firewalls. Use the same `clusterId`, broadcast port, payload limit, and voter map on every broker. Each host should run only its own node id. The Directory can run on any reachable host; clients need only its client-listener address.

CLI reference:

```text
DirectoryService [brokerPort] [clientPort]
BrokerMain raft <nodeId> <votersCSV> [broadcastPort] [clusterId]
                [udpMaxPayloadBytes] [directoryHost] [directoryPort]
BrokerMain raft-local <nodeId> <votersCSV> [directoryHost] [directoryPort]
ClientMain [directoryHost] [directoryPort]
```

## Failure model and limitations

Supported behavior includes minority broker crash/restart, client and link failure, lost UDP, stale messages, leader replacement, divergent uncommitted suffix repair, and temporary Directory loss. The design intentionally does not provide:

- dynamic Raft membership or automatic reconfiguration;
- Byzantine fault tolerance, authentication, authorization, or TLS;
- availability without a broker majority;
- exactly-once delivery to a client that disconnects during a socket write;
- snapshots or log compaction;
- a replicated Directory Service.

The durable Raft log contains serialized command payloads, including message text. This is consensus state, not a client-visible archive: brokers expose no history API and do not replay already applied entries to clients after restart. Because compaction is not implemented, internal retention is currently unbounded; this is the principal storage limitation.

See [docs/DESIGN.md](docs/DESIGN.md) for the reviewed invariants and the reasoning behind the final architecture.
