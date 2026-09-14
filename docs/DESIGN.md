# Design

## System overview

The system is a replicated group chat. A client discovers a live broker through the Directory Service, opens one TCP session, and sends chat requests to that broker. The broker proposes the request to the Raft ordering service; a follower forwards the proposal to the current leader when known. A message is delivered to local connected clients only after its Raft entry is committed and applied.

The Directory Service is a lease-based discovery and load-selection component. It is not a Raft member and does not determine consensus membership. The Raft voter set is static, supplied on the command line, and must be identical on every broker.

```text
client -> Directory Service -> selected broker -> Raft leader
                                            -> replicated Raft log
                                            -> committed application on every broker
                                            -> local connected clients
```

## Ordering and commitment

Each broker owns a Raft node. The leader appends a `ChatCommand`, replicates it with `AppendEntries`, and advances the commit index only after a majority has matched the entry. Commit advancement follows Raft's current-term rule. Brokers apply committed entries sequentially by log index and fan each chat delivery out to their active local sessions.

`ChatDeliverMessage.seq` is the Raft log index. It is a stable ordering identifier; internal no-op entries can create gaps.

The leader appends a no-op after election so entries inherited from a previous term can become committed under the current-term rule. The implementation includes pre-vote to reduce disruption from isolated or stale nodes.

## Client path, retries, and failover

Every client request carries `(clientId, clientSeq)`, the durable identity of one logical proposal. The client keeps one request in flight. If a connection or leader path fails before acknowledgement, it reconnects through the Directory and retries the same pending identity. The ordering service deduplicates committed and in-progress proposals by that identity.

An ACK means the proposal has committed, rather than merely being appended locally. A client may still disconnect while a delivery is being written; the system does not claim exactly-once client delivery across that boundary.

Session activation is serialized with the state-machine application path. This prevents a newly active session from receiving entries that were already applied before activation, while preserving connected-only delivery and avoiding a history replay feature.

## Transport

Two Raft transport modes are available:

- `raft-local` uses TCP unicast for all Raft RPCs and is intended for running several brokers on one machine.
- `raft` uses hybrid LAN transport. Pre-vote and vote requests are sent by UDP broadcast with TCP fallback; common empty heartbeats use UDP broadcast plus periodic TCP probes. Log-bearing or follower-specific `AppendEntries` and forwarded client proposals use TCP.

UDP is treated as lossy and unordered. Raft terms, log indexes, retries, and TCP traffic provide the protocol-level safety and progress mechanisms; UDP itself provides neither consensus nor reliable delivery.

## Persistence and recovery

Each node persists Raft hard state, log entries, and commit/application progress beneath its own `raft-data/n<id>/` directory. On restart, the node validates the stored state, rebuilds the deduplication index from already applied commands, and applies any committed-but-not-applied suffix in order. It does not replay previously applied chat messages to sessions opened after restart.

Corrupt or inconsistent committed state fails startup instead of being silently repaired. A state-machine callback failure is fail-stop for that broker, preventing it from claiming an entry was applied when its local delivery path failed.

## Operational boundaries

- A reachable majority of the configured voters is required to accept new chat messages.
- Membership is static; dynamic reconfiguration and snapshots are not implemented.
- The durable log retains message payloads and grows without bound until a future retention or snapshot design is added.
- Processes are assumed non-Byzantine. Authentication, authorization, encryption, and a replicated Directory Service are outside this project's scope.
