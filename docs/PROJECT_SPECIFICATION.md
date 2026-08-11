# Replicated Chat Infrastructure - Specification and Compliance Baseline

Updated on 2026-08-11 from the official file
`projects_2025-2026_v1.pdf` for the Distributed Systems course (A.Y. 2025-2026).

This is the authoritative project document. It separates three things that must not
be confused during development or presentation:

- requirements stated by the professors;
- design choices made by the group;
- properties that are actually implemented and validated.

`Implemented` means that the corresponding code path exists. `Validated` means that
there is evidence from an appropriate test. A property required by the assignment is
not considered satisfied merely because the architecture intends to provide it.

---

## 1. Official requirements

### 1.1 Course rules applicable to this project

| ID | Official rule | Consequence for this project |
| --- | --- | --- |
| R1 | The project is optional and may increase the final score if correctly developed. | Administrative rule; no software requirement. |
| R2 | Groups must contain two or three students. | The submitted group must have 2-3 members. |
| R3 | The project is valid only for A.Y. 2025-2026 and must be presented before its last official exam session. | The group must schedule the presentation within that deadline. |
| R4 | The demonstration must use the students' own notebooks, at least two, connected to a wired or wireless LAN, and show a truly distributed scenario. | A localhost-only demonstration is not sufficient. At least two physical notebooks must run communicating processes. |
| R5 | A few slides must describe the software architecture and the run-time architecture. | Slides and a deployment diagram are required presentation material. |
| R6 | A Java project may use only sockets - TCP or UDP, unicast or multicast - or RMI as networking technologies. | This implementation uses Java TCP/UDP sockets and object serialization over them; no other networking middleware may be introduced. |
| R7 | Thesis projects follow a separate path through Prof. Cugola. | Not applicable to this course project. |

### 1.2 Replicated Chat Infrastructure requirements

| ID | Requirement or assumption | Required interpretation |
| --- | --- | --- |
| P1 | The brokers are connected to the same LAN and link-layer broadcast is available and must be exploited. | Broker-to-broker design and the demo must actually use LAN broadcast where appropriate. Merely running all brokers on loopback is insufficient. |
| P2 | Clients may run on the Internet, not necessarily on the brokers' LAN. Each client connects to one broker and can communicate with clients connected to any broker. | Client-broker communication must be routable point-to-point traffic; clients must not rely on LAN broadcast. |
| P3 | Every client must receive messages in the same order. | All simultaneously connected recipients must observe the same relative order for the chat messages delivered during their connected intervals. |
| P4 | Delivery must respect causal relationships. | If message `b` is causally dependent on message `a`, no client may observe `b` before `a`. Total order alone is not a proof of causal order unless the chosen total order is shown to extend happens-before. |
| P5 | Brokers do not store messages; clients receive messages only while connected. | There must be no offline inbox, history query or replay after reconnect. The current persistent Raft log contains chat payloads, so the group must explicitly resolve or confirm this interpretation before claiming full compliance; see section 7. |
| P6 | The solution may be a real distributed application or an OmNet++ simulation. | The selected solution is a real Java distributed application. |
| A1 | No Byzantine behavior. | Components may crash or lose communication, but do not send malicious or arbitrary protocol data. |
| A2 | Clients, brokers and network links may fail, but network partitions do not occur. | Failure and recovery paths must be demonstrated without claiming partition tolerance. Tests that intentionally split the cluster into disconnected components are outside the assumed model. |

The phrase "messages sent by others" permits the implementation to suppress the
echo to the originating client. This does not change the order observed by the other
connected clients.

---

## 2. Selected architecture

The implementation uses the following processes:

- `DirectoryService`: a TCP directory that distributes the static voter topology to
  brokers at startup and selects a live broker for clients;
- `Broker`: accepts client TCP connections, proposes chat commands to Raft and
  delivers committed messages to its local connected clients;
- `RaftOrderingService`: replicates a log over the broker cluster and applies
  committed entries in log-index order;
- `ClientMain`: discovers a broker through the directory, maintains a TCP session,
  retries unacknowledged commands and attempts reconnection after failure.

```text
Internet / routable client side

 Client A ---- TCP ----> Broker 0 -----+
                                      |
 Client B ---- TCP ----> Broker 1 -----+---- broker LAN
                                      |     UDP broadcast: votes, heartbeats
 Client C ---- TCP ----> Broker 2 -----+     TCP unicast: log payloads, proposals
                         |
                         +---- TCP ---- DirectoryService
```

The directory is not part of Raft consensus. Raft membership and quorum come only
from the identical static voter map loaded into every `RaftConfig`.

---

## 3. Communication design and Java networking constraint

| Traffic | Current transport | Why it matches the design |
| --- | --- | --- |
| Client to broker, including JOIN, chat, ACK, heartbeat and QUIT | TCP unicast socket | Clients need not be on the broker LAN. |
| Broker/directory registration, heartbeat and lookup | TCP unicast socket | Point-to-point control traffic. |
| Raft `RequestVote` request | UDP LAN broadcast | Small request for all configured voters; directly exploits the LAN broadcast assumption. |
| Raft `RequestVote` response | UDP unicast to the candidate | One response has one destination. |
| Empty Raft `AppendEntries` heartbeat | UDP LAN broadcast | Small periodic request for all followers. |
| Response to an empty heartbeat | UDP unicast to the leader | One response has one destination. |
| Raft `AppendEntries` containing log entries | TCP unicast per follower | Reliable ordered stream and follower-specific catch-up. |
| Follower proposal forwarding | TCP unicast to the known leader | The proposal has one destination and waits for a commit result. |
| Auxiliary LAN discovery | UDP broadcast | Discovery helper only; it never changes voter membership or quorum. |

All networking in `src/main/java` is based on `Socket`, `ServerSocket`,
`DatagramSocket` and `DatagramPacket`. This is compliant with rule R6.

---

## 4. Intended chat message flow

1. A client asks the directory for a broker and opens a TCP session to it.
2. The client sends a message identified by `(clientId, clientSeq)`.
3. The connected broker creates or reuses a `ChatReqMessage`. A retry of the same
   client message reuses its command identity and causal metadata.
4. A follower forwards the proposal to the known leader; a leader appends it
   directly.
5. The leader replicates the `ChatCommand`. After a majority acknowledges it, Raft
   commits the entry.
6. Every broker applies committed log entries in the same log-index order.
7. The broker's hold-back queue checks sequence and causal readiness, then delivers
   the chat message to its currently connected local clients except its originator.
8. The originating client receives an ACK only after the proposal is reported as
   committed. A timeout triggers retry with the same `(clientId, clientSeq)`.

JOIN and QUIT notifications are local system messages. They are not replicated chat
commands and must not be used as evidence for the global order guarantee.

---

## 5. Ordering and causal-delivery argument

### Total order

Raft provides one committed log order under its usual majority and stable-membership
conditions. Mapping each committed chat command to its log position is a reasonable
basis for a global chat order.

The application currently uses the raw Raft log index as `ChatDeliverMessage.seq`,
but it suppresses no-op entries when applying the log. `HoldBackQueue` nevertheless
expects a gap-free sequence of chat deliveries. Because a leader appends a no-op when
elected, the first chat command can have index 2 while the queue waits for index 1;
the same problem can recur after later leader changes or restart. Therefore the
end-to-end total-order requirement is **not yet validated and is currently blocked by
this P0 defect**, even though the Raft layer's log-order tests pass.

An acceptable fix must either:

- advance the application sequence across every committed non-chat entry and restore
  that progress on restart; or
- assign a separate, contiguous chat-delivery sequence independent of raw Raft log
  indexes.

The fix needs an integration test that starts real `Broker` instances, includes a
leader no-op, sends chat traffic and verifies delivery to clients on different
brokers.

### Causal order

The implementation attaches broker vector clocks and uses a hold-back queue. However,
the current causal check ignores the sender broker's component and the broker updates
its outgoing causal clock after client I/O. A client can respond immediately after
receiving a message while the broker's outgoing clock has not yet incorporated that
delivery. The group must fix or formally rule out this race and test a causal chain
across different brokers before claiming P4.

The final evidence must show that the chosen committed total order extends the
happens-before relation, not only that every broker has the same log.

---

## 6. Failure model and claimed scope

Within the official no-partition, non-Byzantine assumptions, the intended scope is:

- a three-voter cluster continues after one broker crashes because two voters still
  form a majority;
- a configured broker may restart with the same id, endpoints and Raft state;
- TCP/UDP messages may be lost or connections may fail transiently; Raft retries and
  later heartbeat/election rounds are expected to recover while the network remains
  connected;
- a client detects a failed broker, asks the directory for another live broker and
  retries uncommitted messages;
- a disconnected client does not receive messages sent while it is disconnected.

The implementation does not claim:

- Byzantine fault tolerance;
- availability during a network partition;
- dynamic Raft membership;
- offline history or replay;
- tolerance of a directory failure for new connections or reconnects;
- more than one simultaneous broker failure in a three-voter cluster.

The client reconnect path, directory liveness window and link-failure recovery still
need the manual tests listed in `PRE_GROUP_MANUAL_TESTING_TODO.md`.

---

## 7. Meaning of "brokers do not store messages"

The application exposes no history API, offline inbox or reconnect replay. In that
application-level sense, clients receive only live traffic while connected.

Nevertheless, `FileRaftPersistence` writes Raft log entries containing the chat text
to disk. That is technical consensus state, but it is still literal storage of
messages. The previous documentation called this unconditionally compliant; the
official text does not explicitly grant that exception.

Before the presentation the group must choose and document one defensible resolution:

1. obtain confirmation that the requirement forbids user-visible history but permits
   bounded internal consensus storage, then implement/describe compaction and never
   replay it to clients; or
2. change the design so persistent recovery state does not retain chat payloads,
   while giving a correct failure model and ordering argument.

Until that decision is made, do not claim that persistent message-bearing Raft logs
are certainly allowed by P5.

---

## 8. Compliance status on 2026-08-11

| Requirement | Current status | Evidence or missing work |
| --- | --- | --- |
| R2 group size | Group responsibility | Confirm 2-3 names in submission/slides. |
| R3 academic-year deadline | Group responsibility | Schedule before the last official session of A.Y. 2025-2026. |
| R4 at least two notebooks on a LAN | **Not yet validated** | Current runbook is localhost-only and broker directory host is hard-coded to `localhost`. Make it configurable and execute the two-notebook runbook. |
| R5 architecture slides | **To prepare** | Include both software and run-time/deployment architecture. |
| R6 only sockets/RMI in Java | **Implemented; code-inspected** | TCP/UDP Java sockets only. |
| P1 LAN brokers and broadcast | **Implemented; real-LAN validation missing** | Hybrid UDP broadcast exists; test packet exchange on the actual demo LAN. |
| P2 clients through any broker | **Implemented; distributed validation missing** | Directory selection and follower forwarding exist. Validate clients on different brokers/notebooks. |
| P3 same delivery order | **Blocked by P0 sequence-gap defect** | Fix no-op/restart sequence handling and add end-to-end broker/client test. |
| P4 causal delivery | **Not yet proven** | Fix/justify causal-clock timing and sender-component rule; run causal-chain test. |
| P5 no storage; connected-only delivery | **Partial / interpretation open** | No history or replay exists, but persistent Raft log stores chat payloads. |
| P6 real distributed application | **Implemented** | Java multi-process application. |
| A1 no Byzantine behavior | Assumption | State explicitly in slides. |
| A2 failures but no partitions | **Partially tested** | Raft automated tests cover several crash/restart paths; physical link/client/broker scenarios remain manual. |

---

## 9. Verification evidence

On 2026-08-11:

```powershell
mvn test
```

completed successfully with:

```text
Tests run: 186, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

This is unit/integration evidence for the tested components, not evidence for rule R4
or for the complete end-to-end P3/P4 guarantees. In particular, the suite does not
currently connect full client sessions to multiple physical broker processes after a
leader no-op.

---

## 10. Presentation checklist and safe claims

The slide deck must contain, at minimum:

1. the official requirements and assumptions;
2. the software architecture and responsibility of each component;
3. the run-time deployment over at least two notebooks and one LAN;
4. the hybrid UDP-broadcast/TCP-unicast transport table;
5. the total-order and causal-order argument;
6. failure scenarios demonstrated and their observed results;
7. the connected-only delivery semantics and the agreed interpretation of Raft
   persistence;
8. limitations: no partitions, no Byzantine behavior, static membership and
   directory dependency.

Safe statements after the remaining P0 work and manual validation:

- "All chat commands are committed in one Raft order and every connected broker
  delivers the same chat sequence."
- "The demonstrated causal chains are ordered consistently with happens-before."
- "UDP link-layer broadcast is used for vote requests and empty heartbeats; TCP
  unicast is used for follower-specific log replication and client traffic."
- "Disconnected clients receive no history or replay."

Do not claim full compliance while any corresponding row in section 8 is marked
blocked, partial or not validated.
