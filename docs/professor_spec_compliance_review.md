# Revisione di conformità alla specifica del professore

> Documento generato analizzando il codice realmente presente nel
> repository (non le sole intenzioni dichiarate nei `*.md`). Ogni
> riferimento a file/classi/metodi è verificato.

---

# Executive Summary

Il progetto implementa una chat replicata con due modalità di ordering
selezionabili da `BrokerConfig.getOrderingMode()`:

- `SEQUENCER` — sequencer centralizzato (legacy);
- `RAFT` — consenso distribuito basato su Raft, dietro la stessa
  interfaccia `OrderingService`.

L'architettura segue in larga misura `PROJECT_SPECIFICATION.md`, ma
alcune lacune impattano direttamente i requisiti del professore:

- **Persistenza del log Raft non cablata.** `RaftLog` lavora solo in
  memoria. `RaftOrderingService.start()` documenta esplicitamente "log
  replay from persistence deferred". Solo `currentTerm` e `votedFor` sono
  durabili. Sotto crash multipli sequenziali si possono perdere entry
  committate (violazione della *Leader Completeness* di Raft).
- **`NOT_LEADER` non gestito lato client.** `Broker.sendNotLeaderToClient`
  invia una stringa che `ClientMessageReceiver` si limita a stampare. Il
  client non si riconnette al leader e ritrasmette inutilmente.
- **Nessuna deduplicazione `(clientId, clientSeq)`** sul leader Raft.
  `Broker.buildChatReq` genera un nuovo `localMsgId` ad ogni retry; un
  messaggio ritrasmesso può essere committato due volte.
- **In modalità sequencer, ACK al client *prima* del commit globale**
  (`SequencerOrderingService.propose` su follower ritorna `true`
  immediatamente). Pericoloso se il sequencer muore subito dopo.
- **`DirectoryService` non partecipa al consenso** (bene), ma è SPoF di
  bootstrap dei client. Per Raft non è in path critico, ma va dichiarato.
- **RPC Raft in TCP unicast** (`RaftRpcClient`/`RaftRpcServer`), non in
  broadcast LAN come suggerito dalla spec. Solo `LanDiscoveryService`
  e (in modalità sequencer) i `ChatDeliverMessage` usano UDP broadcast.
- **Test forti sui moduli Raft singoli, deboli sull'end-to-end della
  chat**: un solo test integrato `RaftOrderingService` (3 nodi,
  1 messaggio). Nessun test esercita un `Broker` reale + client + Raft
  con failover sotto carico.

### Verdetto

Mostly satisfied lungo l'happy path; presenta problemi reali sotto
retry lato client e sotto crash multipli. Etichetta:
**Submit only after fixing X**, dove X è l'insieme dei blocker della
sezione "Critical Issues to Fix Before Submission".

---

# Architecture Reconstruction

## Componenti effettivi

- **`Broker`** (`it.polimi.ds.chat.broker.Broker`): accetta TCP dai
  client (un `ClientHandler` per client). Riceve `ClientMessage` →
  costruisce `ChatReqMessage` con vector clock (`buildChatReq`) →
  `OrderingService.propose`. Riceve in callback `ChatDeliverMessage`,
  li passa a `HoldBackQueue`, distribuisce ai client locali con
  `onChatDeliver` → `ClientHandler.sendMessageToClient`.
- **`BrokerMain`**: due modi CLI: legacy interattivo
  (sequencer/follower) o `raft <nodeId> <rpcPort> <votersCSV> [clientPort]`.
- **`OrderingService`** con due implementazioni:
  - **`SequencerOrderingService`**: sequencer assegna `seq` monotono,
    fa broadcast UDP del `ChatDeliverMessage`, mantiene history (1000)
    per NACK; follower forwarda al sequencer via TCP, ascolta UDP.
  - **`RaftOrderingService`**: cabla `RaftNode`, `RaftLog`,
    `RaftCommitManager`, `RaftElectionManager`, `RaftReplicationManager`,
    `RaftRpcClient/Server`, `DefaultRaftClock`, `FileRaftPersistence`.
    Le entry committate diventano `ChatDeliverMessage` via
    `RaftStateMachineAdapter`, con `seq = entry.getIndex()`.
- **`DirectoryService`** (TCP, porte 60000 broker / 60001 client):
  registrazione + heartbeat dei broker; `chooseBestBroker` per i
  client. **Non** partecipa al consenso. **Non** popola
  `RaftConfig.voters` (statico).
- **`LanDiscoveryService`**: beacon UDP `CHAT_DISCOVERY;HELLO;...`.
  Popola `PeerRegistry`, ma `PeerRegistry` non è usato per il quorum
  Raft (Contract A rispettato).
- **`ClientMain`** + `DirectoryAwareClientConnection` +
  `ClientHeartbeatManager`: bootstrap via Directory; su 10 heartbeat
  consecutivi falliti, riconnessione via Directory + re-JOIN.
- **`HoldBackQueue` + `VectorClock`**: total order (per `seq`) +
  causal order (per VC) lato Broker.

## Flusso reale di un messaggio (modalità Raft)

1. Client invia `MSG ts text` su TCP al suo broker B.
2. `ClientHandler.handleCommand` → `Broker.onClientMessage`.
3. Se B non è leader Raft: `Broker.onClientMessage` invia stringa
   `NOT_LEADER timestamp=... leaderId=...` (ignorata dal client) e
   ritorna.
4. Se B è leader: `buildChatReq` (incrementa VC locale) →
   `RaftOrderingService.propose(req)`.
5. `propose` registra un `CompletableFuture` su `localMsgId`,
   appende al log via `replicationManager.appendCommandAsLeader`,
   e blocca fino al commit (timeout 5 s).
6. Replicazione via `onHeartbeatRoundDue` →
   `AppendEntries` ai peer; quando matchIndex maggioritario raggiunge
   l'indice, `RaftCommitManager.tryAdvanceCommitIndex` committa.
7. `applyCallback` → `RaftStateMachineAdapter.accept` →
   `notifyDelivery(ChatDeliverMessage)` → completa il future ⇒
   `propose` ritorna `true` ⇒ `Broker` invia ACK al client.
8. Sui follower l'apply avviene su ricezione di `AppendEntries` con
   `leaderCommit` aggiornato; `Broker.handleOrderedMessage` →
   `HoldBackQueue` → `onChatDeliver` → `ClientHandler`.

## Flusso reale (modalità Sequencer)

- Sequencer: `handleChatRequest` assegna `seq`, broadcast UDP del
  `ChatDeliverMessage`, ACK TCP al broker proponente.
- Follower: `propose` forwarda via TCP e **ritorna true subito** →
  `Broker` ACKa il client immediatamente.
- Tutti ricevono via UDP; gap recovery via
  `RetransmissionRequestMessage` su TCP back-channel.

## Cosa accade in caso di non-leader / crash

- **Raft, follower riceve propose** → `false`, NOT_LEADER al client,
  client non sa cosa fare.
- **Sequencer, follower riceve propose** → forwarda + ACKa subito,
  rischio di perdita silenziosa.
- **Leader Raft crasha** → rielezione (200–400 ms), client perdono
  TCP, riconnettono via Directory ad un broker qualunque (anche
  follower → loop NOT_LEADER).
- **Sequencer crasha** → no failover, sistema fermo.
- **Restart broker (Raft)** → term/vote ripristinati, log **vuoto**
  (non persistito): viene riallineato dal leader via backtracking.

---

# Requirement-by-Requirement Compliance Matrix

### A. Multiple brokers on the same LAN
- **Status:** satisfied.
- **Evidenza:** `RaftConfig.voters` mappa statica multi-broker;
  `LanDiscoveryService` per discovery LAN.
- **Risk/Fix:** —

### B. Take advantage of LAN/link-layer broadcast
- **Status:** partially satisfied.
- **Evidenza:** broadcast usato per discovery (`LanDiscoveryService`)
  e per `ChatDeliverMessage` solo in modalità sequencer
  (`SequencerOrderingService.broadcastViaUdp`). RPC Raft in **TCP
  unicast** (`RaftRpcClient.sendBlocking`).
- **Risk:** scelta difendibile (cluster piccolo, affidabilità) ma non
  è stato usato l'hint principale dello spec.
- **Fix:** documentare nel report, opzionalmente UDP multicast per
  `RequestVote`.

### C. Clients on Internet, connect to one broker
- **Status:** satisfied (in demo locale).
- **Evidenza:** `ClientMain` + `DirectoryAwareClientConnection` +
  TCP al broker.
- **Risk:** `Broker.DIRECTORY_HOST = "localhost"` hardcoded; in demo
  cross-host il client deve passare `args[0]/args[1]`.
- **Fix:** parametrizzare host Directory in `Broker` e default
  `ClientMain`.

### D. Cross-broker delivery
- **Status:** satisfied in Raft (happy path).
- **Evidenza:** ogni nodo applica entry committate ai propri client.
- **Risk:** in Raft, se il client si trova su un follower, il suo
  messaggio non arriva mai (NOT_LEADER non gestito).
- **Fix:** vedi blocker B2.

### E. Same global order for every client
- **Status:** satisfied teoricamente.
- **Evidenza:** Raft applica per indice; `RaftStateMachineAdapter`
  usa `entry.getIndex()` come `seq`. `HoldBackQueue` ricontrolla.
- **Risk:** non testato end-to-end con `Broker` + client reali.
- **Fix:** test di integrazione 3 broker × N client × M messaggi.

### F. Causal order respected
- **Status:** satisfied (in modo "implicito" da Raft).
- **Evidenza:** `Broker.handleOrderedMessage` aggiorna VC locale
  (`vectorClock.update(deliver.getVectorClock())`); `HoldBackQueue.canDeliverCausally`
  fornisce ridondanza. In Raft il VC porta solo la componente del leader
  corrente, ma il *total order* di Raft estende il happens-before.
- **Risk:** la spiegazione richiede di motivare che la causale è data
  da Raft, non dai VC, in modalità Raft.

### G. No chat history for offline clients
- **Status:** satisfied.
- **Evidenza:** nessun storage di `ChatDeliverMessage` consegnati. La
  history del sequencer (1000 entry) serve solo ai NACK live.

### H. Clients receive only while connected
- **Status:** satisfied.
- **Evidenza:** `Broker.onChatDeliver` itera i `clients` connessi.

### I. No Byzantine
- **Status:** assunzione rispettata.

### J. Crash-recovery (clienti, broker, link)
- **Status:** partially satisfied.
- **Evidenza:** client riconnette; Raft persiste term/vote; TCP
  affidabile.
- **Risk:** log Raft non persistito → sotto crash multipli sequenziali
  possibili perdite di entry.
- **Fix:** blocker B1.

### K. No partitions
- **Status:** assunzione coerente. Raft semplificato (no pre-vote /
  no check-quorum) accettabile.

### L. Coerenza sotto failure assumptions
- **Status:** partially satisfied.
- **Risk:** scenario doppio crash + log non persistito.
- **Fix:** blocker B1.

### M. Real distributed Java application
- **Status:** satisfied.
- **Evidenza:** socket reali, JVM separate, build Maven, CLI.

---

# Ordering and Causality Analysis

## Ordine totale

In Raft: l'indice del log replicato. `RaftCommitManager.applyCommittedEntries`
applica strettamente in ordine (`while (lastApplied < commitIndex)`).
`RaftStateMachineAdapter` usa `entry.getIndex()` come `seq`. Tutti i
broker producono lo stesso flusso `ChatDeliverMessage(seq=k)`. ✓

In Sequencer: `++globalSeq` monotono finché il sequencer vive. ✓

## Ordine causale

L'ordine di propose dal leader è seriale (synchronized in
`buildChatReq`). Il `ClientHandler` legge da TCP serialmente per il
suo client. Cross-client la causale è preservata perché un client può
inviare un messaggio "in risposta" solo dopo averne ricevuto uno
committato → l'altro è già nel log Raft.

## Vector clocks

Usati in `Broker.buildChatReq` (incrementa la componente del broker
locale) e aggiornati in `Broker.handleOrderedMessage`
(`vectorClock.update(deliver.getVectorClock())`). In Raft mode, dato
che solo il leader produce, ogni messaggio porta la componente del
leader corrente. La continuità tra leader diversi è preservata
dall'aggiornamento al delivery. La verifica VC in
`HoldBackQueue.canDeliverCausally` funziona ma è praticamente
ridondante in Raft (la log order è già un'estensione del happens-before).

## Hold-back queue

`HoldBackQueue` enforce sia `seq == expectedSeq` sia VC causal.
In Raft non si vedono mai gap (consegna in log order), quindi il branch
gap-recovery in `Broker.handleOrderedMessage` (gated su
`instanceof SequencerOrderingService`) è codice morto in Raft. ✓

## Possibili duplicati

**CRITICO**: `ClientMessageSender` ritrasmette dopo `ackTimeoutMs`
con stesso `MSG ts text`. Sul broker leader,
`Broker.buildChatReq` genera ogni volta un nuovo `localMsgId`
(`brokerId + "-" + ++localMsgCounter`). `ChatCommand` non porta nessun
ID lato client. Retry → due commit → due deliver. Duplicato concreto.

## Possibili perdite

1. **ACK-before-commit (sequencer mode)**:
   `SequencerOrderingService.propose` su follower ritorna true subito.
2. **NOT_LEADER non gestito**: client su follower in Raft → tutti i
   suoi messaggi rifiutati.
3. **Log Raft non persistito**: doppio crash sequenziale = perdita di
   entry committate.

## ACK prima dell'ordering

- **Raft (oggi)**: NO. `propose` blocca fino al commit ⇒ ACK riflette
  il commit globale. ✓
- **Sequencer**: SÌ sul follower. ✗

## Restart e recupero

- (term, vote) ripristinati da `FileRaftPersistence.loadTermAndVote`.
- Log: ripristinato vuoto (commento esplicito "deferred").
  Verifiche grep:
  - `loadLogEntries()` chiamato solo da `FileRaftPersistence.truncateLogFrom` (interno);
  - `appendLogEntry()` mai chiamato da production.
- Lacuna principale di robustezza.

## Conclusione

Lungo l'happy path Raft, ordine totale + causale soddisfatti. Sotto
**retry client** rischio duplicati. Sotto **crash multipli** rischio
perdite. Entrambi devono essere chiusi prima di consegnare.

---

# Failure Model Analysis

- **Broker non-leader fallisce (Raft):** maggioranza sopravvive,
  leader continua, retry naturali su prossimo heartbeat. ✓
- **Leader Raft fallisce:** rielezione 200–400 ms; client perdono TCP
  e riconnettono via Directory. Possibilità di finire su un follower
  con loop NOT_LEADER (lato client). ✗
- **Client crasha:** rimosso da `Broker.removeClient` nel `finally` di
  `ClientHandler.run`. ✓
- **Link tra broker fallisce:** TCP rotto → `RaftRpcClient.sendBlocking`
  cattura silenziosamente; retry automatico su prossimo tick. ✓
- **No partitions:** assunzione accettata; Raft semplificato coerente.
- **Sequencer fallisce:** SPoF, no failover. Per questo serve Raft.
- **Restart Raft:** term/vote sì, log no. Sotto crash multipli può
  perdere committed entries.

---

# DirectoryService and LAN Broadcast Analysis

## Ruolo della DirectoryService

- `Broker.connectAndRegisterWithDirectoryService` → registrazione +
  heartbeat 3 s.
- `Broker.sendClientCountUpdate` → carico per
  `chooseBestBroker`.
- `ClientMain` → bootstrap via
  `DirectoryAwareClientConnection.open()`.
- **Non** partecipa a Raft (consenso, log, voti, quorum).

## Verdetto

Accettabile come **bootstrap helper**. Non è centrale al consenso,
quindi non entra in conflitto con lo spirito della spec. Difetti reali:

1. SPoF di bootstrap (i client già connessi continuano comunque);
2. host hardcoded su `localhost`;
3. `chooseBestBroker` ignora la leadership Raft (può mettere un nuovo
   client su un follower → bug NOT_LEADER non gestito).

## Verdetto sul broadcast LAN

- Discovery: ✓ broadcast UDP.
- Sequencer: ✓ broadcast `ChatDeliverMessage`.
- **Raft RPC: ✗** TCP unicast.

La spec suggerisce esplicitamente broadcast LAN per le RPC. Scelta
TCP è difendibile (affidabilità su cluster piccolo, niente coordination
serializzazione) ma va motivata.

---

# Sequencer / Raft Analysis

## Modalità Sequencer
- Ordine totale: SÌ.
- SPoF: SÌ (no failover) → **non sufficiente** a soddisfare "brokers
  may fail" da sola.
- ACK pre-commit lato follower: SÌ (rischioso).
- Verdetto: utile come fallback / didattico, non come architettura
  finale.

## Modalità Raft

### Leader election (`RaftElectionManager`)
- Timeout randomizzato; un solo task attivo per volta.
- Self-vote in `beginElectionTracking`.
- Promozione su maggioranza; step-down su term superiore o leader
  activity.
- Log-freshness rispettata (`RaftNode.isCandidateLogUpToDate`).
- 40 unit test in `RaftElectionManagerTest`, fake clock.

### Replication (`RaftReplicationManager`)
- `appendCommandAsLeader` leader-side.
- `handleAppendEntries` follower-side con term/role + conflict
  detection + leader activity notification.
- `handleAppendEntriesResponse` leader-side con majority commit.
- Conflict-optimized hints (`conflictTerm`/`conflictIndex`) +
  backtracking.
- 19 unit test.

### Commit (`RaftCommitManager`)
- `commitIndex`/`lastApplied` monotoni.
- `tryAdvanceCommitIndex` enforces "current-term commit rule" → ✓
  (impedisce commit di entry di term passati senza una entry del term
  corrente).
- `applyCommittedEntries` applica in ordine.

### Follower proxy / NOT_LEADER
`Broker.onClientMessage` rifiuta sul follower con stringa NOT_LEADER.
Il client la stampa soltanto. **Difetto end-to-end critico.**

### Persistenza
- `FileRaftPersistence` atomica (rename tmp → file, fsync, log code
  truncata gestita).
- `RaftNode` cabla persist su ogni mutazione di term/vote.
- **`RaftLog` non integrato:** `append`/`appendEntries`/`truncateFrom`
  non chiamano `RaftPersistence`. `RaftOrderingService.start()` non
  ricarica con `loadLogEntries()`.

### Test Raft

| Area | File | N. test | Profondità |
|---|---|---|---|
| Node | `RaftNodeTest`, `RaftVoteHandlingTest` | 14+8 | Buona |
| Election | `RaftElectionManagerTest` | 40 | Molto buona |
| Log | `RaftLogTest`, `AppendEntriesMessageTest` | 6+2 | Sufficiente |
| Commit | `RaftCommitManagerTest` | 4 | Minima |
| Replication | `RaftReplicationManagerTest` | 19 | Buona |
| Persistence | `FileRaftPersistenceTest`, `RaftPersistenceTest` | 10+10 | Solo isolato |
| RPC | `RaftRpcIntegrationTest` | 3 | Solo round-trip |
| End-to-end | `RaftOrderingServiceIntegrationTest` | 2 | Minima |

### Verdetto Raft

Implementazione **corretta sui moduli singoli**, ma incompleta
nell'integrazione (log non persistito + nessun test che esercita Raft
attraverso `Broker` con client + nessun test di failover sotto carico
o di restart con stato pieno).

---

# Test Coverage Analysis

## Coperto
- Logica interna Raft (election, log, commit, replication,
  persistence term/vote/log isolata) — buona.
- `HoldBackQueue` (total + causale) — buona.
- `VectorClock` base.
- `PeerRegistry` statico.
- `BrokerCausalDeliveryTest`, `VectorClockMultiBrokerIntegrationTest`
  testano `Broker` ma in **modalità sequencer**, con
  `ChatDeliverMessage` costruiti a mano.
- `OrderingServiceTest` testa solo `SequencerOrderingService`.
- `UdpReliabilityTest` testa NACK + UDP broadcast (sequencer).

## Non coperto
- **Raft end-to-end attraverso `Broker`**: nessun test verifica che
  N client su broker diversi vedano la stessa sequenza.
- **Failover leader sotto carico**.
- **Restart con stato persistito + ripristino consegna**.
- **Dedup retry client**.
- **Gestione NOT_LEADER lato client** (perché non implementata).
- **Causale cross-broker dopo cambio leader**.
- **Timeout di `propose()` 5 s**.

## Verdetto

Sufficienti per difendere la "correttezza interna di Raft", **non**
sufficienti per difendere "la chat consegna correttamente sotto
failure" davanti al professore.

---

# Critical Issues to Fix Before Submission

## Blocker

### B1. Log Raft non persistito (correttezza Raft sotto crash multipli)
- **Problema:** `RaftLog.append`/`appendEntries`/`truncateFrom` non
  chiamano `RaftPersistence.appendLogEntry`/`truncateLogFrom`.
  `RaftOrderingService.start()` non chiama `persistence.loadLogEntries()`.
  Commento esplicito "log replay from persistence deferred".
- **Perché importa:** la spec ammette crash multipli sequenziali. Senza
  persistenza del log, una entry committata può essere persa se la
  maggioranza dei nodi che la conosceva crasha prima di replicarla agli
  altri. Violazione della Leader Completeness di Raft.
- **File:** `RaftLog`, `RaftOrderingService`, `FileRaftPersistence`
  (già pronta).
- **Fix suggerito:**
  - iniettare `RaftPersistence` in `RaftLog`;
  - chiamare `persistence.appendLogEntry(entry)` dentro `append` (e dentro
    `appendEntries` per ogni entry nuova) **prima** di renderla visibile;
  - chiamare `persistence.truncateLogFrom(fromIndex)` dentro `truncateFrom`;
  - in `RaftOrderingService.start()`, dopo aver creato `raftLog`, fare
    `persistence.loadLogEntries().forEach(raftLog::loadFromPersistence)`
    (un nuovo metodo dedicato) **prima** di costruire `commitManager`.
- **Test:** propose 3 messaggi → uccidi 2 nodi su 3 → restart → verifica
  che le entry siano ancora applicate alla riconnessione.

### B2. `NOT_LEADER` non gestito lato client
- **Problema:** `Broker.sendNotLeaderToClient` invia una stringa testuale.
  `ClientMessageReceiver.run` la stampa soltanto (è un `String` non-ACK,
  cade nel ramo `else { System.out.println(line); }`). Il client continua
  a ritrasmettere allo stesso broker.
- **Perché importa:** in Raft, un client su follower non vede mai i
  propri messaggi consegnati.
- **File:** `ClientMessageReceiver`, `ClientMessageSender`,
  `ClientConnection`/`DirectoryAwareClientConnection`,
  `Broker.sendNotLeaderToClient`.
- **Fix suggerito:**
  1. Definire `NotLeaderResponse(leaderId, timestamp)` Serializable.
  2. In `ClientMessageReceiver` riconoscerla → callback di riconnessione
     verso il leader (es. `ClientReconnectHandler.redirectToLeader(leaderId)`).
  3. Aggiungere a `DirectoryService` la possibilità di restituire un
     broker per `brokerId`, oppure far inviare al broker corrente le coords
     del leader.
- **Test:** client → follower invia 1 msg → riceve NotLeader → si
  riconnette al leader → tutti i client vedono il msg.

### B3. Nessuna deduplicazione `(clientId, clientSeq)`
- **Problema:** `ClientMessageSender.run` ritrasmette `MSG ts text`.
  `Broker.buildChatReq` genera ogni volta un nuovo `localMsgId`
  (`brokerId + "-" + ++localMsgCounter`). `ChatCommand` non porta nessun
  ID lato client → due commit, due deliver per lo stesso messaggio.
- **Perché importa:** viola "stesso ordine senza duplicati" appena il
  client ritrasmette.
- **File:** `ChatCommand`, `ChatReqMessage`, `Broker.buildChatReq`,
  `ClientMessage`, `RaftOrderingService.propose` o
  `Broker.onClientMessage`.
- **Fix suggerito:** propagare `(clientId = username, clientSeq = ts)`
  fino al `ChatCommand`. Sul leader, mantenere
  `Map<(clientId, clientSeq), CompletableFuture<Boolean>>` short-lived.
  Se la chiave è già presente con stato "committed", restituisci ACK
  senza nuovo append.
- **Test:** client invia stesso `(ts, text)` due volte → un solo entry
  nel log + una sola consegna.

## High

### H1. ACK al client prima del commit globale (modalità sequencer)
- **Problema:** `SequencerOrderingService.propose` su follower ritorna
  `true` immediatamente. `Broker.onClientMessage` ACKa il client prima
  di sapere se il sequencer ha ordinato.
- **Fix:** introdurre `pendingProposals` anche lato sequencer; ACK al
  client solo dopo `ChatReqAck` dal sequencer.
- **Test:** stoppa il sequencer mid-flight → il client non riceve ACK.

### H2. DirectoryService SPoF + host hardcoded
- **Problema:** `Broker.DIRECTORY_HOST = "localhost"` costante;
  `ClientMain` default `localhost:60001`. Demo cross-host scomoda.
- **Fix:** parametrizzare via CLI, documentare nel README.

### H3. Raft RPC in TCP unicast
- **Problema:** la spec suggeriva broadcast LAN.
- **Fix minimo:** motivazione nel report (cluster piccolo, TCP
  affidabile, semplicità di serializzazione).
- **Fix massimo:** UDP multicast per `RequestVote` mantenendo TCP per
  `AppendEntries`.

### H4. Nessun test end-to-end Broker + Raft con client
- **Fix:** test JUnit con 3 broker Raft in-process + 2-3 client +
  N messaggi → assert stessa sequenza ordinata su ogni client.

### H5. Nessun test failover sotto carico / restart con stato pieno
- **Fix:** test che (a) uccide il leader durante una raffica e
  controlla nessun ACK perso (dopo aver cablato persistenza log);
  (b) restart di un nodo con storage popolata e verifica che riprende.

## Medium

### M1. Sequencer-style code mescolato in `Broker`
- Esempio: `Broker.handleOrderedMessage` ha `if (orderingService instanceof SequencerOrderingService)`
  per gap recovery.
- **Fix:** isolare il gap recovery in `SequencerOrderingService`, o
  introdurre un metodo `OrderingService.requestGap(seq)` no-op in Raft.

### M2. `OrderingServiceCallback.onLeaderChanged` mancante
- `RaftElectionListener.onLeaderObserved` esiste ma non è propagato a
  `OrderingServiceCallback`.
- **Fix:** aggiungere `default void onLeaderChanged(int newLeaderId, long term)`,
  cablarlo in `RaftOrderingService` come listener interno.

### M3. `pom.xml` con `<source>16` e anche `<maven.compiler.source>1.7`
- **Fix:** rimuovere le property `1.7`.

## Low

### L1. `VectorClockIntegrationTest` in `src/main/java`
- È in `it.polimi.ds.chat.integration.VectorClockIntegrationTest`.
- **Fix:** spostarlo in `src/test/java/.../integration/`.

### L2. Logging italiano `[CONN] [HB] [MAIN]` rumoroso
- Va bene per debug, ridurre prima della demo.

### L3. `BrokerConfig` con campi sequencer-only anche in Raft
- `sequencerHost`, `sequencerPort`, `udpPort`, `handlerState` sono
  passati come dummy in Raft.
- **Fix cosmetico:** factory separate.

---

# Recommended Minimal Fix Plan

## 1) Fix minimi necessari

In ordine di priorità per consegnare:

1. **Cablare la persistenza del log Raft** (B1).
2. **Gestione `NOT_LEADER` lato client** (B2).
3. **Deduplicazione retry client** (B3).
4. **Un test end-to-end Broker + Raft** (H4).

Con queste 4 fix, il progetto regge una domanda critica del professore
sulla correttezza in scenari di failover.

## 2) Miglioramenti forti se c'è tempo

5. ACK after commit anche in modalità sequencer (H1).
6. Test di failover sotto carico (H5).
7. `onLeaderChanged` su `OrderingServiceCallback` (M2).
8. Parametrizzare host della Directory (H2).

## 3) Polishing / documentazione

9. Sistemare `pom.xml` (M3).
10. Spostare `VectorClockIntegrationTest` (L1).
11. README operativo: "come avviare un cluster Raft a 3 nodi", "come
    avviare un client".
12. Documento di motivazione TCP per Raft RPC (H3).

---

# Questions I Should Be Ready to Answer

### 1. "Come garantite ordine totale tra tutti i client?"
In modalità Raft, l'ordine totale è l'indice del log replicato.
`RaftCommitManager.applyCommittedEntries` applica le entry committate
in ordine stretto di indice; `RaftStateMachineAdapter` usa
`entry.getIndex()` come `seq` del `ChatDeliverMessage`. Tutti i broker
producono lo stesso flusso `ChatDeliverMessage(seq=k, ...)` per ogni k.

### 2. "Come garantite ordine causale?"
Raft fornisce un ordine totale che *estende* il happens-before. La
seriale-ità del `ClientHandler` per ciascun client garantisce la
causale intra-client. Cross-client la causale è preservata perché un
client può inviare un messaggio "in risposta" solo dopo aver ricevuto
quello al quale risponde, che a quel punto è già committato. I vector
clock in `Broker.buildChatReq` + l'aggiornamento in
`Broker.handleOrderedMessage` forniscono un secondo livello (ridondante
in Raft, ancora utile in modalità sequencer).

### 3. "Cosa succede se il leader Raft muore?"
- Timeout di elezione (200–400 ms randomizzato in
  `RaftElectionManager`) → un follower diventa candidate, vince con
  quorum → emette heartbeat → diventa nuovo leader.
- Le entry committate restano se persistite su una maggioranza.
  **Caveat:** oggi il log non è persistito su disco, quindi sotto crash
  multipli sequenziali si possono perdere entry committate (vedi B1).
- I client connessi al vecchio leader perdono TCP, riconnettono via
  Directory. **Caveat:** possono finire su un follower (loop NOT_LEADER,
  vedi B2).

### 4. "Cosa succede se un client invia lo stesso messaggio due volte (retry)?"
**Oggi:** duplicato silenzioso. Vedi B3.
**Dopo la fix:** dedup `(clientId, clientSeq)` → un solo append.

### 5. "Perché avete una DirectoryService? Non viola la spec?"
La DirectoryService non partecipa al consenso, alla replicazione, al
quorum, all'ordering. È un puro helper di bootstrap dei client e di
load balancing iniziale (`chooseBestBroker`). I client già connessi
continuano a funzionare anche se la Directory muore. Il quorum Raft
viene da `RaftConfig.voters` statica. Quindi non viola la spec ma
introduce una dipendenza "soft" che va dichiarata.

### 6. "Perché RPC Raft in TCP e non in broadcast LAN?"
Scelta di affidabilità: il cluster è piccolo (3-5 nodi), TCP elimina
gestione lossy + frammentazione di `AppendEntries` con payload grandi.
Il broadcast LAN è comunque sfruttato per la discovery
(`LanDiscoveryService`) e, in modalità sequencer, per la diffusione
dei `ChatDeliverMessage`. Estensione futura: UDP multicast per
`RequestVote`.

### 7. "Cosa succede se due broker pensano entrambi di essere leader?"
Impossibile in Raft con maggioranza intatta e term monotono: ogni
elezione richiede `floor(N/2)+1` voti, e ogni nodo vota al massimo una
volta per term. Se due broker si autopromuovono, sono in term diversi;
quello con term inferiore farà step-down al primo `AppendEntries`
ricevuto dall'altro. Il vincolo di persistenza atomica di term/vote
(`FileRaftPersistence`) garantisce che un voto già concesso non venga
"dimenticato" dopo un riavvio.

### 8. "Cosa succede se un broker viene partizionato dalla rete?"
La spec esclude le partizioni. Tuttavia, oggi il Raft semplificato (no
pre-vote, no check-quorum) sarebbe vulnerabile a "term inflation" su
una partizione minoritaria che ritorna disponibile. Visto che la spec
esclude partizioni, l'implementazione corrente è coerente.

### 9. "Cosa testano i vostri test?"
- Election restriction, persistenza atomica term/vote, replicazione e
  conflict-optimized backtracking, commit di majority, RPC round-trip.
- Hold-back queue (total + causal).
- Vector clock + Broker (in modalità sequencer).
- **Non testano:** end-to-end client + Broker + Raft, failover sotto
  carico, restart con stato pieno, dedup retry client. Questo è il
  gap principale.

### 10. "Se il professore vi chiede di uccidere il leader durante la demo, cosa fate?"
Demo:
1. Avvio 3 broker Raft in-process (storage dir per ciascuno).
2. Avvio 3 client su 3 terminali.
3. Mostro che tutti vedono lo stesso flusso.
4. Killo il leader. Mostro che dopo ~300 ms un nuovo leader emerge.
5. Mostro che i client che erano sul vecchio leader riconnettono.
   **Caveat oggi:** se uno di loro finisce su un follower, NOT_LEADER
   loop. Va risolto B2 prima della demo.

---

# Note di stile su questo documento

Il report è scritto deliberatamente con tono critico (il prompt
chiedeva un parere "tosto" e onesto). I punti elencati come Blocker
sono tali secondo i requisiti scritti dal professore (`PROFESSOR_PROJECT_DESCRIPTION.md`)
combinati con il modello di failure dichiarato. I punti High/Medium/Low
sono giudizi di priorità relativa per la consegna.

Nessun fix è già stato applicato al codice; il documento fornisce solo
diagnosi e piano. L'utente ha chiesto esplicitamente di non scrivere
codice, ma di produrre un'analisi.
