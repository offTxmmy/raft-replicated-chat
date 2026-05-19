# Raft LAN Broadcast Migration Report

Aggiornato al 2026-05-19.

## Obiettivo

Questo report descrive tutte le modifiche necessarie per rendere la
cooperazione tra broker aderente al requisito/hint della specifica:
usare la LAN broadcast/multicast tra broker, invece di usare solo TCP
unicast per le RPC Raft.

Il punto non e' la correttezza teorica di Raft: Raft funziona anche su
TCP unicast. Il punto e' che la specifica del progetto dice che i broker
sono sulla stessa LAN e che il broadcast di livello link e' disponibile
e va sfruttato dove appropriato. La nostra implementazione oggi sfrutta
UDP broadcast per discovery e per la modalita' sequencer, ma non per il
consenso Raft.

## Stato attuale

### Cosa fa oggi il codice

- `LanDiscoveryService` usa UDP broadcast solo per discovery dei broker.
- `SequencerOrderingService` usa UDP broadcast per distribuire
  `ChatDeliverMessage` in modalita' sequencer.
- `RaftRpcClient` usa TCP unicast per:
  - `RequestVoteRequestMessage`;
  - `AppendEntriesRequestMessage`.
- `RaftRpcServer` accetta le RPC Raft tramite `ServerSocket`, quindi TCP.
- `RaftElectionManager` e `RaftReplicationManager` sono gia' disaccoppiati
  dal trasporto tramite:
  - `RaftVoteRequestSender`;
  - `RaftAppendEntriesSender`.

### Cosa non rispetta bene la specifica

La specifica dice che:

- i broker condividono una LAN;
- il link-layer broadcast e' disponibile;
- il traffico broker-broker Raft (`RequestVote`, `AppendEntries`) e'
  pensato per essere trasportabile su UDP broadcast/multicast;
- le risposte possono restare unicast, perche' il destinatario e'
  specifico.

Oggi invece ogni `RequestVote` e ogni `AppendEntries` viene inviato a un
peer preciso aprendo una connessione TCP separata. Questo significa che
la modalita' Raft non sfrutta davvero la proprieta' LAN broadcast per la
cooperazione tra broker.

## Architettura target

La soluzione consigliata e' introdurre un trasporto Raft LAN-based, senza
riscrivere la logica Raft.

La logica esistente deve rimanere nei manager:

- `RaftElectionManager` decide quando chiedere voti e come contarli.
- `RaftReplicationManager` decide quando inviare `AppendEntries`, come
  gestire conflict hints, `matchIndex`, commit, retry.
- Il nuovo codice deve stare nel livello trasporto.

### Scelta consigliata

Implementare una modalita' ibrida:

1. **`RequestVote` via UDP broadcast/multicast.**
   - E' piccolo.
   - E' naturalmente destinato a tutti i peer votanti.
   - E' direttamente nella parte leader election.
   - Le risposte restano unicast.

2. **Heartbeat `AppendEntries` vuoti via UDP broadcast/multicast.**
   - Sono piccoli.
   - Sono inviati periodicamente a tutti.
   - Permettono di dire che anche la leadership activity usa LAN
     broadcast.

3. **`AppendEntries` con log entries via TCP unicast, almeno nella prima
   implementazione.**
   - Possono essere grandi.
   - UDP ha limite pratico di payload.
   - Su UDP servirebbero frammentazione, ritrasmissione e deduplica.
   - TCP e' piu' sicuro per payload grandi e catch-up dopo crash.

Questa scelta e' difendibile: sfruttiamo davvero il broadcast LAN per la
cooperazione frequente e per l'election, ma manteniamo TCP dove serve
affidabilita' stream e payload potenzialmente grande.

### Alternativa full broadcast

Per aderire al massimo alla specifica, anche `AppendEntries` con entry
di log potrebbe passare su UDP broadcast/multicast. Pero' richiede una
reliability layer applicativa:

- limite dimensione datagram;
- frammentazione e riassemblaggio;
- `messageId` per deduplica;
- retry selettivo;
- ACK/NACK per frammenti;
- backpressure per evitare di saturare la LAN.

Questa alternativa e' piu' rischiosa e va fatta solo se il professore
richiede esplicitamente che anche la replica log sia broadcast.

Ordine consigliato: questa opzione va affrontata **dopo** aver
implementato e verificato il broadcast per `RequestVote`. Prima si porta
l'election su LAN broadcast, poi si valuta l'estensione a `AppendEntries`
su UDP broadcast affidabile. In questo modo si riduce il rischio: se il
transport broadcast per election non e' stabile, non conviene usarlo
anche per la replica del log.

## Modifiche di configurazione

### `RaftConfig`

Aggiungere campi per il trasporto LAN:

- `raftTransportMode`
  - `TCP_UNICAST`;
  - `UDP_BROADCAST`;
  - `HYBRID`.
- `raftBroadcastPort`
  - porta UDP comune su cui tutti i broker ascoltano le RPC broadcast.
- `raftMulticastGroup` opzionale
  - esempio `230.0.0.10`;
  - se assente, usare broadcast IPv4 sulle interfacce.
- `udpMaxPayloadBytes`
  - limite prudente, es. 1200-1400 byte se si vuole evitare
    frammentazione IP.
- `clusterId`
  - stringa o UUID per ignorare pacchetti di altri cluster sulla stessa
    LAN.

Nota importante: oggi in `BrokerMain` la `udpPort` Raft e' costruita come
`50002 + nodeId`. Per broadcast reale non va bene: tutti i broker devono
ascoltare la stessa porta broadcast/multicast, oppure bisogna inviare un
datagram per ogni porta, che torna ad assomigliare a unicast.

### `BrokerMain`

Aggiornare la CLI Raft.

Possibile forma:

```text
raft <nodeId> <rpcPort> <votersCSV> [clientPort] [raftBroadcastPort] [transportMode]
```

Esempio:

```text
raft 0 7000 0@host0:7000,1@host1:7001,2@host2:7002 50000 7100 HYBRID
```

Per demo locale va previsto un fallback, perche' piu' processi che
ascoltano la stessa porta UDP sulla stessa macchina possono comportarsi
in modo diverso tra sistemi operativi. Per i test in-process conviene
tenere un transport fake/in-memory.

## Nuovi messaggi/protocollo

Conviene non mandare direttamente gli oggetti Raft nudi su UDP. Serve un
envelope comune.

### Nuova classe `RaftUdpEnvelope`

Package consigliato:

```text
it.polimi.ds.chat.messages.raft
```

Campi:

- `String clusterId`
- `String messageId`
- `int senderId`
- `int targetId`
  - `-1` per broadcast a tutti;
  - id specifico per risposta unicast.
- `RaftUdpMessageType type`
  - `REQUEST_VOTE_REQUEST`
  - `REQUEST_VOTE_RESPONSE`
  - `APPEND_ENTRIES_REQUEST`
  - `APPEND_ENTRIES_RESPONSE`
- `long term`
- `Object payload`
- `long createdAtMillis`

Scopi:

- filtrare pacchetti di altri cluster;
- ignorare pacchetti generati dal nodo stesso;
- deduplicare ritrasmissioni;
- distinguere richiesta e risposta;
- evitare cast ambigui in ricezione.

### Nuovo enum `RaftUdpMessageType`

Package consigliato:

```text
it.polimi.ds.chat.messages.raft
```

Valori:

```java
REQUEST_VOTE_REQUEST,
REQUEST_VOTE_RESPONSE,
APPEND_ENTRIES_REQUEST,
APPEND_ENTRIES_RESPONSE
```

### Serializzazione

Opzione piu' rapida:

- continuare con Java serialization (`ObjectOutputStream`) dentro il
  datagram.

Opzione migliore ma piu' lunga:

- serializzazione esplicita binaria o JSON.

Per coerenza col codice attuale, Java serialization e' accettabile per
una prima implementazione, ma va imposto un controllo sulla dimensione
del datagram prima dell'invio.

## Nuovo trasporto Raft UDP

### Nuova classe `RaftUdpBroadcastTransport`

Package:

```text
it.polimi.ds.chat.ordering.raft
```

Responsabilita':

- aprire una `DatagramSocket` o `MulticastSocket`;
- ascoltare la porta broadcast/multicast Raft;
- inviare envelope broadcast a tutte le interfacce LAN;
- inviare envelope unicast al mittente quando serve una risposta;
- filtrare:
  - `clusterId` diverso;
  - `senderId == localNodeId`;
  - `targetId` non locale e non broadcast;
  - messaggi da broker non presenti in `RaftConfig.voters`;
- dispatchare ai manager tramite handler gia' esistenti:
  - `RequestVoteRequestMessage` -> `electionManager.onRequestVoteRequest`;
  - `AppendEntriesRequestMessage` -> `replicationManager.handleAppendEntries`;
  - `RequestVoteResponseMessage` -> `electionManager.onRequestVoteResponse`;
  - `AppendEntriesResponseMessage` -> `replicationManager.handleAppendEntriesResponse`.

Interfacce implementate:

```java
RaftVoteRequestSender
RaftAppendEntriesSender
```

Metodi principali:

```java
void start()
void stop()
void attachHandlers(...)
void sendRequestVote(int peerId, RequestVoteRequestMessage request)
void sendAppendEntries(int peerId, AppendEntriesRequestMessage request)
```

### Semantica di `sendRequestVote`

Oggi `RaftElectionManager` chiama `sendRequestVote(peerId, request)` in
loop per ogni peer.

Per usare broadcast senza cambiare il manager ci sono due opzioni:

1. Il trasporto deduplica internamente e manda una sola broadcast per
   `(term, candidateId, lastLogIndex, lastLogTerm)`, ignorando le chiamate
   successive del loop.
2. Si cambia `RaftElectionManager` aggiungendo un metodo nuovo tipo
   `broadcastRequestVote(request)`.

Consigliata la seconda opzione solo se si vuole pulizia architetturale.
Per una patch meno invasiva, meglio la prima: il manager resta invariato
e il trasporto decide come ottimizzare.

### Semantica di `sendAppendEntries`

Per `AppendEntries` serve distinguere:

- heartbeat vuoto (`entries.isEmpty()`);
- append con payload reale.

In modalita' `HYBRID`:

- heartbeat vuoto -> broadcast UDP una sola volta per round;
- append con entries -> TCP unicast esistente.

Per fare questo senza rompere il manager:

- creare un transport composito:
  - `RaftHybridRpcClient`;
  - contiene `RaftRpcClient` TCP esistente;
  - contiene `RaftUdpBroadcastTransport`;
  - implementa entrambe le interfacce sender.

Quando riceve `sendAppendEntries(peerId, request)`:

- se `request.getEntries().isEmpty()`, invia broadcast una sola volta per
  heartbeat round;
- altrimenti delega a TCP.

Deduplica suggerita:

```text
key = term + leaderId + prevLogIndex + prevLogTerm + leaderCommit
```

Serve per evitare di broadcastare lo stesso heartbeat N volte, una per
ogni peer.

## Modifiche ai file esistenti

### `RaftOrderingService`

Modifiche:

- scegliere il trasporto in base a `RaftConfig.raftTransportMode`;
- oggi costruisce sempre `RaftRpcClient`;
- domani deve costruire:
  - `RaftRpcClient` per `TCP_UNICAST`;
  - `RaftUdpBroadcastTransport` per `UDP_BROADCAST`;
  - `RaftHybridRpcClient` per `HYBRID`.

Punto del codice attuale:

```java
rpcClient = new RaftRpcClient(localNodeId, raftConfig.getVoters());
```

Va sostituito da una factory:

```java
RaftTransport transport = RaftTransportFactory.create(...);
```

Oppure, piu' semplice:

```java
RaftVoteRequestSender voteSender = ...
RaftAppendEntriesSender appendSender = ...
```

Pero' oggi `RaftRpcClient` fa anche dispatch delle risposte tramite
`attachHandlers`. Per pulizia conviene introdurre una piccola interfaccia
comune.

### Nuova interfaccia `RaftTransport`

Package:

```text
it.polimi.ds.chat.ordering.raft
```

Firma:

```java
public interface RaftTransport extends RaftVoteRequestSender, RaftAppendEntriesSender {
    void attachHandlers(
        Consumer<RequestVoteResponseMessage> voteResponseHandler,
        BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler
    );
    void start();
    void stop();
}
```

Poi:

- `RaftRpcClient` implementa `RaftTransport`;
- `RaftUdpBroadcastTransport` implementa `RaftTransport`;
- `RaftHybridRpcClient` implementa `RaftTransport`.

`RaftOrderingService` tiene:

```java
private RaftTransport raftTransport;
```

invece di:

```java
private RaftRpcClient rpcClient;
```

### `RaftRpcClient`

Modifiche:

- far implementare `RaftTransport`;
- opzionalmente rinominarlo in `RaftTcpTransport`, ma non e' necessario;
- tenerlo come fallback affidabile per append con payload.

### `RaftRpcServer`

In modalita' full UDP non sarebbe piu' necessario per le RPC Raft.
Pero' in modalita' ibrida serve ancora per:

- append con entries;
- risposte se si decide di rispondere via TCP;
- fallback in demo locale.

Consigliato: non rimuoverlo. Tenerlo e farlo partire solo quando il
transport mode lo richiede.

### `RaftElectionManager`

Possibile nessuna modifica se il transport deduplica le chiamate
`sendRequestVote(peerId, request)`.

Modifica migliore ma piu' invasiva:

- introdurre una nuova interfaccia:

```java
interface RaftVoteBroadcaster {
    void broadcastRequestVote(RequestVoteRequestMessage request);
}
```

- cambiare `beginElectionTracking` per inviare una sola richiesta
  broadcast invece del loop su ogni peer.

Rischio: piu' test da aggiornare.

### `RaftReplicationManager`

Possibile nessuna modifica se il transport composito deduplica heartbeat
broadcast.

Modifica migliore ma piu' invasiva:

- separare esplicitamente:
  - `sendHeartbeatToAll`;
  - `sendAppendEntriesToFollower`.

Questo permetterebbe di broadcastare heartbeat in modo naturale e usare
unicast solo per entry reali o backtracking.

### `LanDiscoveryService`

Due possibilita':

1. Lasciarlo separato.
   - Discovery resta discovery.
   - Raft UDP transport ha la propria porta e il proprio protocollo.
   - Meno rischio di mischiare pacchetti diversi.

2. Riutilizzare parte del codice di broadcast.
   - Estrarre utility comune:

```text
UdpBroadcastSupport
```

Responsabilita':

- iterare network interface;
- inviare datagram su ogni broadcast address;
- ignorare loopback/down interface;
- gestire `setBroadcast(true)`.

Consigliata la seconda come refactor piccolo dopo aver fatto funzionare
il transport.

### `BrokerConfig`

Oggi `udpPort` e' generico e nasce dalla modalita' sequencer. In Raft
serve distinguere:

- `discoveryUdpPort`;
- `sequencerDataUdpPort`;
- `raftBroadcastPort`.

Se non si vuole refactorare troppo:

- aggiungere solo `raftBroadcastPort` dentro `RaftConfig`;
- lasciare `BrokerConfig.udpPort` per discovery/sequencer.

### `PeerRegistry`

Non deve diventare sorgente di membership.

Puo' servire per indirizzi di risposta unicast, ma il quorum deve restare
basato su `RaftConfig.voters`.

Regola da mantenere:

- se arriva un pacchetto UDP da un broker non presente in `voters`, il
  pacchetto si ignora.

## Reliability layer necessaria

UDP broadcast non garantisce consegna. Raft puo' tollerare perdita di
messaggi, ma il transport deve evitare alcuni problemi pratici.

### Deduplica

Ogni envelope deve avere `messageId`.

Ogni nodo mantiene una cache bounded:

```text
Map<String, Long> recentlySeenMessageIds
```

Con TTL breve, per esempio 30 secondi.

Serve per ignorare:

- ritrasmissioni;
- duplicati causati dal broadcast su piu' interfacce;
- pacchetti ricevuti due volte da OS/rete.

### Retry

Non serve ACK applicativo per `RequestVoteRequest`: se un voto si perde,
l'election timeout genera una nuova elezione.

Serve invece la normale risposta:

- `RequestVoteResponseMessage` unicast al candidato.

Per heartbeat persi:

- non e' grave se sporadico;
- se se ne perdono troppi, parte una nuova election, come in Raft.

Per `AppendEntries` con log entries:

- se resta TCP, la retry logic attuale resta valida.
- se si passa full UDP, serve ACK/NACK e retry esplicito.

### Dimensione pacchetti

Per UDP bisogna evitare datagram grandi.

Regola consigliata:

- `RequestVote`: sempre UDP.
- `AppendEntries` vuoto: sempre UDP.
- `AppendEntries` con entries:
  - se serialized size <= `udpMaxPayloadBytes`, opzionalmente UDP;
  - altrimenti TCP.

Per evitare bug, nella prima versione ibrida fare:

- entries vuote -> UDP;
- entries non vuote -> TCP.

## Test da aggiungere

### Unit test del transport

Nuovo file consigliato:

```text
src/test/java/it/polimi/ds/chat/ordering/raft/RaftUdpBroadcastTransportTest.java
```

Test:

- ignora pacchetti con `clusterId` diverso;
- ignora pacchetti inviati da se stesso;
- ignora sender non presente in `voters`;
- deduplica `messageId`;
- dispatcha `RequestVoteRequestMessage` al vote handler;
- invia `RequestVoteResponseMessage` al candidato;
- dispatcha heartbeat `AppendEntriesRequestMessage` all'append handler.

### Test election con broadcast

Nuovo o esteso:

```text
RaftElectionManagerTest
```

Test:

- una election produce una sola broadcast `RequestVote`;
- duplicati di `RequestVote` non causano doppio voto;
- perdita di una request non rompe: nuova election dopo timeout.

### Integration test 3 nodi

Nuovo file consigliato:

```text
RaftUdpTransportIntegrationTest
```

Test:

- 3 nodi sulla stessa porta UDP broadcast/multicast;
- un nodo diventa candidate;
- gli altri ricevono `RequestVote` via UDP;
- il candidato riceve risposte unicast;
- viene eletto un leader.

Per CI e Windows, meglio avere anche un fake in-memory transport, perche'
il broadcast reale puo' essere instabile o bloccato dall'ambiente.

### Regression test TCP fallback

Testare che `AppendEntries` con entries reali usi ancora TCP in modalita'
`HYBRID`.

## Roadmap consigliata

### Fase 1 - Preparazione interfacce

- Introdurre `RaftTransport`.
- Far implementare `RaftTransport` a `RaftRpcClient`.
- Cambiare `RaftOrderingService` per dipendere da `RaftTransport`.
- Nessun cambio funzionale: deve compilare e passare i test esistenti.

### Fase 2 - UDP broadcast per RequestVote

- Aggiungere `RaftUdpEnvelope`.
- Aggiungere `RaftUdpMessageType`.
- Aggiungere `RaftUdpBroadcastTransport`.
- In modalita' `HYBRID`, fare `RequestVote` via UDP broadcast.
- Risposte `RequestVoteResponse` unicast.
- Aggiornare report/tracking: H3 parzialmente risolto.

### Fase 3 - UDP broadcast per heartbeat

- Estendere `RaftHybridRpcClient`.
- Broadcastare solo `AppendEntries` vuoti.
- Lasciare entries reali su TCP.
- Aggiungere test heartbeat.

### Fase 4 - Valutare full UDP AppendEntries

Solo se richiesto:

- portare anche `AppendEntries` con log entries su UDP broadcast
  affidabile;
- frammentazione;
- ACK/NACK frammenti;
- riassemblaggio;
- retry;
- max payload;
- test di perdita/duplicazione.

Questa fase deve venire dopo la Fase 2 (`RequestVote` broadcast) e dopo
una verifica pratica su LAN reale o ambiente di test equivalente.

## Impatto sui ruoli del progetto

### Person A

Coinvolta soprattutto in:

- `RequestVote` broadcast;
- election con messaggi duplicati o persi;
- test di leader election via broadcast;
- documentazione della scelta.

### Person B

Coinvolta se si tocca:

- `AppendEntries`;
- heartbeat;
- replication;
- conflict hints;
- commit.

### Person C

Coinvolta per:

- transport layer;
- `RaftOrderingService`;
- config;
- integration test;
- demo cross-host.

## Rischi principali

- UDP broadcast puo' essere filtrato da firewall o rete universitaria.
- In locale, piu' JVM sulla stessa porta UDP possono comportarsi in modo
  diverso tra Windows/Linux/macOS.
- Broadcast su piu' interfacce puo' generare duplicati.
- UDP non garantisce ordine, consegna o assenza di duplicati.
- Full UDP per `AppendEntries` rischia di introdurre piu' bug di quanti
  ne risolva.

## Raccomandazione finale

Non conviene riscrivere tutta la replica Raft su UDP subito.

La modifica migliore per aderire alla specifica e restare pragmatici e':

1. introdurre transport Raft ibrido;
2. usare UDP broadcast/multicast per `RequestVote`;
3. usare UDP broadcast/multicast per heartbeat `AppendEntries` vuoti;
4. mantenere TCP unicast per `AppendEntries` con log entries;
5. documentare chiaramente che il broadcast LAN e' sfruttato per election
   e leadership activity, mentre la replica payload resta TCP per evitare
   frammentazione e ritrasmissione manuale.

Questa soluzione permette di rispondere al professore:

> Usiamo la LAN broadcast dove porta beneficio e dove il payload e'
> piccolo: election e heartbeat. Manteniamo TCP per la replica delle log
> entries perche' e' il punto dove affidabilita', dimensione payload e
> backtracking sono piu' delicati. La membership resta statica e sicura:
> il broadcast non cambia il quorum, serve solo come trasporto.
