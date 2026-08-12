# Repository compliance baseline

Aggiornata con verifica del **2026-08-12**, current `HEAD`
`e639cf7c8e20b400555b5f4ec096cc9c5ccfb832` e working tree candidato non
committato.

Questo documento non sostituisce la consegna ufficiale. La gerarchia usata e':

1. specifica ufficiale del progetto;
2. chiarimenti del professore;
3. modello e principi del corso;
4. decisioni progettuali esplicite del gruppo;
5. implementazione e test correnti.

Lo stato delle azioni e' soltanto nel tracker canonico
[`PRE_GROUP_MANUAL_TESTING_TODO.md`](PRE_GROUP_MANUAL_TESTING_TODO.md). I dettagli dei
controesempi sono in [`CODE_ISSUES_BY_GROUP.md`](CODE_ISSUES_BY_GROUP.md).

## 1. Requisiti ufficiali applicabili

### 1.1 Vincoli generali

| ID | Requisito | Conseguenza verificabile |
|---|---|---|
| R1 | Il progetto puo' essere una vera applicazione distribuita (per esempio Java) oppure una simulazione OmNet++; il gruppo ha scelto Java. | I componenti consegnati devono compilare ed eseguire come programma Java. |
| R2 | Comunicazione tramite socket Java oppure RMI. | L'implementazione usa socket TCP/UDP e Java serialization. |
| R3 | Gruppo da due a tre studenti e presentazione entro l'ultima sessione valida dell'anno accademico. | Fuori dall'audit di codice; va verificato dal gruppo. |
| R4 | Presentazione con slide e demo distribuita su almeno due notebook del gruppo, collegati alla stessa LAN. | Localhost e same-JVM non bastano; `LAN-01..03` sono gate ufficiali. |

### 1.2 Replicated Chat Infrastructure

| ID | Requisito | Interpretazione usata nell'audit |
|---|---|---|
| P1 | I broker sono sulla stessa LAN e devono sfruttare il broadcast di link disponibile. | UDP broadcast e' appropriato per messaggi piccoli comuni; lo stato peer-specific resta unicast. Serve prova sulla LAN fisica. |
| P2 | Un client si connette a un broker e comunica con client collegati anche ad altri broker; i client possono essere fuori LAN. | Client-broker e Directory-client devono essere point-to-point/routable; nessun broadcast lato client. |
| P3 | Tutti i client connessi ricevono i messaggi nello stesso ordine. | Serve un unico ordine client-visible, non soltanto un log Raft identico. Il testo dice che il client riceve messaggi “sent by others”: l'origine puo' ricevere solo ACK, ma due destinatari comuni devono osservare gli stessi messaggi nello stesso ordine. |
| P4 | L'ordine deve rispettare la causalita'. | Il total order scelto deve estendere happens-before, incluso il program order del singolo client. |
| P5 | I broker non memorizzano messaggi; i client ricevono messaggi soltanto mentre sono connessi. | Vietati inbox offline, history e replay. L'ammissibilita' del payload nel log tecnico persistente richiede chiarimento; la non esposizione ai client e' comunque obbligatoria. |
| F1 | Client, broker e collegamenti possono fallire. | Vanno gestiti i failure nello scope scelto e dimostrati senza inventare requisiti di produzione. |
| F2 | Network partition e failure Byzantine sono fuori scope. | Non sono gate di consegna; quorum safety resta una proprieta' interna di Raft. |

Il requisito non impone dynamic membership, consenso Byzantine, TLS, autenticazione,
snapshot o restart della stessa identita'. Queste feature diventano obbligatorie solo
se il gruppo le sceglie e le dichiara.

## 2. Architettura scelta e implementata

### 2.1 Componenti

- **DirectoryService:** distribuisce il voter set statico ai broker, registra endpoint
  client e heartbeat, seleziona un broker per i client. Non partecipa al consenso.
- **Broker:** mantiene sessioni client locali, costruisce proposte, riceve apply Raft,
  esegue hold-back/delivery e heartbeat verso Directory.
- **RaftOrderingService:** compone node, election, replication, commit, transport,
  persistence e dedup delle proposal.
- **ClientMain:** consulta Directory, mantiene una connessione broker, invia JOIN/chat,
  gestisce ACK, heartbeat, retry e reconnect.
- **LanDiscoveryService/PeerRegistry:** discovery ausiliaria; non modifica membership o
  quorum ed e' attualmente inefficace fra porte per-node differenti.

### 2.2 Membership

Il design usa **membership Raft statica**. Il voter set arriva dalla Directory al
bootstrap e viene copiato in `RaftConfig`; quorum e maggioranza non dipendono dalla
reachability o dal PeerRegistry. Questa scelta e' coerente con il progetto: dynamic
membership non e' un requisito.

### 2.3 Matrice dei trasporti

| Messaggio | Trasporto corrente | Motivazione |
|---|---|---|
| `RequestVote` request | UDP broadcast LAN | Piccolo e naturalmente one-to-many; la perdita e' recuperata da future election round. |
| `RequestVote` response | UDP unicast | Risposta per uno specifico candidate. |
| `AppendEntries` senza entry | UDP broadcast se identico per tutti | Heartbeat piccolo e comune; se lo stato differisce, il send resta peer-specific. |
| `AppendEntries` con payload | TCP unicast | `prevLog*` e suffisso dipendono dal `nextIndex` del follower; servono affidabilita' e payload non datagram-sized. |
| `AppendEntries` response | UDP unicast | Piccola risposta per il leader; deve essere trattata come potenzialmente duplicata/riordinata. |
| Forward client proposal | TCP unicast | Richiesta sincrona follower-leader con risposta. |
| Client-broker e client-Directory | TCP | Endpoint point-to-point, anche fuori LAN. |
| Discovery ausiliaria | UDP broadcast | Non necessaria al consenso e non va usata come prova di membership. |

La scelta ibrida e' coerente con il chiarimento del professore: scegliere il mezzo in
base al pattern del messaggio e spiegare garanzie/traffico, non usare broadcast per
tutto. Java usa directed IP broadcast sulle interfacce LAN idonee; questo e' il modo
socket-level con cui si sfrutta il broadcast disponibile, non accesso raw Ethernet.

## 3. Flussi di esecuzione

### 3.1 Startup

1. Directory riceve il voter CSV statico e apre transazionalmente i listener broker e
   client, di default `60000` e `60001` ma entrambi configurabili da CLI.
2. Ogni `BrokerMain` usa la stessa coppia `directoryHost/directoryPort` per leggere i
   voter, registrarsi, inviare heartbeat e re-registrarsi. `raft-local` usa TCP
   same-host; `raft` mantiene HYBRID come modalita' ufficiale.
3. Il broker avvia Raft e fa bind del listener client prima di pubblicare l'endpoint.
   La porta client deriva dal voter endpoint salvo override CLI esplicito.
4. Il client consulta il listener client della Directory, apre TCP al broker e invia
   JOIN. La sessione diventa destinataria solo dopo un fence Raft applicato localmente.

Il voter set copiato in `RaftConfig` resta statico e indipendente dal registro live
della Directory; readiness/re-registration non cambiano quorum o membership.

### 3.2 Chat con broker leader

1. Il client assegna `(clientId, clientSeq)` e invia `MSG`.
2. Il broker costruisce `ChatReqMessage` con identita' e vector clock.
3. Il leader appende un `ChatCommand` al log.
4. I follower replicano; il leader avanza il commit soltanto su maggioranza e secondo
   la current-term rule.
5. Il commit manager applica in ordine; l'adapter crea una delivery.
6. Il broker passa la delivery alla hold-back queue e poi ai client locali, esclusa
   l'origine identificata da `clientId`.
7. Il producer riceve ACK dopo l'esito di commit nel path nominale; l'assenza di echo
   al sender e' coerente con “receive messages sent by others”.

### 3.3 Chat con broker non leader

Il broker inoltra la proposal via TCP al leader noto e attende la risposta. Il client
rimane connesso al proprio broker. Se il forward fallisce, non viene inviato ACK e il
client ritenta. Il client production mantiene una sola head FIFO in-flight fino
all'ACK di commit; dedup e pending future sono leader-change-safe.

### 3.4 Election e failover

Un timeout avvia una candidatura, incrementa/persiste term e self-vote e invia
RequestVote. Il voto usa freshness term/index e la maggioranza del voter set statico.
Il nuovo leader inizializza `nextIndex/matchIndex`, appende una no-op del proprio term
e avvia heartbeat/replication.

Il no-op e' una tecnica Raft corretta e non consuma sequence applicativa. Append/send
sono serializzati rispetto allo step-down; election possiede il lifecycle higher-term
completo, inclusi timer, heartbeat, replication cleanup e pending future.

### 3.5 JOIN, disconnect e no-history

Non esistono API di history o inbox offline. Un client non entra nei destinatari
all'accept: il broker ordina un fence JOIN interno via Raft e lo attiva, accodando
prima WELCOME, nel callback di apply locale. Le entry precedenti non vedono la nuova
sessione e quelle successive si'. JOIN/LEAVE non producono `MSG` globali.

### 3.6 Failure client/broker

- Il quorum Raft consente nominalmente progresso con maggioranza viva.
- Il client rileva il broker tramite heartbeat/receiver e usa reconnect eventuale con
  backoff, cancellazione, quarantena endpoint e connection generation.
- JOIN/QUIT/chat/retry/heartbeat condividono un solo writer serializzato per generation.
- Il fan-out usa code bounded per sessione; uno slow consumer viene isolato senza
  bloccare apply o gli altri client.
- Startup/stop hanno rollback/teardown bounded e il vector clock viene unito per ogni
  messaggio effettivamente rilasciato, prima della visibilita'.
- Restart della stessa identita' e power-loss recovery non sono claim minimi correnti;
  i finding relativi sono `OPT-01..04`.

## 4. Argomento di ordering e stato corrente

### 4.1 Total order

In condizioni Raft valide, un log committed unico e l'application in ordine forniscono
un total order dei comandi. L'implementazione mantiene una sequence applicativa densa
e deterministica distinta dall'indice Raft: no-op e JOIN fence occupano il log senza
creare buchi client-visible. L'E2E verifica consegna cross-broker e continuita' dopo
rielezione.

### 4.2 Causal order

Un total order rispetta causalita' solo se non inserisce `b` prima di una causa `a`.
Nel path stabile, una risposta inviata dopo la ricezione di `a` viene proposta quando
`a` e' gia' committed/applied; Leader Completeness mantiene il predecessore nel log.
Il vecchio sospetto “vector clock aggiornato dopo I/O” non e' quindi, da solo, una
prova di violazione P0.

Il controesempio storico era il failure path in cui il retry di `m1` poteva arrivare
dopo `m2` dello stesso client. La FIFO single-in-flight ora impedisce che `m2` venga
trasmesso prima dell'ACK committed di `m1`; dedup rende sicuro il retry della head.
Il total order Raft conserva poi questa relazione. I metadati vettoriali vengono
aggiornati sul prefisso realmente ready prima della visibilita'. Resta utile eseguire
lo scenario dimostrativo `TEST-07/MAN-04` con catena causale e invii concorrenti.

## 5. Raft: proprieta' confermate e finding

### Confermate nel path nominale

- voter set statico, majority corretta e self-vote;
- persistenza term/voto prima della vote response osservabile;
- one-vote-per-term e log freshness;
- `prevLogIndex/prevLogTerm`, conflict repair/hints e protezione del prefisso committed;
- append nominale al leader, ordered apply, single-node commit;
- commit su maggioranza con current-term restriction;
- no-op di nuovo term e follower proposal forwarding;
- filtro UDP per cluster/voter/target/duplicate envelope id.

### Correzioni di safety/liveness verificate

- append e send non sopravvivono a una leadership/term transition;
- higher-term request/response completa un solo lifecycle di step-down;
- dedup e pending future sono ripulite su truncation, append failure e leader loss;
- `matchIndex/nextIndex` non regrediscono sotto progresso gia' confermato;
- callback di election timeout obsolete sono invalidate da una generation;
- il test applicativo con socket reali ferma il leader con maggioranza viva e
  verifica rielezione e chat successiva. Il processo OS e la LAN fisica restano prove
  manuali separate.

## 6. Storage e scope di recovery

Il codice persiste term, voto, log con payload e commit progress. Questa e' una feature
implementata, non automaticamente una garanzia richiesta. Lo scope corrente di
consegna deve essere descritto come **crash-stop** finche' `OPT-01..04` non sono
promossi e verificati. Un nodo crashato non deve essere riavviato con la stessa
identita' nella stessa esecuzione se si fa affidamento su questo scope.

Separatamente, P5 resta aperto: il docente deve confermare se il log tecnico interno
persistente e non esposto ai client sia ammesso. La parte connected-only e' chiusa dal
JOIN fence e dai test no-history; l'interpretazione dello storage tecnico resta
`CODE-11 BLOCKED_BY_DECISION`.

## 7. Matrice di conformita' corrente

| Requisito | Stato corrente | Evidenza/blocco |
|---|---|---|
| R1 Java | Implementato | Maven compila il progetto Java. |
| R2 socket/RMI | Implementato | TCP/UDP socket Java. |
| R4 due notebook e slide | **Non validato** | `LAN-01..03`; nessun test multi-host. |
| P1 LAN broadcast | Implementato in codice, non validato fisicamente | Transport ibrido coerente; serve capture/log inter-host. |
| P2 cross-broker client | Implementato e testato same-host | E2E socket reale con client su follower differenti e forwarding al leader. |
| P3 stesso ordine | Implementato e testato same-host | Sequence densa, Raft total order, stream solo chat; E2E prima/dopo rielezione. |
| P4 causalita' | Implementato e testato same-host | FIFO single-in-flight estende program order; l'E2E socket prova `m1 -> ricezione -> m2`, due osservatori e invii concorrenti nello stesso total order. `MAN-04` resta la ripetizione multi-process/LAN. |
| P5 no storage/connected-only | Connected-only verificato; decisione storage aperta | JOIN fence e nessun replay; `CODE-11` sul log tecnico e' bloccato dal docente. |
| F1 failure client/broker/link | Implementato nello scope crash-stop scelto | Leader failure E2E e multi-process locale, retry/dedup, reconnect generation-safe e Directory restart; follower/link e LAN fisica restano manuali. |
| F2 no partition/Byzantine | Correttamente fuori scope | Non va presentato come feature mancante. |

Verdetto: **NOT READY per la consegna finale, code-ready per review**. Non restano
P0/P1 software implementabili noti; mancano la decisione `CODE-11`, la prova fisica
`LAN-01/LAN-02` e gli ultimi scenari/runbook manuali del tracker.

## 8. Evidenza test verificata

Ambiente: Windows 11, Oracle JDK 23.0.2, Maven 3.9.15.

```text
mvn clean test
Tests run: 274, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

I sei casi legacy JUnit 4 sono migrati a Jupiter e inclusi nel conteggio Maven. La
suite comprende unit/component, filesystem persistence, TCP/UDP Raft integration,
client socket/reconnect, Directory lifecycle e un vero percorso applicativo con
Directory, Broker, Raft, HBQ e socket client. E' passato anche uno smoke locale con
Directory, tre broker e tre client in JVM separate, incluso leader kill, rielezione e
reconnect. Entrambe le evidenze sono same-host/loopback: non dimostrano multi-host,
firewall o broadcast fisico.

## 9. Assunzioni di deployment da dichiarare

- tutti i voter configurati appartengono allo stesso cluster e usano identici voter
  set, `clusterId` e porta UDP Raft;
- gli host del voter CSV sono raggiungibili e gli RPC/client port corrispondono ai
  processi avviati;
- firewall e access point consentono UDP broadcast fra notebook e TCP sulle porte
  Directory/Raft/client;
- la demo non dipende dalla discovery ausiliaria;
- per lo smoke con piu' broker sullo stesso host, il binding UDP comune va provato sul
  sistema operativo scelto;
- broker e client devono ricevere esplicitamente gli endpoint Directory corretti; i
  default `localhost:60000/60001` sono soltanto backward compatibility.

## 10. Claim consentiti

Supportati dal codice e dai test same-host:

- “La membership di voto e' statica e il quorum non cambia con la reachability.”
- “RequestVote ed heartbeat vuoti possono usare UDP broadcast; le repliche con
  payload e le proposal peer-specific usano TCP.”
- “Il core implementa term/vote, log matching, conflict repair e current-term commit.”
- “Il client preserva FIFO attraverso retry/reconnect e ogni generation ha un solo writer.”
- “Il JOIN crea un confine committed/applicato e non espone history precedente.”
- “La chat attraversa broker differenti e prosegue dopo leader failure con maggioranza viva.”

Non consentiti senza ulteriore evidenza/decisione:

- “Il divieto di storage ammette certamente il payload nel log persistente Raft.”
- “Crash-recovery del broker e' supportato correttamente.”
- “La LAN broadcast e la demo a due notebook sono validate.”
- “Network partition, Byzantine failure, dynamic membership o hardening production sono supportati.”
