# Repository compliance baseline

Aggiornata con audit fresco del **2026-08-11**, commit
`c77dbb981a3db837c50ef17bf9bd279278c04998`.

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

1. Directory avvia i listener broker `60000` e client `60001` con voter CSV statico.
2. Ogni `BrokerMain` legge i voter da `localhost:60000`, costruisce Raft e apre RPC/UDP.
3. Il broker si registra nella Directory e successivamente apre il listener client.
4. Il client consulta la Directory, apre TCP al broker e invia JOIN.

Limiti correnti: l'endpoint Directory dei broker e' hard-coded; il broker e'
pubblicizzato prima della client readiness. La topologia single-Directory su due host
richiede `CODE-10`; il lifecycle richiede `CODE-13`.

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
client ritenta; oggi questo percorso non preserva necessariamente FIFO fra due
`clientSeq` (`CODE-05`) e dedup/future non sono leader-change-safe (`CODE-04`).

### 3.4 Election e failover

Un timeout avvia una candidatura, incrementa/persiste term e self-vote e invia
RequestVote. Il voto usa freshness term/index e la maggioranza del voter set statico.
Il nuovo leader inizializza `nextIndex/matchIndex`, appende una no-op del proprio term
e avvia heartbeat/replication.

Il no-op e' una tecnica Raft corretta, ma la sua integrazione applicativa e' P0
(`CODE-01`). In parallelo, role/term/append non sono una transizione atomica e possono
violare State Machine Safety (`CODE-02`). Alcune higher-term response non completano
correttamente lo step-down (`CODE-03`).

### 3.5 JOIN, disconnect e no-history

Non esistono API di history o inbox offline. Tuttavia un client entra nei destinatari
senza join watermark; un follower in ritardo puo' applicare e inoltrare dopo il JOIN
un messaggio committato prima della connessione (`CODE-07`). Le notifiche JOIN/LEAVE
sono inoltre `MSG` locali non consensuali e rendono divergente lo stream visibile
(`CODE-08`, P2): vanno separate dai messaggi soggetti al requisito oppure replicate.

### 3.6 Failure client/broker

- Il quorum Raft consente nominalmente progresso con maggioranza viva.
- Il client rileva il broker tramite heartbeat, ma il reconnect e' one-shot e puo'
  riscegliere l'endpoint stale (`CODE-09`).
- Piu' thread client scrivono sullo stesso ObjectOutputStream (`CODE-06`).
- Un client lento puo' bloccare delivery/apply sincroni (`CODE-12`).
- Lifecycle parziale e bookkeeping vector-clock restano rischi P2
  (`CODE-16`, `CODE-17`).
- Restart della stessa identita' e power-loss recovery non sono claim minimi correnti;
  i finding relativi sono `OPT-01..04`.

## 4. Argomento di ordering e stato corrente

### 4.1 Total order

In condizioni Raft valide, un log committed unico e l'application crescente per
indice forniscono un ordine totale dei comandi. La delivery client-visible, pero',
aggiunge una seconda sequenza: le no-op occupano indici ma non producono delivery.
Pertanto l'implementazione corrente non realizza la chat end-to-end, anche se i test
del log passano.

Soluzioni corrette non devono rimuovere il no-op solo per ottenere indici densi. Serve
una sequenza applicativa deterministica distinta oppure un meccanismo che avanzi il
livello applicativo attraverso entry interne.

### 4.2 Causal order

Un total order rispetta causalita' solo se non inserisce `b` prima di una causa `a`.
Nel path stabile, una risposta inviata dopo la ricezione di `a` viene proposta quando
`a` e' gia' committed/applied; Leader Completeness mantiene il predecessore nel log.
Il vecchio sospetto “vector clock aggiornato dopo I/O” non e' quindi, da solo, una
prova di violazione P0.

Il controesempio confermato e' il failure path: il retry di `m1` puo' arrivare dopo
`m2` dello stesso client. Poiche' program order implica `m1 -> m2`, CODE-05 viola P4.
La vector clock broker-wide e la regola che ignora il componente sender non riparano
questa inversione. I metadati vettoriali restano utili, ma vanno validati soltanto
dopo aver garantito FIFO e total delivery.

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

### Non conformi o non provate

- `CODE-02`: leadership epoch non atomica, possibile State Machine Safety violation;
- `CODE-03`: higher-term response/lifecycle;
- `CODE-04`: dedup e pending future su leader change;
- `CODE-14`: response obsolete possono far regredire `nextIndex`;
- `CODE-15`: election timeout senza generation;
- nessuna prova process-level di leader crash con client attivi.

## 6. Storage e scope di recovery

Il codice persiste term, voto, log con payload e commit progress. Questa e' una feature
implementata, non automaticamente una garanzia richiesta. Lo scope corrente di
consegna deve essere descritto come **crash-stop** finche' `OPT-01..04` non sono
promossi e verificati. Un nodo crashato non deve essere riavviato con la stessa
identita' nella stessa esecuzione se si fa affidamento su questo scope.

Separatamente, P5 resta aperto: il docente deve confermare se il log tecnico interno
persistente e non esposto ai client sia ammesso. In ogni interpretazione, CODE-07 e la
prova connected-only sono obbligatori.

## 7. Matrice di conformita' corrente

| Requisito | Stato corrente | Evidenza/blocco |
|---|---|---|
| R1 Java | Implementato | Maven compila il progetto Java. |
| R2 socket/RMI | Implementato | TCP/UDP socket Java. |
| R4 due notebook e slide | **Non validato** | `LAN-01..03`; nessun test multi-host. |
| P1 LAN broadcast | Implementato in codice, non validato fisicamente | Transport ibrido coerente; serve capture/log inter-host. |
| P2 cross-broker client | Parziale | Forwarding esiste; nessun true E2E e Directory broker hard-coded. |
| P3 stesso ordine | **Non conforme** | `CODE-01`, `CODE-02`; CODE-08 rende inoltre ambiguo lo stream se le notifiche locali vengono incluse nel claim. |
| P4 causalita' | **Non conforme sotto failure** | `CODE-05`; nessun causal E2E. |
| P5 no storage/connected-only | **Parziale / decisione aperta** | `CODE-07`, `CODE-11`; nessuna history API. |
| F1 failure client/broker/link | Parziale | Core nominale; `CODE-03/04/06/09`, test process-level mancanti. |
| F2 no partition/Byzantine | Correttamente fuori scope | Non va presentato come feature mancante. |

Verdetto: **NOT READY**. I blocker immediati sono CODE-01, CODE-02 e LAN-01; il
tracker contiene gli ulteriori P1 necessari prima di una claim di conformita'.

## 8. Evidenza test verificata

Ambiente: Windows 11, Oracle JDK 23.0.2, Maven 3.9.15.

```text
mvn test
Tests run: 186, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Tre classi JUnit 4 non sono scoperte da Maven. Esecuzione diretta:

```text
JUnit version 4.13.1
OK (6 tests)
```

I 186 test comprendono unit test Raft/protocollo, persistence su filesystem, RPC
loopback e otto integration test di tre `RaftOrderingService` nella stessa JVM. Non
comprendono Directory/ClientMain/Broker/HBQ nel medesimo scenario, failure process,
no-history o LAN fisica.

Lo smoke multi-process fresco ha attraversato broker, Raft, apply, HBQ e socket client
diretti e ha riprodotto CODE-01 e osservato il comportamento locale di CODE-08. Il listener Directory-client non e' stato
usato perche' la porta fissa 60001 era occupata nell'ambiente; questo limite e'
riportato e non viene mascherato come true E2E.

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
- prima di CODE-10 non esiste un runbook corretto single-Directory multi-host senza
  modifica del sorgente: non vanno documentati argomenti CLI inesistenti.

## 10. Claim consentiti

Gia' supportati dal codice/test nominale:

- “La membership di voto e' statica e il quorum non cambia con la reachability.”
- “RequestVote ed heartbeat vuoti possono usare UDP broadcast; le repliche con
  payload e le proposal peer-specific usano TCP.”
- “Il core implementa term/vote, log matching, conflict repair e current-term commit.”

Non consentiti nello stato corrente:

- “La chat consegna end-to-end nello stesso ordine.”
- “La causalita' e' garantita durante failover/reconnect.”
- “I client ricevono soltanto messaggi prodotti mentre erano connessi.”
- “Crash-recovery del broker e' supportato correttamente.”
- “La LAN broadcast e la demo a due notebook sono validate.”
- “Tutti i test presenti sono eseguiti da Maven.”
