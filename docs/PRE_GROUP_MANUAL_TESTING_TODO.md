# Tracker canonico pre-consegna

Audit aggiornato: **2026-08-12**

Commit di partenza e current `HEAD` ispezionato:
`ff5062d5f3be90485b65dfddf77e6fe31c029d45`; le correzioni mirate correnti sono
nel working tree per review.

Verdetto corrente: **NOT READY**

Questo e' il ledger dettagliato del tracker canonico esposto da
`PROJECT_DELIVERY_AUDIT.md`. Gli altri documenti sono baseline normative,
approfondimenti tecnici o storico e non costituiscono backlog paralleli.

## Regole del tracker

Categorie:

- **REQUIRED BY SPECIFICATION**: richiesto dalla consegna ufficiale;
- **REQUIRED FOR CORRECTNESS**: necessario per la correttezza dell'architettura scelta;
- **RECOMMENDED FOR DEMO/ROBUSTNESS**: non imposto letteralmente, ma necessario per
  una demo credibile o per isolare failure realistiche;
- **OPTIONAL / PRODUCTION HARDENING**: fuori dal minimo della consegna.

Priorita': `P0` blocker/safety violation certa, `P1` alta, `P2` media, `P3` bassa.
Stati: `OPEN`, `BLOCKED_BY_DECISION`, `READY_TO_VERIFY`, `VERIFIED`, `OPTIONAL`.

La consegna non e' pronta finche' restano aperti `P0`, `P1` classificati
**REQUIRED BY SPECIFICATION** o **REQUIRED FOR CORRECTNESS**, oppure il gate fisico
su due notebook non e' `VERIFIED`.

## Evidenza corrente

- Baseline pre-modifica su `ff5062d`: `mvn clean test` ha eseguito **274 test**, 0
  failure, 0 error, 0 skipped. Il working tree iniziale conteneva soltanto
  `?? raft-data/`, preesistente e intenzionalmente non toccato.
- Suite finale: `mvn clean test` esegue **283 test**, 0 failure, 0 error, 0 skipped.
- I sei test JUnit 4 prima esclusi sono migrati a Jupiter e vengono ora scoperti dalla
  normale suite Maven; la dipendenza JUnit 4 compile-scope e' stata rimossa.
- Il nuovo `ReplicatedChatApplicationIntegrationTest` attraversa il vero percorso
  `client socket -> Broker -> Raft -> apply -> HoldBackQueue -> client socket` con
  Directory, tre broker `LOCAL_TCP`, follower forwarding, ACK/no-echo, JOIN
  no-history e leader failover. E' multi-broker e usa socket reali, ma resta
  **in-process/same-host**.
- Uno smoke separato ha avviato processi OS reali: una Directory, tre `BrokerMain` e
  tre `ClientMain`, distribuiti uno per endpoint broker. Quattro chat sono arrivate
  ai destinatari nello stesso ordine e una sola volta; il sender non ha visto echo.
  Dopo il kill del leader, il cluster 2/3 ha eletto un nuovo leader e il client
  collegato al processo terminato si e' riconnesso continuando la chat. Ambiente
  same-host/loopback `LOCAL_TCP`: non e' una prova LAN fisica HYBRID.
- Test deterministici aggiuntivi coprono FIFO/retry/reconnect, un solo writer per
  generation, write bloccate, slow consumer, Directory restart/replacement,
  lifecycle transazionale, higher-term `AppendEntries`, response obsolete, timer
  cancellati e vector clock su messaggi ready.
- Lo scenario applicativo causale usa il protocollo socket reale: A invia `m1`, B su
  un altro broker la riceve prima di inviare `m2`; due osservatori vedono `m1 < m2`.
  Invii concorrenti da A/B producono lo stesso total order per entrambi, ACK esatti,
  nessun self-echo, perdita o duplicazione.
- Non sono stati eseguiti test multi-host o su due notebook. La modalita' HYBRID e'
  coperta same-host, ma broadcast cross-host, firewall e subnet restano gate manuali.

---

# 1. Code fixes

## CODE-01 - Separare la sequenza applicativa dagli indici Raft

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P0
- **Area/file:** `RaftOrderingService`, `RaftStateMachineAdapter`, `Broker`,
  `HoldBackQueue`
- **Problema:** ogni leader appende una no-op; l'adapter la scarta ma usa il raw log
  index per la chat. La queue attende una sequenza densa da 1.
- **Impatto:** su cluster fresco no-op `index=1`, prima chat `seq=2`; la chat e tutte
  le successive restano bufferizzate. Ogni elezione aggiunge un nuovo buco.
- **Azione:** mantenere la no-op Raft e introdurre una sequenza applicativa densa e
  deterministica, oppure far avanzare esplicitamente la delivery per ogni indice
  interno senza esporre no-op ai client. Definire anche il recovery del watermark.
- **Verifica:** prima chat e prima chat dopo rielezione arrivano a client su broker
  differenti; nessun `Gap detected`; sequenze client-visible contigue.
- **Esito 2026-08-12:** la sequenza applicativa densa resta separata dagli indici
  Raft; no-op e JOIN barrier non consumano sequence. Il nuovo E2E applicativo verifica
  `MSG 1`, consegne successive e continuita' dopo rielezione.
- **Stato:** VERIFIED

## CODE-02 - Rendere atomica la leadership rispetto ad append e send

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P0
- **Area/file:** `RaftNode`, `RaftElectionManager`, `RaftReplicationManager`,
  `RaftLog`
- **Problema:** election e replication usano lock distinti. Dopo il check
  `role == LEADER`, un `RequestVote` higher-term puo' rendere il nodo follower;
  l'append riprende, legge il nuovo term e inserisce comunque un comando.
- **Impatto:** il vecchio leader puo' creare `x` con lo stesso `(index, term)` del
  comando `y` del vero leader. Il conflict check confronta il term, non il payload,
  quindi due state machine possono applicare comandi diversi allo stesso indice.
- **Azione:** serializzare le transizioni Raft in un solo event loop/lock oppure
  introdurre una leadership epoch atomica verificata immediatamente prima di append
  e invio; nessuna mutazione o heartbeat deve sopravvivere allo step-down.
- **Verifica:** test deterministico con latch fra role-check e append/send; dopo
  step-down non compare alcuna entry o heartbeat della vecchia leadership.
- **Esito 2026-08-12:** gia' chiuso dai fix Raft precedenti su `master`; restano verdi
  i regression test deterministici su append/send e step-down.
- **Stato:** VERIFIED

## CODE-03 - Processare il termine superiore prima dei filtri di ruolo

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P1
- **Area/file:** `RaftElectionManager.onRequestVoteResponse`,
  `RaftReplicationManager.handleAppendEntriesResponse`
- **Problema:** una response viene scartata se il nodo non e' piu' candidate/leader
  prima di esaminarne il term. Lo step-down effettuato dalla replication non arma un
  nuovo election timeout nel manager election.
- **Impatto:** un nodo puo' ignorare evidenza di term superiore o restare follower
  senza timeout dopo lo step-down, compromettendo convergenza e liveness.
- **Azione:** centralizzare l'osservazione higher-term: persistenza, ruolo follower,
  stop heartbeat, pulizia election/replication, nuovo follower timeout e callback.
- **Verifica:** response higher-term ritardata dopo cambio ruolo aggiorna sempre il
  term; dopo step-down il fake clock osserva una nuova election se non arriva leader
  activity.
- **Esito 2026-08-12:** gia' chiuso su `master`; election possiede term/role/timer e
  ogni response higher-term viene osservata prima dei filtri di ruolo.
- **Stato:** VERIFIED

## CODE-04 - Rendere dedup e retry leader-change-safe

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P1
- **Area/file:** `RaftOrderingService.pendingCommits`,
  `committedProposalKeys`, truncation hook e state-machine apply
- **Problema:** il futuro leader non riconosce una key gia' nel log ma non ancora
  applicata e puo' appendere il retry una seconda volta. Una proposal troncata lascia
  inoltre una future pendente che impedisce ogni nuovo append della stessa key.
- **Impatto:** doppia delivery dopo failover oppure retry che scadono per sempre dopo
  truncation e successiva rielezione.
- **Azione:** associare key, log index e stato `in-flight/committed`; deduplicare in
  modo deterministico nella state machine; chiudere/rimuovere future su truncation e
  leadership loss senza produrre ACK prematuri.
- **Verifica:** crash/leader change prima dell'ACK e prima del commit noto al nuovo
  leader produce una sola application; una key troncata puo' essere riproposta.
- **Esito 2026-08-12:** dedup deterministica, cleanup su truncation/append failure e
  leadership loss sono verificati. Il percorso residuo higher-term `AppendEntries`
  passa ora da `RaftElectionManager`, fallisce le vere `pendingCommits`, cancella il
  lifecycle leader una sola volta e continua append/commit/apply.
- **Stato:** VERIFIED

## CODE-05 - Preservare il program order del singolo client

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** `ClientMessageSender`, `ClientHandler`, `RaftOrderingService`
- **Problema:** piu' messaggi possono restare pending; dopo un rifiuto/forward fallito
  il comando successivo puo' essere accettato dal nuovo leader prima del retry del
  precedente. Il server deduplica la key ma non impone `nextClientSeq`.
- **Impatto:** `m2` puo' precedere `m1` per lo stesso client, violando program order e
  quindi causalita'. La vector clock broker-wide non ripara questo interleaving.
- **Azione:** mantenere un solo messaggio client in-flight fino ad ACK oppure
  introdurre gating server-side per `(clientId, nextClientSeq)` con gestione dei gap.
- **Verifica:** due messaggi dello stesso client durante failover/reconnect sono
  committati e consegnati una sola volta nell'ordine `1,2`.
- **Esito 2026-08-12:** `ClientMessageSender` usa una FIFO single-in-flight: solo la
  testa e' inviata/ritentata e soltanto il suo ACK committed abilita la successiva.
  Test component e socket verificano wire order `1,1,2`, reconnect e ACK obsoleti.
- **Stato:** VERIFIED

## CODE-06 - Usare un solo writer per ogni ObjectOutputStream client

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P1
- **Area/file:** `ClientMain`, `ClientMessageSender`, `ClientHeartbeatManager`,
  `DirectoryAwareClientConnection`
- **Problema:** JOIN/QUIT, messaggi, retry e heartbeat scrivono concorrentemente sullo
  stesso `ObjectOutputStream`, che non e' thread-safe.
- **Impatto:** stream corrotto, disconnessione e perdita di comandi anche nel percorso
  normale; il cambio generazione della connessione aumenta la race.
- **Azione:** introdurre un writer/lock condiviso per connessione e una transizione
  atomica di generazione; idealmente una coda outbound bounded.
- **Verifica:** stress socket con heartbeat, input e retry concorrenti; il broker
  deserializza tutti gli oggetti e gli ACK corrispondono alle key attese.
- **Esito 2026-08-12:** JOIN, QUIT, chat/retry e heartbeat condividono un
  `ClientObjectWriter` serializzato per generation; invalidazione, socket close e
  quiescenza impediscono scritture tardive sul vecchio stream.
- **Stato:** VERIFIED

## CODE-07 - Introdurre un watermark di JOIN per la semantica no-history

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** `Broker`, `ClientHandler`, commit/apply follower
- **Problema:** il client entra nella lista destinatari all'accept, senza confine di
  join. Un follower arretrato puo' applicare in seguito entry gia' committate prima
  della connessione e inviarle al nuovo client.
- **Impatto:** esposizione accidentale di history a un client che non era connesso.
- **Azione:** definire un join watermark certo rispetto al commit globale/apply locale
  oppure accettare client solo dopo catch-up; non usare il log tecnico come replay.
- **Verifica:** client collegato dopo il commit ma prima dell'apply locale non riceve
  il vecchio messaggio e riceve il primo messaggio successivo al JOIN.
- **Esito 2026-08-12:** ogni JOIN propone un fence interno Raft. La sessione resta
  pending fino all'apply locale; WELCOME e attivazione avvengono atomicamente nel
  commit loop prima dell'entry successiva. Test con latch ed E2E provano no-history.
- **Stato:** VERIFIED

## CODE-08 - Separare JOIN/LEAVE dallo stream chat globale

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `Broker.sendSystemNotification`, `ClientHandler`
- **Problema:** JOIN/LEAVE usano `MSG` e una sequenza locale per broker, bypassando
  Raft. Client su broker differenti vedono payload diversi allo stesso numero.
- **Impatto:** se presentate come messaggi globalmente ordinati, contraddicono il
  claim e collidono con le sequenze chat; lo smoke fresco ha mostrato due diversi
  `MSG 1`. Se sono eventi UI locali fuori dal requisito, vanno resi distinguibili.
- **Azione:** rimuovere queste notifiche, assegnare un tipo esplicitamente non ordinato
  e fuori dal contratto chat, oppure ordinarle via consenso.
- **Verifica:** confrontare gli stream di client su broker diversi durante JOIN/LEAVE;
  i messaggi soggetti al contratto globale devono coincidere.
- **Esito 2026-08-12:** le notifiche automatiche JOIN/LEAVE sono rimosse. Il fence di
  sessione e' interno e non consuma sequence; solo chat committate producono `MSG`.
- **Stato:** VERIFIED

## CODE-09 - Rendere la riconnessione eventuale e FIFO

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P1
- **Area/file:** `ClientMain`, `DirectoryAwareClientConnection`, Directory reaper
- **Problema:** al failure il client effettua un solo reconnect. La Directory conserva
  il broker morto fino al reaper, quindi il tentativo puo' riscegliere subito lo
  stesso endpoint e non essere mai ripetuto.
- **Impatto:** client permanentemente disconnesso e pending non consegnati nonostante
  esistano broker vivi.
- **Azione:** retry con backoff/jitter e cancellazione su `/quit`, esclusione temporanea
  dell'endpoint fallito e riaggancio atomico di writer/receiver/heartbeat; combinare
  con CODE-05 e CODE-06.
- **Verifica:** kill del broker con Directory ancora stale; il client raggiunge un
  altro broker e consegna i pending in FIFO senza duplicati.
- **Esito 2026-08-12:** controller single-worker con backoff 250 ms..2 s, cancellazione
  su quit, callback generation-aware e bootstrap iniziale nello stesso loop eventuale.
  La richiesta Directory porta gli id temporaneamente esclusi, quindi un endpoint
  ancora registrato ma irraggiungibile per quel client non impedisce la selezione di
  un broker vivo. Test con Directory reale copre stale preferred endpoint, rotazione,
  JOIN e pending head. Un secondo test forza il failure dopo l'installazione su A,
  lascia A registrato/preferito, verifica `excluded={A}`, JOIN/retry su B e ACK1 ->
  seq2 -> ACK2/QUIT senza inversioni o duplicati.
- **Stato:** VERIFIED

## CODE-10 - Rendere configurabile la Directory dei broker

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P1
- **Area/file:** `BrokerMain`, `Broker`
- **Problema:** bootstrap membership, registrazione e heartbeat usano
  `localhost:60000`; solo il client accetta host/porta Directory.
- **Impatto:** un Directory Service unico sul notebook A non puo' servire un broker
  sul notebook B. Directory separate per host sarebbero una topologia diversa e
  frammenterebbero registrazione/load balancing.
- **Azione:** aggiungere una sola configurazione coerente di host/porta Directory,
  propagarla a bootstrap e runtime e fallire esplicitamente su mismatch.
- **Verifica:** broker su due notebook usano lo stesso Directory Service senza edit
  del sorgente; voter set, endpoint pubblicizzati e registrazioni coincidono.
- **Esito 2026-08-12:** host/porta Directory sono in `BrokerConfig` e CLI e vengono
  usati da bootstrap, registration, heartbeat e re-registration. Il voter set resta
  statico e indipendente. L'RPC port CLI deve coincidere con l'endpoint del voter
  locale e il client port viene pubblicato coerentemente. Config e socket integration
  same-host sono verdi; la prova fisica cross-host resta `LAN-01/LAN-02`, non un CODE
  aperto.
- **Stato:** VERIFIED

## CODE-11 - Chiudere il contratto “brokers do not store messages”

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** design Raft, `FileRaftPersistence`, documentazione
- **Problema:** il log persistente contiene il payload chat completo. E' certo che non
  esistono history API/replay intenzionale; non e' certo che il log tecnico persistito
  sia ammesso dal testo letterale della consegna.
- **Impatto:** claim di conformita' P5 non dimostrabile e possibile violazione
  letterale del requisito.
- **Azione:** chiedere conferma al docente. Se l'internal log e' ammesso, documentare
  invisibilita' ai client e retention; altrimenti adottare crash-stop senza restart
  della stessa identita' o un design conforme esplicitamente approvato.
- **Verifica:** risposta docente o decisione progettuale tracciata, comportamento
  no-history provato da TEST-05/MAN-05.
- **Analisi 2026-08-12:** `FileRaftPersistence` conserva il `ChatCommand` completo
  per log matching, replica e recovery della replicated state machine. Non esistono
  history API, offline inbox o replay volontario; il JOIN fence impedisce anche la
  history accidentale durante il catch-up. `cachedClientRequests` mantiene request e
  vector clock soltanto per retry falliti/non confermati e rimuove l'entry dopo la
  conferma definitiva di commit. La retention applicativa delle proposal concluse e'
  quindi chiusa; resta quella del log tecnico. Rimuovere il payload senza un modello
  alternativo romperebbe Raft/recovery e non viene fatto senza decisione esterna.
- **Domanda docente:** “Nel requisito *brokers do not store messages*, e' ammesso che
  il log tecnico persistente di Raft contenga temporaneamente il payload completo per
  replica, commit e recovery, pur non essendo mai esposto come history/replay ai
  client? Se no, quale modello di recovery e retention e' richiesto?”
- **Alternative se vietato:** broker crash-stop senza riuso della stessa identita' e
  senza recovery locale; storage volatile con perdita dello stato al crash; oppure
  log cifrato/esterno e retention/snapshot esplicitamente approvati. Tutte richiedono
  una scelta architetturale/docente e non sono fix cosmetici equivalenti.
- **Stato:** BLOCKED_BY_DECISION

## CODE-12 - Isolare Raft dai client lenti

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `Broker`, `ClientHandler`, `RaftCommitManager`
- **Problema:** l'application callback scrive sincronicamente ai client mantenendo il
  lock della lista; un socket che non legge puo' bloccare apply e replication. Se una
  callback lancia, il servizio logga l'errore ma il commit manager avanza comunque
  `lastApplied`, senza una policy esplicita per gli altri client locali.
- **Impatto:** un client lento puo' fermare altri client locali e destabilizzare il
  leader.
- **Azione:** snapshot della lista e code outbound bounded per sessione; disconnettere
  il solo slow consumer senza bloccare il thread Raft; separare l'application
  deterministica dalla fan-out best-effort e isolare le failure per sessione.
- **Verifica:** un client non legge mentre un altro continua a ricevere e il cluster
  mantiene heartbeat/commit entro deadline.
- **Esito 2026-08-12:** fan-out su snapshot e `offer` non bloccante verso una coda
  bounded per sessione; un solo worker effettua I/O. Queue full/write failure chiude
  soltanto lo slow consumer e l'application callback ritorna senza attendere socket.
- **Stato:** VERIFIED

## CODE-13 - Correggere readiness e lifecycle Directory

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `Broker.start`, heartbeat Directory, `DirectoryService`
- **Problema:** il broker si registra prima del bind client; dopo errore Directory non
  si registra nuovamente. Re-registration dello stesso id/endpoint non e' atomica; il
  reaper puo' rimuovere un record usando un timestamp letto prima di un heartbeat
  concorrente e un endpoint vecchio puo' restare nella mappa per-config.
- **Impatto:** endpoint non pronto o stale, cluster vivo ma invisibile ai nuovi client.
- **Azione:** bind prima della pubblicazione, rollback su startup failure, loop di
  re-registration e sostituzione atomica del record per broker id.
- **Verifica:** porta client occupata non pubblica il broker; restart Directory porta
  alla ricomparsa dei broker senza riavviarli.
- **Esito 2026-08-12:** bind client precede la pubblicazione; il broker mantiene un
  loop di re-register. Directory usa uno slot atomico/epoch per broker id, replacement
  generation-safe e startup dei due listener transazionale. Testano bind failure,
  restart, stale replacement e heartbeat/reaper.
- **Stato:** VERIFIED

## CODE-14 - Rendere monotono lo stato di replica rispetto a response obsolete

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `RaftReplicationManager`, RPC/UDP response metadata
- **Problema:** una success response vecchia puo' ridurre `nextIndex`; una failure
  vecchia puo' fare backtrack dopo un successo recente. Non esiste una RPC generation.
- **Impatto:** retry inutili e possibile liveness degradata sotto riordino/duplicati;
  `matchIndex` resta monotono, quindi non e' confermata una safety violation.
- **Azione:** correlare response a term/leadership epoch e send generation; imporre
  `nextIndex >= matchIndex + 1` e ignorare backtrack obsoleti.
- **Verifica:** consegnare `success(10)`, `success(5)`, `failure(2)` fuori ordine;
  `nextIndex` non regredisce sotto 11.
- **Esito 2026-08-12:** `matchIndex` e `nextIndex` sono monotoni e ogni backtrack e'
  clampato a `matchIndex + 1`; il trace `success(10), success(5), failure(2)` e' testato.
- **Stato:** VERIFIED

## CODE-15 - Proteggere i timer election da callback cancellate

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `RaftElectionManager`
- **Problema:** `cancel()` non impedisce a un task gia' partito di entrare dopo un
  heartbeat valido; non c'e' una generation del timeout.
- **Impatto:** election spurie sotto interleaving sfavorevole, senza prova corrente di
  violazione safety.
- **Azione:** token/generation validato nel callback o event loop seriale.
- **Verifica:** fake clock/latch con vecchio callback dopo reset; il term non avanza.
- **Esito 2026-08-12:** ogni election timeout cattura una generation; callback
  cancellate/dequeued non mutano il term dopo reset o stop. Fake clock deterministico.
- **Stato:** VERIFIED

## CODE-16 - Rendere transazionale il lifecycle dei processi

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `Broker`, `RaftOrderingService`, RPC/UDP/discovery, Directory
- **Problema:** startup e stop attraversano piu' listener/thread senza una singola
  transizione; una failure intermedia puo' lasciare risorse o registrazioni attive e
  callback concorrenti possono arrivare durante/oltre lo stop.
- **Impatto:** porte e storage ancora in uso, restart locale fragile e mutazioni da una
  vecchia istanza mentre parte quella nuova.
- **Azione:** stati lifecycle espliciti, startup con rollback inverso, readiness dopo
  tutti i bind e stop idempotente che chiude listener/executor e attende i thread.
- **Verifica:** iniettare failure a ogni fase e poi riavviare sulle stesse porte/storage;
  nessun listener, thread o callback della vecchia istanza resta attivo.
- **Esito 2026-08-12:** ordering, broker e Directory eseguono rollback inverso e stop
  idempotente/bounded; manager fermati rifiutano RPC e append senza mutazioni. Una
  lifecycle generation e riferimenti manager per-run impediscono inoltre a callback
  uscite dal vecchio run di toccare quello riavviato. Testano bind parziali, riuso
  porta, restart ordering, sessioni pre-JOIN e writer bloccati.
- **Stato:** VERIFIED

## CODE-17 - Rendere coerente il bookkeeping della vector clock

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** `Broker.handleOrderedMessage`, `HoldBackQueue`
- **Problema:** il broker aggiorna il clock dopo l'I/O client, usa il messaggio appena
  arrivato anche quando non e' ancora ready e non itera esplicitamente tutti i
  messaggi rilasciati dalla queue. Il clock e' inoltre broker-wide.
- **Impatto:** metadati causali incompleti o dipendenze spurie. Non e' una violazione
  P0 autonoma nel path stabile, perche' il prefisso Raft committed ordina gia' una
  risposta; CODE-05 resta il controesempio causale ad alta priorita'.
- **Azione:** definire l'invariante della clock layer e aggiornarla per ogni delivery
  realmente resa osservabile, prima della visibilita', oppure semplificare e provare
  formalmente che il solo total-order service estende happens-before.
- **Verifica:** unit test con piu' messaggi buffered/ready e TEST-07 sul path reale;
  nessun claim causale deve dipendere da metadata non aggiornati.
- **Esito 2026-08-12:** il clock viene unito per ogni messaggio effettivamente
  rilasciato, prima della visibilita'; un incoming ancora held-back non influenza nuove
  proposte. Unit/component test coprono prefissi buffered/ready.
- **Stato:** VERIFIED

---

# 2. Missing or insufficient automated tests

## TEST-01 - Fare scoprire tutti i test a Maven

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** `pom.xml`, tre classi `VectorClock*Test`
- **Problema:** 192 test dichiarati, ma Maven ne esegue 186; sei test JUnit 4 sono
  esclusi per assenza Vintage. JUnit 4 e' inoltre in scope `compile`.
- **Impatto:** build verde con copertura causale inferiore a quella dichiarata.
- **Azione:** migrare le tre classi a Jupiter oppure aggiungere Vintage; portare la
  dipendenza JUnit 4 a scope `test` se resta necessaria.
- **Verifica:** `mvn test` riporta 192/0/0/0 e nessuna suite diretta separata.
- **Esito 2026-08-12:** le tre classi sono migrate a Jupiter e JUnit 4 e' rimosso dal
  compile scope. Tutti i test fanno parte dei 274 eseguiti dalla suite finale.
- **Stato:** VERIFIED

## TEST-02 - Aggiungere un vero E2E applicativo

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** nuovi test process-level/Failsafe
- **Problema:** il test chiamato end-to-end si ferma alla callback di
  `RaftOrderingService`, bypassando `Broker`, hold-back e client.
- **Impatto:** CODE-01 resta invisibile ai 186 test verdi.
- **Azione:** avviare Directory, almeno tre broker e client socket/processi reali;
  verificare prima chat e chat dopo rielezione.
- **Verifica:** attraversamento `Client -> Broker -> Raft -> apply -> HBQ -> Client`,
  con asserzioni su payload, sequenza, ACK e assenza duplicati.
- **Esito 2026-08-12:** `ReplicatedChatApplicationIntegrationTest` attraversa il
  percorso completo con Directory, 3 broker, socket client, follower forwarding,
  no-history, ACK/no-echo e leader failover. E' in-process su loopback; l'isolamento
  in processi OS e' tracciato separatamente in MAN-01.
- **Stato:** VERIFIED

## TEST-03 - Coprire le race Raft di leadership e higher-term

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** test election/replication/core
- **Problema:** mancano interleaving fra role-check e append/send e response
  higher-term ricevute dopo un cambio ruolo.
- **Impatto:** CODE-02 e CODE-03 non hanno regressioni deterministiche.
- **Azione:** fake clock e latch, senza `sleep`, per forzare gli interleaving descritti.
- **Verifica:** nessuna entry old-leader; term aggiornato e timeout follower armato.
- **Esito 2026-08-12:** latch/fake clock e test cross-manager coprono append/send,
  response tardive e vero higher-term `handleAppendEntries` con pending future.
- **Stato:** VERIFIED

## TEST-04 - Coprire dedup, ACK perso e FIFO attraverso failover

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** ordering service + client/process integration
- **Problema:** i retry correnti coprono il leader stabile dopo commit.
- **Impatto:** duplicazione, future orfana e inversione `clientSeq` non rilevate.
- **Azione:** failure fra replicate/commit/ACK, retry sul nuovo leader prima della
  no-op, truncation e rielezione, due clientSeq pending.
- **Verifica:** una sola application per key, nessun ACK prematuro, ordine `1,2`.
- **Esito 2026-08-12:** test Raft coprono dedup, truncation, ACK perso/leader change;
  test client component/socket coprono retry head e FIFO `1,1,2`. L'E2E applicativo
  verifica inoltre sequenze contigue prima/dopo leader failover.
- **Stato:** VERIFIED

## TEST-05 - Coprire no-history e notifiche di sessione

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** Broker/HBQ/client integration
- **Problema:** nessun test connette un client durante il catch-up di un follower;
  nessun test confronta gli stream JOIN/LEAVE fra broker.
- **Impatto:** CODE-07 e CODE-08 non sono verificati.
- **Azione:** controllare commit/apply con latch, poi JOIN; confrontare stream completi
  di client su broker differenti.
- **Verifica:** nessun messaggio pre-JOIN; stream soggetto a ordering identico.
- **Esito 2026-08-12:** test con callback/latch forza old delivery durante il fence e
  verifica WELCOME -> first new MSG; l'E2E committa old prima del JOIN e non lo espone.
  JOIN/QUIT non generano `MSG`.
- **Stato:** VERIFIED

## TEST-06 - Coprire stream writer, reconnect e Directory lifecycle

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** client, Directory, Broker startup
- **Problema:** nessun test usa `ClientMain`, heartbeat/retry reali o failure della
  Directory.
- **Impatto:** CODE-06, CODE-09, CODE-10 e CODE-13 restano senza prova.
- **Azione:** stress concorrente e test process-level con endpoint stale, porta
  occupata, broker alternativo e restart Directory.
- **Verifica:** stream sempre leggibile, reconnect eventuale, nessun endpoint falso.
- **Esito 2026-08-12:** writer concorrente e write bloccata, bootstrap/reconnect con
  Directory reale e preferred broker irraggiungibile, generation replacement, bind
  failure, Directory restart/re-register e record same-id sono coperti da test
  deterministici e socket integration.
- **Stato:** VERIFIED

## TEST-07 - Provare causalita' e concorrenza sul percorso reale

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** client/broker E2E
- **Problema:** i test causali iniettano sequenze gia' decise; uno usa un gap di
  sequenza che non isola la causalita'.
- **Impatto:** nessuna prova che l'ordine totale estenda happens-before sotto failure.
- **Azione:** A invia `m1`; B su altro broker riceve e risponde subito `m2`; aggiungere
  messaggi realmente concorrenti da C/D e un failover controllato.
- **Verifica:** almeno due client osservatori vedono `m1 < m2` e lo stesso ordine dei
  concorrenti; i sender ricevono ACK e la proiezione attesa senza il proprio echo;
  nessuna perdita/duplicazione.
- **Esito 2026-08-12:** test applicativo con Directory, tre broker e socket reali:
  A invia `m1`, B su broker differente la riceve e poi invia `m2`; due osservatori
  vedono `m1 < m2`. A/B inviano poi contemporaneamente con latch; gli osservatori
  vedono lo stesso ordine totale e i sender ricevono un solo ACK, senza self-echo,
  perdita o duplicazione.
- **Stato:** VERIFIED

## TEST-08 - Ripulire i test deboli/flaky

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** `PeerRegistryTest`, allocazione porte, test RPC
- **Problema:** una callback non e' asserita; alcune porte sono scelte con
  open-close-rebind o assumendo che la porta 1 sia libera.
- **Impatto:** piccola possibilita' di falso verde o flakiness ambientale.
- **Azione:** asserzioni reali e port reservation gestita dal test harness.
- **Verifica:** suite ripetuta su Windows/Linux senza failure intermittenti.
- **Stato:** OPTIONAL

---

# 3. Manual and E2E validation

## MAN-01 - Smoke locale multi-process completo

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P1
- **Area/file:** runbook locale
- **Problema:** oggi non esiste una prova fresca completa con `ClientMain`; lo smoke
  diretto conferma i blocker ma non sostituisce il path Directory-client.
- **Impatto:** rischio di scoprire problemi di startup/protocollo solo in demo.
- **Azione:** dopo CODE-01/02/06, avviare Directory, tre `BrokerMain` e almeno tre
  `ClientMain`; conservare comandi, log e commit hash.
- **Verifica:** JOIN, chat cross-broker, ACK e shutdown senza gap/eccezioni.
- **Esito 2026-08-12:** PASS su Windows con JVM reali: Directory, tre broker
  `LOCAL_TCP` e tre `ClientMain` su tre endpoint distinti. Quattro messaggi sono stati
  osservati nello stesso ordine/esattamente una volta, senza self-echo; kill del
  leader, rielezione e reconnect del client sul leader terminato sono riusciti.
  Processi e directory temporanee sono stati chiusi e rimossi.
- **Stato:** VERIFIED

## MAN-02 - Failure di leader, follower e link senza partition

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** scenario multi-process
- **Problema:** nessuna prova completa mantiene client attivi mentre leader/follower
  falliscono; network partition e Byzantine restano correttamente fuori scope.
- **Impatto:** tolleranza ai failure dichiarata ma non dimostrata.
- **Azione:** kill leader con maggioranza viva, kill follower, perdita/ritardo
  temporaneo di un link senza dividere stabilmente il cluster.
- **Verifica:** nuova election, nessun commit in minoranza accidentale, chat successiva
  consegnata nello stesso ordine ai destinatari comuni e nessuna duplicate delivery.
- **Esito parziale 2026-08-12:** lo smoke multi-process ha coperto il kill del leader
  con maggioranza 2/3, nuova election e chat successiva senza duplicati. Restano da
  eseguire esplicitamente kill del follower e failure/ritardo del singolo link.
- **Stato:** READY_TO_VERIFY

## MAN-03 - Failure e riconnessione client

- **Categoria:** REQUIRED FOR CORRECTNESS
- **Priorita':** P1
- **Area/file:** scenario ClientMain/Directory
- **Problema:** il reconnect one-shot e i writer concorrenti non sono stati provati.
- **Impatto:** client bloccato o messaggi riordinati durante il failure piu' visibile.
- **Azione:** lasciare pending due messaggi, uccidere il broker connesso, attendere
  riassegnazione e continuare la chat.
- **Verifica:** eventuale riconnessione, FIFO, un solo ACK/delivery per key.
- **Esito parziale 2026-08-12:** un vero `ClientMain` collegato al leader ucciso si e'
  riconnesso e ha continuato a inviare; i test socket deterministici coprono pending
  FIFO e dedup. Resta utile ripetere manualmente il kill con due input certamente
  pending nel preciso istante del failure.
- **Stato:** READY_TO_VERIFY

## MAN-04 - Ordine totale, causalita' e concorrenza cross-broker

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** scenario con due sender e almeno due client osservatori
- **Problema:** manca evidenza client-visible, non solo ordine nel log.
- **Impatto:** i requisiti centrali P3/P4 non sono dimostrati.
- **Azione:** catena A:`m1` -> B riceve -> B:`m2`; poi A/B inviano messaggi concorrenti
  da broker diversi mentre C/D restano osservatori.
- **Verifica:** C e D producono lo stesso transcript e vedono `m1 < m2`; A/B ricevono
  ACK e le proiezioni corrette che escludono il proprio messaggio.
- **Esito parziale 2026-08-12:** lo scenario esatto e' verde nel test applicativo
  same-host con socket reali e tre broker. Resta da ripeterlo nel run manuale con JVM
  separate e, infine, sui due notebook della demo.
- **Stato:** READY_TO_VERIFY

## MAN-05 - Connected-only/no-history

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** scenario join/catch-up/reconnect
- **Problema:** catch-up tardivo puo' esporre messaggi antecedenti al JOIN.
- **Impatto:** violazione esplicita della semantica connected-only.
- **Azione:** committare `old`, collegare un client a follower arretrato prima del suo
  apply, completare catch-up, poi inviare `new`.
- **Verifica:** il client non vede `old` e vede `new`; nessun replay dopo reconnect.
- **Esito parziale 2026-08-12:** unit/component ed E2E in-process verificano il fence,
  `old` escluso e `new` consegnato. Resta la ripetizione manuale con follower
  deliberatamente arretrato e processi separati.
- **Stato:** READY_TO_VERIFY

## MAN-06 - Runbook riproducibile e raccolta evidenze

- **Categoria:** RECOMMENDED FOR DEMO/ROBUSTNESS
- **Priorita':** P2
- **Area/file:** documentazione/packaging
- **Problema:** jar senza `Main-Class`, nessun wrapper/launcher/README root; il runbook
  dipende da `target/classes` e prerequisiti impliciti.
- **Impatto:** setup fragile per gruppo e valutatore.
- **Azione:** documentare JDK/Maven, build, tre entry point, porte, storage temporaneo,
  cleanup e formato dei log; aggiungere launcher solo se il gruppo lo ritiene utile.
- **Verifica:** un compagno parte da clone pulito e completa MAN-01 senza conoscenza
  implicita.
- **Stato:** OPEN

---

# 4. Two-notebook demo preparation

## LAN-01 - Eseguire la demo su almeno due notebook fisici

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P0
- **Area/file:** ambiente di presentazione
- **Problema:** tutte le prove correnti sono loopback/same-host.
- **Impatto:** requisito ufficiale di demo non soddisfatto anche dopo i fix software.
- **Azione:** notebook A e B sulla stessa LAN cablata/Wi-Fi, processi distribuiti e
  client connessi a broker fisicamente diversi.
- **Verifica:** transcript identico su almeno due destinatari comuni, causal chain,
  failure leader e chat successiva; commit hash e topologia annotati.
- **Stato:** OPEN

## LAN-02 - Validare rete, configurazione e broadcast reali

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** firewall/AP/porte/config
- **Problema:** UDP e IP pubblicizzati sono stati provati solo localmente; CODE-10
  impedisce oggi il runbook single-Directory corretto.
- **Impatto:** broadcast filtrato da firewall/AP isolation o endpoint errati possono
  impedire election/heartbeat durante la demo.
- **Azione:** dopo CODE-10, scrivere comandi esatti senza argomenti inventati; stesso
  voter CSV, `clusterId` e porta UDP comune. Aprire TCP Directory `60000/60001`, RPC
  e client port; aprire UDP Raft. Non dipendere dalla discovery ausiliaria.
- **Verifica:** capture/log prova RequestVote ed heartbeat UDP fra notebook; payload
  AppendEntries e proposal forwarding restano TCP peer-to-peer.
- **Stato:** OPEN

## LAN-03 - Preparare slide e copione verificabili

- **Categoria:** REQUIRED BY SPECIFICATION
- **Priorita':** P1
- **Area/file:** presentazione
- **Problema:** manca un artefatto finale che separi requisiti, design scelto e limiti.
- **Impatto:** rischio di claim non supportati durante la valutazione.
- **Azione:** mostrare componenti, percorso messaggio, Raft statico, tabella trasporti,
  total/causal-order argument, no-history e failure scope. Dichiarare restart,
  partitions, dynamic membership e hardening soltanto secondo lo scope deciso.
- **Verifica:** ogni claim ha test/log/demo associato; nessuna slide chiama broadcast
  “dynamic membership” o integration in-process “true E2E”.
- **Stato:** OPEN

---

# 5. Optional improvements

Questi item non bloccano la consegna minima salvo che il gruppo scelga di promettere
la relativa garanzia. In tal caso vanno promossi a `P1` e spostati nelle sezioni
obbligatorie.

## OPT-01 - Recovery completo del Broker

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P2
- **Area/file:** persistence, `Broker`, HBQ, vector clock
- **Problema:** al restart Raft ripristina `lastApplied`, ma HBQ e causal state
  ripartono da zero e il prefisso non viene riapplicato. Lo storage `raft-data/n{id}`
  non contiene inoltre un fingerprint di cluster/membership che impedisca il riuso
  accidentale con una topologia diversa.
- **Impatto:** se si promette crash-recovery, la prima chat post-restart puo' bloccarsi
  o perdere dipendenze.
- **Azione:** snapshot/replay silenzioso coerente prima della readiness, senza history
  client-visible; legare metadata e storage a cluster id/voter set.
- **Verifica:** process kill/restart dello stesso broker e chat successiva senza replay.
- **Stato:** OPTIONAL

## OPT-02 - Ricostruire dedup dal solo prefisso committed

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P2
- **Area/file:** `RaftOrderingService.start`
- **Problema storico:** tutte le entry persistite, incluse quelle oltre il progresso
  applicato, venivano inserite nel set committed.
- **Impatto storico:** in crash-recovery un retry poteva ricevere `true` prima del
  commit e poi essere perso.
- **Azione implementata:** ricostruire `committedProposalKeys` soltanto fino al
  `restoredLastApplied`; la tail successiva non viene dichiarata applicata.
- **Verifica:** entry persistita non committed, restart e retry: nessun ACK prima del
  quorum.
- **Stato:** VERIFIED

## OPT-03 - Riparare fisicamente la tail WAL parziale

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P2
- **Area/file:** `FileRaftPersistence`
- **Problema:** il loader ignora una tail troncata ma non tronca il file; append validi
  successivi possono diventare illeggibili al secondo restart.
- **Impatto:** recovery non ripetibile dopo power loss/torn write.
- **Azione:** validare record, troncare e fsync all'ultimo offset valido prima di nuovi
  append.
- **Verifica:** tail parziale -> load -> append -> secondo load conserva tutti i record
  validi.
- **Stato:** OPTIONAL

## OPT-04 - Fail-stop su errori di stable storage

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** `RaftNode`, persistence lifecycle
- **Problema:** eccezioni di persistenza possono lasciare stato volatile mutato senza
  mettere fuori servizio il nodo.
- **Impatto:** semantica incerta sotto failure del disco, non richiesta esplicitamente
  dalla consegna.
- **Azione:** arresto fail-stop o rollback formalmente sicuro; test con fake storage.
- **Verifica:** nessun RPC success viene esposto dopo persistence failure.
- **Stato:** OPTIONAL

## OPT-05 - Correggere o rimuovere la discovery ausiliaria

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** `LanDiscoveryService`, `PeerRegistry`
- **Problema:** ogni broker ascolta e trasmette su `50002 + nodeId`; peer con id diversi
  non ricevono gli HELLO. Gli annunci sono solo iniziali.
- **Impatto:** registry ausiliario incompleto; Raft e quorum non dipendono da esso.
- **Azione:** porta comune e annunci periodici oppure rimozione/documentazione esplicita.
- **Verifica:** peer late-joining convergono senza alterare il voter set statico.
- **Stato:** OPTIONAL

## OPT-06 - Bounded resources, snapshot e retention

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** log, cache dedup Raft, executor/socket
- **Problema:** log, dedup key cache, object-stream handle table e alcuni pool crescono
  senza bound; la cache retry applicativa e' invece rimossa dopo commit e conserva
  solo proposal fallite/non confermate. AppendEntries invia l'intero suffisso e il
  commit progress puo' essere fsyncato piu' volte per avanzamento.
- **Impatto:** memoria, storage e catch-up non scalano su esecuzioni lunghe.
- **Azione:** limiti, `ObjectOutputStream.reset`/codec idoneo, batching, snapshot,
  InstallSnapshot e retention coerente con P5; misurare prima di ottimizzare fsync.
- **Verifica:** soak/catch-up lungo con memoria e payload bounded.
- **Stato:** OPTIONAL

## OPT-07 - Hardening di protocollo e deserializzazione

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** endpoint Java serialization
- **Problema:** assenza di authentication, object filters, size/rate limits e timeout
  completi; parsing ambiguo e runtime exception possono terminare singoli receiver.
- **Impatto:** DoS/impersonation in reti ostili; Byzantine e security non sono scope
  minimo del progetto.
- **Azione:** filtri, limiti e codec esplicito solo dopo i blocker distribuiti.
- **Verifica:** input ostili vengono rifiutati senza fermare i listener.
- **Stato:** OPTIONAL

## OPT-08 - Build/CI multipiattaforma

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** `pom.xml`, wrapper, CI
- **Problema:** `source/target=16` senza `--release`, nessun wrapper/CI e jar senza
  launcher unico.
- **Impatto:** riproducibilita' inferiore, non correttezza distribuita.
- **Azione:** `maven.compiler.release`, wrapper/toolchain, CI Windows/Linux e launcher
  documentati.
- **Verifica:** clean build e smoke identici sugli ambienti dichiarati.
- **Stato:** OPTIONAL

## OPT-09 - Igiene repository, API e osservabilita'

- **Categoria:** OPTIONAL / PRODUCTION HARDENING
- **Priorita':** P3
- **Area/file:** metadata IDE, API Raft non production, logging
- **Problema:** alcuni file `.idea` sono gia' tracciati nonostante `.gitignore`;
  `RaftNode.recordVoteFor` e altre API/helper non integrati possono essere usati senza
  le garanzie del path production; i log sono `System.out/err` non correlati.
- **Impatto:** repository meno pulito, superfici d'uso fuorvianti e diagnosi demo piu'
  difficile, senza impatto diretto sulla safety se restano inutilizzati.
- **Azione:** rimuovere metadata tracciati con decisione del gruppo, restringere o
  documentare API test-only e introdurre log essenziali con node/term/request key.
- **Verifica:** clone pulito senza metadata locali; nessun call site production usa
  API non persistenti; log di MAN/LAN ricostruibili.
- **Stato:** OPTIONAL

## Ordine residuo raccomandato

1. Ottenere la decisione docente per `CODE-11` e registrarla senza reinterpretazioni.
2. Completare gli scenari manuali residui `MAN-02..05` sul build candidato
   (`TEST-07` e `MAN-01` sono gia' verificati automaticamente/multi-process locale).
3. Eseguire `LAN-01/LAN-02` su due notebook e conservare topologia, comandi e log.
4. Preparare `MAN-06/LAN-03` usando soltanto claim dimostrati.
5. Affrontare gli `OPT-*` ancora `OPTIONAL` solo se il gruppo decide di promettere le
   relative garanzie production/crash-recovery.
