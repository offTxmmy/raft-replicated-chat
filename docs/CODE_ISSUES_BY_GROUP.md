# Allegato tecnico ai finding di codice

Failure analysis originale: **2026-08-11**, commit
`c77dbb981a3db837c50ef17bf9bd279278c04998`. Verifica aggiornata:
**2026-08-13**. Baseline precedente agli ultimi fix mirati:
`b5bf3857ea85be8f8d1758261cc428b0c5fba09b` (`master`).

Questo documento conserva evidenze, execution trace e ragionamento tecnico. Non e'
un secondo backlog: priorita', stato, azione e verifica ufficiali sono esclusivamente
nel tracker canonico [`PRE_GROUP_MANUAL_TESTING_TODO.md`](PRE_GROUP_MANUAL_TESTING_TODO.md).

> **Stato corrente:** le failure trace sotto restano utili come root cause storiche,
> ma `CODE-01..10`, `CODE-12..17` e i finding finali `F-01..03` sono stati corretti
> e verificati. `CODE-11` resta `BLOCKED_BY_DECISION`: il log tecnico Raft conserva
> payload per replica/recovery, mentre la cache retry applicativa elimina le proposal
> anche dopo late commit e non esistono history API o replay client-visible.

## 1. Percorso production ricostruito

Il percorso di una chat e':

`ClientMessageSender` -> TCP client/broker -> `ClientHandler` ->
`Broker.buildChatReq` -> `RaftOrderingService.propose` -> eventuale forward TCP al
leader -> append Raft -> AppendEntries -> commit -> `RaftStateMachineAdapter` ->
`Broker.handleOrderedMessage` -> `HoldBackQueue` -> TCP broker/client.

Le proprieta' applicative dipendono quindi dalla composizione di tre ordini:

1. program order e retry del client;
2. ordine totale del log Raft;
3. sequenza/causal readiness della hold-back queue.

I test in-process dell'ordering service si fermano prima del punto 3. Questa e' la
ragione strutturale per cui il core Raft puo' essere verde mentre la chat production
non consegna.

## 2. Finding P0 confermati

### CODE-01 - No-op Raft e sequenza applicativa incompatibili

**Evidenza.** `RaftOrderingService.java:150-156` appende una no-op a ogni nuova
leadership. `RaftStateMachineAdapter.java:27-39` non emette una delivery per la no-op,
ma assegna alle chat `entry.getIndex()`. `HoldBackQueue.java:34-48,88-110` parte da
`expectedSeq=1` e rilascia soltanto l'indice esatto. L'hook
`Broker.onBrokerIdAssigned` (`Broker.java:130-135`) non e' invocato dal production
path e, da solo, non risolverebbe tutti i buchi futuri.

**Execution trace.** Leader term 1: no-op `(index=1)`; prima chat `(index=2)`. Il
commit manager applica entrambi, l'adapter scarta il primo e crea `ChatDeliver(seq=2)`.
La queue attende 1 per sempre. Una nuova leadership introduce un altro indice interno
non rappresentato nella sequenza client-visible.

**Proprieta' violata.** Liveness della delivery e requisiti di ordine totale della
chat. Il no-op e' corretto a livello Raft e non deve essere semplicemente rimosso.

**Evidenza dinamica fresca.** Tre broker process-level hanno eletto un leader e tutti
e tre hanno stampato `Gap detected! Received seq = 2, expected = 1` dopo una chat
inviata tramite due socket client reali.

### CODE-02 - Append/send non atomici rispetto allo step-down

**Evidenza.** `RaftReplicationManager.appendCommandAsLeader`
(`RaftReplicationManager.java:86-92`) controlla il ruolo e legge il term in due
chiamate distinte a `RaftNode`. Il manager election ha un lock diverso e puo' chiamare
`RaftNode.handleRequestVote` (`RaftElectionManager.java:281-314`,
`RaftNode.java:223-253`) tra i due accessi. `RaftLog.appendEntries`
(`RaftLog.java:211-225`) considera uguali entry esistenti quando coincide il term,
come e' legittimo soltanto se l'invariante di leadership e' gia' garantita.

**Execution trace.** L e' leader nel term T e supera il role-check. Una RequestVote
T+1 lo rende follower. L riprende, legge T+1 e appende `x`. Il vero leader T+1 crea
`y` allo stesso index. Quando `y` arriva a L, il confronto del solo term conserva
`x`; un successivo `leaderCommit` puo' far applicare `x` su L e `y` sugli altri nodi.

La stessa radice consente a un heartbeat preparato dalla vecchia leadership di
leggere il nuovo term prima dell'invio. Non basta sincronizzare singolarmente Node,
Election e Replication: serve una leadership epoch o una serializzazione comune.

**Proprieta' violata.** Potenziale State Machine Safety violation, quindi P0
**REQUIRED FOR CORRECTNESS**.

## 3. Finding ad alta priorita' e issue client-visible

### CODE-03 - Higher-term response e lifecycle di election divergono

`RaftElectionManager.java:411-422` filtra prima per ruolo candidate; una response di
term superiore arrivata dopo la promozione puo' essere ignorata. Analogamente,
`RaftReplicationManager.java:160-171` filtra prima per ruolo leader. Quando la
replication gestisce una response higher-term, modifica direttamente `RaftNode` e
pulisce il proprio stato, ma non informa il manager election. Il timeout era stato
cancellato all'elezione (`RaftElectionManager.java:534-539`), quindi il follower puo'
restare senza election timeout finche' non osserva nuova leader activity.

La regola Raft corretta e': incorporare ogni evidenza di term superiore prima dei
filtri su ruolo, request generation o staleness; poi eseguire una transizione follower
completa e unica.

### CODE-04 - Dedup e future pending non sopravvivono correttamente al leader change

La key entra in `committedProposalKeys` soltanto durante apply
(`RaftOrderingService.java:406-419`). Un nuovo leader che possiede K nel log ma non
l'ha ancora applicata puo' ricevere il retry prima che la no-op committi il prefisso e
appendere una seconda K (`RaftOrderingService.java:269-316`). Entrambe possono essere
applicate quando una entry del nuovo term raggiunge il quorum.

In un secondo trace, una proposal va in timeout
(`RaftOrderingService.java:318-325`) e viene poi troncata. Il truncation hook
(`RaftOrderingService.java:117-123`) non completa/rimuove la future in
`pendingCommits`. Se il nodo torna leader, il retry trova la future orfana, non
riappende e scade indefinitamente.

### CODE-05 - Program order del client non garantito durante failure

`ClientMessageSender` conserva piu' pending in una `ConcurrentHashMap` e il retry loop
itera senza una barriera FIFO (`ClientMessageSender.java:66-84,137-161`). Dopo che
`ClientHandler` ha ricevuto un esito negativo per `m1`, puo' leggere `m2`; il nuovo
leader non applica alcun vincolo `nextClientSeq`.

Trace: forward di `m1(seq=1)` verso il vecchio leader fallisce; il nuovo leader e'
conosciuto quando arriva `m2(seq=2)` e lo committa; il retry di m1 arriva dopo. Il log
ordina `m2,m1`. Questo e' il finding causale concreto: il program order dello stesso
client e' una relazione happens-before.

### CODE-06 - ObjectOutputStream client con writer concorrenti

`ClientMain` scrive JOIN/QUIT e JOIN di reconnect
(`ClientMain.java:69-72,93-94,159-164`), `ClientMessageSender` scrive da input e retry
(`ClientMessageSender.java:73-84,143-159`) e `ClientHeartbeatManager` scrive dallo
heartbeat thread. Non condividono un lock. `ObjectOutputStream` mantiene stato di
protocollo e handle table e non e' thread-safe. Il lato broker usa invece
correttamente un `outLock` per sessione in `ClientHandler`.

### CODE-07 - Nessun join watermark per connected-only delivery

Il `ClientHandler` entra nella lista dei destinatari gia' all'accept
(`Broker.java:263-271`). Non registra un commit/application watermark al JOIN. Un
follower puo' ricevere `leaderCommit` e applicare un suffisso in ritardo
(`RaftReplicationManager.java:136-149`); `Broker.java:324-331` lo invia a tutti i
client correntemente presenti.

Trace: `old` e' committed mentre F e' indietro; C si collega a F; F completa catch-up
e applica `old`; C riceve un messaggio precedente alla propria connessione. Questo e'
distinto dal problema di storage persistente: viola certamente la semantica
client-visible no-history.

### CODE-08 - JOIN/LEAVE locali usano lo stesso stream delle chat

**Classificazione:** P2 **RECOMMENDED FOR DEMO/ROBUSTNESS**. Non sono messaggi chat
richiesti dalla specifica; diventano un problema di correttezza soltanto se il gruppo
li presenta come parte dello stream globalmente ordinato.

`Broker.java:68-69,297-303,399-410` assegna una sequenza locale e invia notifiche
system come `MSG`; `ClientHandler.java:93,112` le genera automaticamente. Esse non
passano da Raft e sono limitate al broker locale.

Nello smoke fresco, due client su broker differenti hanno visto rispettivamente
`MSG 1 [system]:bob joined...` e `MSG 1 [system]:alice joined...`. Se il tipo resta
nello stream soggetto al contratto globale, l'ordine/payload client-visible divergono.
Le notifiche non sono richieste dalla specifica e possono essere rimosse o separate.

### CODE-09 - Reconnect one-shot verso una Directory ancora stale

Il failure handler in `ClientMain.java:142-177` esegue una sola `connection.open()`.
La Directory rimuove un broker soltanto nel reaper periodico, dopo oltre 10 secondi
(`DirectoryService.java:322-347`). Se il client rileva il failure prima, puo'
riscegliere il broker morto; dopo il connect fallito non esiste un nuovo heartbeat
manager che riprovi.

### CODE-10 - Directory hard-coded impedisce la topologia single-Directory multi-host

`BrokerMain` usa `localhost:60000` sia nel bootstrap membership sia nel percorso
documentato; `Broker` ripete l'endpoint per registration/heartbeat. `ClientMain`, al
contrario, accetta host/porta Directory. E' possibile avviare Directory separate per
host, ma non sarebbe la topologia unica documentata e ogni istanza conoscerebbe solo
le registrazioni locali. La scelta va resa esplicita; il fix minimo e' configurare un
endpoint unico e propagarlo a tutto il broker lifecycle.

### CODE-11 - Persistenza payload e requisito “brokers do not store messages”

Il fatto verificato e' duplice:

- non esistono history API, offline inbox o replay intenzionale;
- `cachedClientRequests` conserva request/vector clock durante retry falliti o non
  confermati, ma elimina l'entry dopo conferma sincrona o apply di un late commit;
- `FileRaftPersistence` conserva il `ChatCommand`, incluso il testo, nel log tecnico.

La specifica ufficiale non chiarisce nel repository se lo storage interno necessario
a un Raft restart-capable sia ammesso. Non si deve trasformare l'assenza di history in
una concessione implicita: serve una decisione del docente o uno scope crash-stop
coerente. La cache applicativa non trattiene piu' indefinitamente le proposal concluse;
la decisione residua riguarda il log tecnico Raft. Il tracker mantiene CODE-11 come
decisione P1, non come P0 automatico.

## 4. Finding P2 e limiti di robustezza

- **CODE-12:** delivery sincrona sotto lock verso client lenti puo' bloccare apply e
  replication (`Broker.java:324-331`, `RaftCommitManager.java:161-170`); una callback
  fallita viene loggata mentre `lastApplied` avanza, quindi la fan-out va isolata dalla
  state machine deterministica.
- **CODE-13:** registrazione precedente al bind client e assenza di re-registration
  dopo failure della Directory producono endpoint stale/non pronti; reaper e
  sostituzione endpoint hanno inoltre race fra mappe separate.
- **CODE-14:** success/failure AppendEntries fuori ordine possono far regredire
  `nextIndex`; `matchIndex` resta monotono, quindi e' un problema di liveness/traffico,
  non una safety violation confermata.
- **CODE-15:** i timeout election cancellati non hanno una generation; un callback gia'
  partito puo' provocare un'elezione spuria dopo un heartbeat valido.
- **CODE-16:** startup/stop non sono transazionali attraverso listener, executor,
  registration e storage; una failure intermedia puo' lasciare risorse o callback
  della vecchia istanza.
- **CODE-17:** il vector clock viene aggiornato dopo I/O, anche per un incoming non
  ancora ready, e non esplicitamente per ogni elemento rilasciato; e' una incoerenza
  P2 della metadata layer, non il vecchio P0 causale.
- Più broker sullo stesso host condividono la porta UDP Raft con reuse; la consegna
  unicast al socket corretto e' OS-dependent e va pre-validata per lo smoke locale.

## 5. Crash-recovery: implementato ma non assunto come requisito minimo

La consegna ufficiale ammette broker failure, ma non impone automaticamente il
restart della stessa identita'. Per questo i seguenti finding restano opzionali finche'
il gruppo non promette crash-recovery:

- `RaftOrderingService.start` inserisce nel set committed anche entry oltre il
  `commitIndex`, consentendo un ACK falso dopo restart;
- il commit progress riparte avanzato, mentre HBQ e vector clock del nuovo `Broker`
  ripartono da zero e il prefisso non viene rieseguito;
- `FileRaftPersistence` ignora una tail parziale senza troncarla fisicamente, quindi
  append validi successivi possono non essere raggiunti al secondo restart;
- una persistence exception non porta necessariamente l'intero nodo in fail-stop.

Se il gruppo dimostra o dichiara restart, `OPT-01..04` del tracker vanno promossi a P1.
In caso contrario il runbook deve usare crash-stop: un broker terminato non rientra con
la stessa identita' nella stessa esecuzione.

## 6. Aree verificate come corrette nel path nominale

- voter set statico immutabile e quorum derivato soltanto dalla membership;
- term/voto persistiti prima di una response di voto osservabile nel path senza
  storage failure;
- freshness del voto: confronto `lastLogTerm`, poi `lastLogIndex`;
- prefix/log matching, conflict hints e protezione dalla truncation del prefisso
  committed;
- append consentito nominalmente al leader e apply ordinato per indice;
- current-term commit rule e commit single-node;
- follower-to-leader proposal forwarding e ACK nominale dopo commit;
- esclusione dell'origine dalla fan-out, coerente con il testo “messages sent by
  others”; i confronti d'ordine devono usare destinatari comuni;
- transport ibrido: RequestVote e heartbeat comuni su UDP, payload AppendEntries e
  forward peer-specific su TCP;
- filtro UDP per cluster, voter, self, target e duplicate message id;
- identita' retry `(clientId, clientSeq)` corretta sul leader stabile dopo commit;
- membership Raft non viene modificata da discovery o reachability.

Queste conferme non compensano CODE-01/CODE-02 e non sono una prova E2E.

## 7. Ipotesi storiche corrette o scartate

Le raccomandazioni dei documenti precedenti sono state ri-verificate:

- **Scartata:** “`RaftLog.append` persiste prima della memoria senza rollback”. Il
  codice corrente aggiunge in memoria, persiste e rimuove l'entry se la persistenza
  lancia un'eccezione (`RaftLog.java:176-185`).
- **Riclassificata:** update del vector clock dopo I/O e componente sender ignorata non
  costituiscono da soli il vecchio P0. Nel path stabile, una risposta puo' essere
  proposta solo dopo che il predecessore e' gia' committed/applied e il log ne conserva
  l'ordine. Restano un'imprecisione P2 della layer vettoriale; il P1 causale concreto e'
  CODE-05 sotto retry/failover.
- **Riclassificata:** restart, torn write e failure del disco non sono requisiti
  ufficiali automatici; sono `OPT-01..04` salvo claim esplicito.
- **Scartata come finding P0:** non e' stato dimostrato che una persistence exception
  esponga una vote response non durevole; l'eccezione precede il return. Il fail-stop su
  storage error resta hardening utile.
- **Confermata ma ausiliaria:** la discovery usa porte diverse per id e non scopre
  realmente i peer; non influenza voter set, quorum o Raft.
- **Corretta la terminologia test:** `RaftOrderingServiceIntegrationTest` e' una buona
  integrazione component/in-process, non un true E2E.

## 8. Copertura test reale

`mvn clean test` esegue 290 test verdi. La suite include i casi Jupiter prima
esclusi, test deterministici per dispatch stale fra generation, late-commit cleanup e
timeout di connect/handshake/response Broker->Directory, oltre al percorso applicativo
con Directory, tre broker, Raft, HBQ e socket client reali. Restano manual-only la
distribuzione multi-process fra due notebook, firewall/AP e broadcast HYBRID fisico.
