# Code Issues by Group

Aggiornato al 2026-06-07.

Questo documento raccoglie in una lista unica i problemi emersi dalla review del
codice, divisi secondo le aree di lavoro del gruppo:

- Election Raft;
- Replication / AppendEntries / Log;
- collegamenti Broker-Client-Directory;
- causal delivery / livello applicativo.

Le priorita sono:

- P0: da sistemare prima della demo, rischio alto per correttezza o stabilita;
- P1: importante, da sistemare o dichiarare chiaramente come limite;
- P2: rischio secondario, miglioramento o pulizia tecnica.

---

## 1. Election Raft

### P0

- [FATTO] Deadlock AB-BA tra `RaftElectionManager` e `RaftReplicationManager`.
  - `RaftElectionManager` puo tenere il proprio lock e chiamare il listener di
    replication.
  - `RaftReplicationManager` puo tenere il proprio lock e chiamare
    `onValidLeaderActivityObserved` sull'election manager.
  - Effetto: il cluster puo bloccarsi.
  - Fix suggerito: non chiamare callback/listener mentre si tiene un lock
    `synchronized`, oppure dispatchare la leader activity fuori dal lock.
  - Risolto in `f5c1871`: i callback election/replication vengono chiamati fuori
    dai lock dei manager, con test anti-regressione dedicati.

- [FATTO] Gestione della leader activity troppo intrecciata con replication.
  - E lo stesso rischio architetturale del punto precedente.
  - Va separato il path di gestione `AppendEntries` dal path di reset election
    timeout / osservazione leader.
  - Risolto in `f5c1871`: `handleAppendEntries` completa la gestione replication
    sotto lock e notifica la leader activity solo dopo il rilascio del lock.

### P1

- [FATTO] Election timeout non resettato su `AppendEntries` respinto.
  - Se il follower rifiuta per mismatch di log, oggi non sempre viene considerata
    leader activity valida.
  - Effetto: elezioni spurie durante il catch-up.
  - Non e un problema primario di safety, ma puo peggiorare molto la liveness.
  - Risolto: `handleAppendEntries` notifica la leader activity anche quando
    respinge la RPC per mismatch di log, mantenendo invariata la response di
    conflitto.

- [FATTO] `PeerRegistry.getQuorumSize()` e API ingannevole.
  - La membership Raft e statica e viene da `RaftConfig.getVoters()`.
  - Un quorum calcolato su discovery LAN non deve essere usato per Raft.
  - Risolto: rimosso il metodo da `PeerRegistry`; il quorum Raft resta esposto
    solo da `RaftConfig`.

### P2

- Mancato fsync della parent directory dopo rename atomico in persistence.
  - Tecnicamente rilevante per crash-safety molto rigorosa.
  - Per la demo non e una priorita rispetto ai bug di liveness/deduplica.

- Commenti/API rimasti con riferimenti storici.
  - Esempi: riferimenti a persone del gruppo, "for now", note di integrazione
    vecchie.
  - Non rompe il codice, ma indebolisce la qualita del progetto presentato.

---

## 2. Replication / AppendEntries / Log

### P0

- [FATTO] Follower sovrastima `matchIndex`.
  - In `handleAppendEntries`, il follower risponde con `log.lastLogIndex()`.
  - Su heartbeat o append vuoto con `prevLogIndex` vecchio, il leader puo credere
    che il follower abbia replicato piu entry di quelle effettivamente confermate
    da quella RPC.
  - Effetto: `nextIndex` puo avanzare oltre il log del leader e rompere il path di
    heartbeat/replication.
  - Risolto: il follower risponde con l'indice effettivamente confermato dalla
    RPC, cioe `prevLogIndex + entries.size()`.

- [FATTO] Deduplica Raft non persistente/rebuildata.
  - `committedProposalKeys` vive solo in memoria e viene svuotata allo stop.
  - Dopo crash/restart, un retry gia committato puo essere riappeso come nuova
    entry.
  - Risolto: all'avvio `RaftOrderingService` ricostruisce le chiavi di deduplica
    dalle entry del log persistito.

### P1

- [FATTO] No-op all'elezione mancante.
  - Owner suggerito: Replication / AppendEntries / Log.
  - Il leader rifiuta correttamente di committare entry di term passati basandosi
    solo su majority match.
  - Senza no-op all'inizio del nuovo term, entry gia replicate di term vecchi
    possono restare non committate finche non arriva nuovo traffico.
  - Risolto: quando il nodo locale diventa leader, viene appesa una no-op entry
    nel nuovo term.

- [FATTO] `commitIndex` / `lastApplied` persistiti e ripristinati esplicitamente.
  - Al restart il servizio ricarica il log e ripristina anche il progresso di
    commit da `commit.bin`.
  - `RaftCommitManager` riparte dallo stato persistito e completa solo gli entry
    ancora non applicati.
  - Il progresso viene aggiornato in modo incrementale durante l'applicazione
    delle entry committate, cosi il restart non ricomincia piu da zero.

- [FATTO] `RaftLog.append` non atomico tra persistenza e `entries.add`.
  - L'entry viene scritta su disco prima di essere aggiunta alla lista in memoria.
  - Un crash in mezzo non e necessariamente safety-breaking, ma puo creare
    divergenza temporanea tra memoria e disco.

- [FATTO] Truncate non verifica esplicitamente di non troncare entry committate.
  - In Raft non si dovrebbero troncare entry gia committate.
  - Il controllo oggi non e espresso nel log layer.

- [FATTO] Cluster Raft a un solo nodo non committa.
  - L'avanzamento del commit avviene oggi tramite response dei follower.
  - Non blocca la demo a tre broker, ma e un buco del caso base Raft.

### P2

- [FATTO] `serialVersionUID` mancante su classi serializzate nel log.
  - Rischio: vecchi file Raft persistiti possono non essere leggibili dopo piccole
    modifiche alle classi.
  - Rilevante se si riusa stato persistito tra versioni diverse del codice.

- Gap nella hold-back queue senza recovery attiva.
  - Se arriva `seq > expectedSeq`, la queue aspetta.
  - Con Raft corretto non dovrebbe succedere; quindi e piu un sintomo di bug a
    monte che un problema primario.

---

## 3. Collegamenti Broker-Client-Directory

### P0

- [FATTO] Deduplica client fragile.
  - Il client usa un contatore locale che riparte da `1` a ogni avvio.
  - Broker e Raft deduplicano usando `(username, clientTimestamp)`.
  - Effetto: dopo reconnect/restart client, un messaggio nuovo puo collidere con
    uno vecchio e venire perso o sostituito.
  - Risolto: il client genera un `clientId` stabile per processo e usa un
    `clientSeq` monotono per i messaggi. Broker e Raft deduplicano su
    `(clientId, clientSeq)`.
  - Anche gli ACK usano `(clientId, clientSeq)` e vengono inviati dal
    `ClientHandler` sulla stessa connessione che ha inviato il messaggio.

- [FATTO] Username usato come identita tecnica.
  - ACK e deduplica non usano piu il nome utente: usano `(clientId, clientSeq)`.
  - Il client invia il `clientId` anche nel JOIN e il `ClientHandler` lo associa
    alla connessione.
  - `ChatDeliverMessage` trasporta il `clientId` del mittente.
  - `Broker.onChatDeliver` esclude solo la connessione con lo stesso `clientId`,
    quindi due client con lo stesso username restano distinguibili.

- Scritture concorrenti sullo stesso `ObjectOutputStream` lato client.
  - Sender, retry, heartbeat, JOIN e QUIT possono scrivere sullo stesso stream
    senza un lock comune.
  - `ObjectOutputStream` non e thread-safe.
  - Effetto: possibile corruzione del protocollo in demo reale.
  - Fix suggerito: writer unico o lock condiviso per tutte le scritture verso il
    broker.

### P1

- Directory puo restituire broker morti per alcuni secondi.
  - Quando la connessione broker muore, la Directory aspetta il timeout del
    reaper prima di rimuoverlo.
  - Durante quella finestra un client puo riconnettersi a un broker gia morto.
  - Fix suggerito: rimuovere subito il broker su morte della socket, oppure far
    ritentare al client broker alternativi.

- Phantom entries in `registeredBrokers`.
  - `registeredBrokers` e indicizzato da `BrokerConfig`, mentre `brokersById` e
    indicizzato da id.
  - Restart con stesso id ma porta diversa puo lasciare il vecchio broker come
    entry selezionabile.
  - Fix suggerito: rendere la Directory indicizzata primariamente da broker id e
    sostituire sempre la configurazione precedente.

- Slow client head-of-line nel broadcast locale.
  - `Broker.onChatDeliver` tiene il lock su `clients` mentre fa I/O verso i
    client.
  - Un client lento puo bloccare la consegna agli altri client dello stesso
    broker.
  - Fix suggerito: copiare la lista dei client sotto lock e fare I/O fuori dal
    lock, oppure usare code per-client.

- Forward proposal e ACK perso.
  - Se la response TCP dal leader al follower si perde dopo commit, il retry puo
    coprire il caso.
  - La deduplica Raft viene ricostruita dal log al restart.
  - La chiave applicativa ora usa `(clientId, clientSeq)`.
  - Il problema residuo e' solo la crescita non limitata delle cache di deduplica
    in processi long-running.

- Reconnect ricorsivo nel client.
  - La riconnessione richiama `startReceiverAndHeartbeat` in modo ricorsivo.
  - Non rompe subito, ma puo trattenere thread/closure e rendere piu difficile
    ragionare sui failure.

### P2

- Directory e single point of failure per reconnect.
  - Se la Directory cade, i broker possono essere vivi ma i client non hanno modo
    di scegliere un nuovo broker.
  - Accettabile se dichiarato come limite.

- Cache di deduplica applicativa non limitate.
  - `cachedClientRequests` e le chiavi di deduplica Raft possono crescere nel
    tempo se il sistema resta attivo a lungo.
  - Non rompe la demo, ma andrebbe aggiunta una politica di purge/TTL per un
    servizio long-running.

- Heartbeat client migliorabile.
  - Il meccanismo puo essere reso piu pulito separando heartbeat inviati,
    heartbeat scaduti e ACK tardivi.
  - Non e una priorita critica.

- `onClientMessage` blocca il thread client fino al timeout di commit.
  - Problema di throughput e reattivita.
  - Per la demo funziona se il carico e basso.

---

## 4. Causal Delivery / Applicativo

### P0

- Nessun P0 separato, se vengono sistemate identita client e deduplica.
  - I rischi applicativi piu gravi dipendono soprattutto da identita client,
    retry e deduplica.

### P1

- Causalita intra-broker indebolita.
  - `HoldBackQueue` ignora la componente del broker mittente nel vector clock.
  - Questa scelta evita blocchi quando Raft ordina due proposte dello stesso
    broker diversamente dall'ordine di proposta.
  - Pero, con interpretazione stretta della specifica, puo violare causalita tra
    messaggi dello stesso broker.
  - Fix possibile: serializzare la generazione delle proposte per broker, oppure
    definire una regola chiara per cui l'ordine Raft e autoritativo per eventi
    dello stesso broker e documentare il limite.

- Clock causale aggiornato dopo la delivery ai client.
  - Il broker consegna ai client e poi aggiorna il proprio vector clock.
  - Se un client risponde subito dopo aver ricevuto un messaggio, la nuova
    proposta potrebbe non includere la dipendenza appena consegnata.
  - Fix suggerito: aggiornare il clock per ogni messaggio rilasciato dalla
    hold-back queue prima dell'I/O verso i client.

- [FATTO] Esclusione del mittente basata su username.
  - La scelta applicativa resta: il mittente non riceve l'echo del proprio
    messaggio committato.
  - Il filtro pero non usa piu lo username: usa il `clientId` tecnico del
    mittente, evitando collisioni tra utenti con lo stesso nome visualizzato.

### P2

- Join/leave usano `nextSeq` locale separato da Raft.
  - Le notifiche locali partono da sequenza `1`, separata dal log Raft.
  - Puo confondere l'output lato client.
  - Accettabile se dichiarate come messaggi locali, non globalmente ordinati.

- `syncToSequence` non purga eventuali entry stale nella priority queue.
  - Rischio secondario in scenari di riallineamento.

---

## 5. Top Fix Prima Della Demo

1. [FATTO] Sistemare il deadlock tra election e replication.
2. [FATTO] Correggere il `matchIndex` restituito dal follower in `AppendEntries`.
3. [FATTO] Introdurre `(clientId, clientSeq)` per deduplica e ACK.
4. [FATTO] Usare `clientId` anche per distinguere il mittente nella delivery.
5. Usare un writer unico o un lock comune per `ObjectOutputStream` lato client.
6. [FATTO] Ricostruire la deduplica Raft dal log al restart.
