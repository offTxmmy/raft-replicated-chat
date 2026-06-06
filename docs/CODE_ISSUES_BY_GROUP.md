# Code Issues by Group

Aggiornato al 2026-06-06.

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

- `PeerRegistry.getQuorumSize()` e API ingannevole.
  - La membership Raft e statica e viene da `RaftConfig.getVoters()`.
  - Un quorum calcolato su discovery LAN non deve essere usato per Raft.
  - Fix suggerito: rimuovere il metodo, rinominarlo, o documentarlo come non-Raft.

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
  - Fix suggerito: rispondere con `min(prevLogIndex + entries.size(),
    log.lastLogIndex())` per quella specifica RPC.

- [FATTO] Deduplica Raft non persistente/rebuildata.
  - `committedProposalKeys` vive solo in memoria e viene svuotata allo stop.
  - Dopo crash/restart, un retry gia committato puo essere riappeso come nuova
    entry.
  - Fix suggerito: ricostruire le chiavi deduplicate dal log persistito all'avvio,
    oppure persistere metadati di deduplica.

### P1

- [FATTO] No-op all'elezione mancante.
  - Owner suggerito: Replication / AppendEntries / Log.
  - Il leader rifiuta correttamente di committare entry di term passati basandosi
    solo su majority match.
  - Senza no-op all'inizio del nuovo term, entry gia replicate di term vecchi
    possono restare non committate finche non arriva nuovo traffico.
  - Fix suggerito: quando un nodo diventa leader, appendere una no-op entry nel
    nuovo term.

- `commitIndex` / `lastApplied` non persistiti o non ricostruiti in modo esplicito.
  - Al restart il servizio ricarica il log, ma il commit manager riparte da
    `commitIndex = 0` e `lastApplied = 0`.
  - Rischio: replay indesiderato o difficolta a distinguere history tecnica da
    delivery applicativa.
  - Importante soprattutto se si fa demo con restart del broker.

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

- Deduplica client fragile.
  - Il client usa un contatore locale che riparte da `1` a ogni avvio.
  - Broker e Raft deduplicano usando `(username, clientTimestamp)`.
  - Effetto: dopo reconnect/restart client, un messaggio nuovo puo collidere con
    uno vecchio e venire perso o sostituito.
  - Fix suggerito: usare `(clientId, clientSeq)`; `clientId` deve distinguere la
    sessione/processo client dal solo username.

- Username usato come identita tecnica.
  - ACK, filtro del mittente e deduplica usano il nome utente.
  - Due client con lo stesso username possono ricevere ACK sbagliati, non vedere
    messaggi o collidere nella deduplica.
  - Fix suggerito: introdurre un id di connessione/client separato dal nome
    visualizzato.

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
  - Il problema residuo e che la deduplica in memoria cresce e non e persistente.

- Reconnect ricorsivo nel client.
  - La riconnessione richiama `startReceiverAndHeartbeat` in modo ricorsivo.
  - Non rompe subito, ma puo trattenere thread/closure e rendere piu difficile
    ragionare sui failure.

### P2

- Directory e single point of failure per reconnect.
  - Se la Directory cade, i broker possono essere vivi ma i client non hanno modo
    di scegliere un nuovo broker.
  - Accettabile se dichiarato come limite.

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

- Il mittente non riceve il proprio messaggio committato.
  - `Broker.onChatDeliver` esclude i client con username uguale al sender.
  - Questo indebolisce la proprieta "tutti i client connessi vedono lo stesso
    ordine".
  - Fix suggerito: consegnare anche al mittente e lasciare al client distinguere
    i propri messaggi.

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
2. Correggere il `matchIndex` restituito dal follower in `AppendEntries`.
3. Introdurre `(clientId, clientSeq)` e limitare/purgare la cache di deduplica.
4. Usare un writer unico o un lock comune per `ObjectOutputStream` lato client.
5. Gestire restart e retry: ricostruire la deduplica dal log e prevenire replay
   applicativo indesiderato.
