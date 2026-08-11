# Code Issues by Group - Specification Compliance

Aggiornato al 2026-08-11 dopo il confronto con la specifica ufficiale
`projects_2025-2026_v1.pdf`.

Questo documento contiene solo problemi ancora rilevanti per la conformita' o per la
demo. La matrice normativa completa e' in `PROJECT_SPECIFICATION.md`.

Legenda:

- P0: blocca un requisito esplicito o la demo obbligatoria;
- P1: rischio importante da correggere o dichiarare con precisione;
- P2: robustezza, manutenzione o qualita' della presentazione;
- FATTO: verificato nel codice corrente o da test automatici;
- DA VALIDARE: implementato, ma senza prova end-to-end adeguata.

---

## 1. Ordering Raft e sequenza applicativa

### P0

- [ ] **Il no-op Raft crea un gap permanente nella `HoldBackQueue`.**
  - A ogni elezione il leader aggiunge una entry con comando `null`.
  - `RaftStateMachineAdapter` non emette una `ChatDeliverMessage` per il no-op.
  - Il successivo messaggio chat usa comunque il raw log index. Al primo term puo'
    quindi arrivare `seq=2` mentre la queue attende `seq=1`.
  - La queue non puo' ricevere l'indice mancante e non consegna il messaggio.
  - Il problema ricompare con nuovi term e al restart, perche' il progresso
    applicativo della queue non viene riallineato al `lastApplied` Raft.
  - Owner: Replication/Log + livello applicativo.
  - Fix accettabili: propagare alla queue anche l'avanzamento delle entry non-chat e
    ripristinarlo al restart, oppure introdurre una sequenza chat contigua distinta
    dal log index.
  - Test richiesto: tre `Broker` reali, elezione con no-op, client su broker diversi,
    almeno un cambio leader e verifica della stessa sequenza consegnata.

- [ ] **La proprieta' "ogni client riceve nello stesso ordine" non ha ancora un test
  end-to-end.**
  - Gli integration test correnti verificano soprattutto `RaftOrderingService`, non
    l'intero percorso client -> broker -> hold-back queue -> client.
  - I 186 test automatici passano, ma non rilevano il gap precedente.
  - Owner: integrazione/test.

### P1

- [FATTO] Membership votante statica e quorum derivato da `RaftConfig.getVoters()`.
- [FATTO] `RequestVote` e heartbeat vuoti usano UDP LAN broadcast; append con payload
  e proposal forwarding usano TCP unicast.
- [FATTO] Il leader aggiunge una no-op nel nuovo term; resta da correggere il suo
  effetto sulla sequenza applicativa descritto nel P0.
- [FATTO] `commitIndex` e `lastApplied` sono persistiti e ripristinati.
- [FATTO] Il follower risponde con l'indice effettivamente confermato dalla singola
  `AppendEntries`, senza sovrastimare `matchIndex`.
- [FATTO] Il log impedisce il truncate di entry gia' committate.
- [FATTO] Il cluster a un solo nodo puo' avanzare il commit senza attendere response
  inesistenti; la demo prevista resta comunque a tre broker.

### P2

- [ ] `RaftLog.append` persiste prima di aggiornare la lista in memoria. Un errore tra
  i due passi puo' lasciare disco e memoria temporaneamente divergenti.
- [ ] La hold-back queue non ha recovery attiva per un gap: attende indefinitamente.
  Dopo la correzione P0, un gap deve essere trattato come sintomo di catch-up o bug e
  reso osservabile nella demo.

---

## 2. Causal delivery e livello applicativo

### P0

- [ ] **Aggiornamento causale eseguito dopo l'I/O verso i client.**
  - `Broker.handleOrderedMessage` consegna prima ai client e aggiorna poi il clock
    usato per nuove proposte.
  - Un client che risponde immediatamente puo' generare una nuova proposta prima che
    il broker incorpori la dipendenza appena consegnata.
  - Fix: aggiornare lo stato causale prima di rendere osservabile il messaggio ai
    client, evitando di tenere lock durante I/O lento.
  - Test richiesto: `m1` consegnato a un client su broker B, risposta immediata `m2`
    da B e verifica che nessun client osservi `m2` prima di `m1`.

- [ ] **La regola causale ignora la componente del broker mittente.**
  - E' una scelta introdotta per evitare blocchi quando l'ordine di proposta locale e
    l'ordine Raft differiscono.
  - Non basta documentarla come ottimizzazione: occorre dimostrare che l'ordine totale
    scelto estende happens-before, inclusa la program order di uno stesso client.
  - Owner: causal delivery.

### P1

- [FATTO] Identita' tecnica dei messaggi basata su `(clientId, clientSeq)`.
- [FATTO] Retry dello stesso messaggio riusa il medesimo comando e il medesimo vector
  clock tramite la cache del broker.
- [FATTO] ACK e deduplica non dipendono piu' dallo username.
- [FATTO] Due client con lo stesso username restano distinti tramite `clientId`.
- [ ] Le notifiche JOIN/QUIT sono locali e hanno una numerazione separata. Devono
  restare fuori dalla prova di total order della chat e dalle schermate usate per
  confrontare le sequenze globali.

---

## 3. Client, Directory e demo su almeno due notebook

### P0

- [ ] **I broker non possono ancora configurare l'host remoto della Directory.**
  - `BrokerMain` usa `DIRECTORY_HOST = "localhost"` per leggere il voter set.
  - `Broker` usa nuovamente `localhost` per registrazione e heartbeat.
  - Questo rende fuorviante il vecchio runbook: una singola Directory su notebook A
    non e' raggiungibile dal broker avviato su notebook B.
  - Fix: rendere host e porte Directory parametri coerenti di `BrokerMain` e
    `BrokerConfig`, quindi usare gli IP LAN reali.
  - Owner: collegamenti Broker/Client/Directory.

- [ ] **Manca la prova obbligatoria su almeno due notebook fisici.**
  - La specifica richiede una LAN wired/wireless e almeno due notebook degli studenti.
  - Il test localhost resta utile come smoke test, ma non vale come demo finale.
  - Dopo il fix precedente verificare firewall, TCP inter-host e ricezione UDP
    broadcast sulla rete scelta per l'esame.

- [ ] **Scritture concorrenti sullo stesso `ObjectOutputStream` del client.**
  - Sender, retry, heartbeat, JOIN e QUIT possono scrivere sul medesimo stream senza
    un lock condiviso.
  - `ObjectOutputStream` non e' thread-safe: il protocollo puo' corrompersi durante
    traffico e failover.
  - Fix: writer unico o funzione di invio condivisa con un solo lock per ogni
    connessione; lo stesso meccanismo deve essere aggiornato atomically al reconnect.

### P1

- [ ] La Directory rimuove un broker morto solo dopo timeout/reaper. In quella finestra
  puo' restituire un endpoint morto.
- [ ] Se un tentativo di reconnect fallisce, il nuovo heartbeat manager non viene
  riavviato e non esiste un retry loop con backoff. Il client puo' restare disconnesso.
- [ ] La Directory e' un single point of failure per nuove connessioni e reconnect.
  Questo non e' vietato dalla specifica, ma limita la gestione dei client failure e
  va dichiarato.
- [ ] `registeredBrokers` e `brokersById` possono divergere se lo stesso broker id si
  registra con un endpoint diverso; la selezione puo' conservare una entry fantasma.
- [ ] `Broker.onChatDeliver` tiene il lock della lista client mentre esegue I/O. Un
  client lento puo' ritardare tutti gli altri client dello stesso broker.

### P2

- [ ] Cache `cachedClientRequests` e deduplica Raft crescono senza TTL o compaction.
- [ ] I timeout directory/client devono essere misurati sul Wi-Fi reale della demo,
  non scelti solo in base a test locali.

---

## 4. Requisito "brokers do not store messages"

### P0 di decisione progettuale

- [ ] **Chiarire la compatibilita' tra la frase ufficiale e il log Raft persistente.**
  - Non esistono history API, offline inbox o replay: questo rispetta la semantica
    "clients receive messages only while connected".
  - Tuttavia `FileRaftPersistence` salva su disco `ChatCommand` con il testo del
    messaggio. E' storage tecnico, ma resta storage letterale del contenuto.
  - Non presentare come fatto certo che il requisito vieti solo la history.
  - Azione: chiedere conferma ai docenti oppure modificare la persistenza e la relativa
    strategia di recovery. Se la persistenza e' ammessa, definire retention/compaction
    e ribadire che non e' mai esposta ai client.

---

## 5. Requisiti di consegna e presentazione

### P0/P1 organizzativi

- [ ] Confermare gruppo di 2-3 studenti.
- [ ] Prenotare la presentazione prima dell'ultima sessione ufficiale dell'A.Y.
  2025-2026.
- [ ] Preparare poche slide con entrambe le architetture richieste:
  - software architecture: componenti e responsabilita';
  - run-time architecture: processi, notebook, IP/porte e canali TCP/UDP.
- [ ] Inserire nelle slide assunzioni e limiti: niente Byzantine, niente partition,
  membership statica, dipendenza dalla Directory.
- [ ] Usare esclusivamente socket TCP/UDP o RMI. Il codice corrente usa solo socket;
  non introdurre HTTP, gRPC, broker middleware o framework di networking non ammessi.

---

## 6. Fix storici gia' verificati

Questi punti non devono essere riaperti salvo regressioni:

- [FATTO] callback election/replication invocate fuori dai lock critici per evitare il
  deadlock AB-BA;
- [FATTO] election timeout resettato anche quando una `AppendEntries` valida viene
  respinta per mismatch di log;
- [FATTO] quorum non derivato da LAN discovery/`PeerRegistry`;
- [FATTO] deduplica Raft ricostruita dal log persistito;
- [FATTO] `clientId` incluso in JOIN, chat delivery e filtro dell'originatore;
- [FATTO] `serialVersionUID` espliciti per i principali oggetti persistiti;
- [FATTO] fsync best-effort della parent directory dopo i rename atomici della
  persistenza.

---

## 7. Ordine di lavoro prima della demo

1. Correggere il gap no-op/log-index nella delivery applicativa.
2. Rendere configurabile la Directory per i broker su notebook diversi.
3. Serializzare tutte le scritture client sullo stesso stream.
4. Correggere e testare il passaggio delle dipendenze causali prima dell'I/O client.
5. Risolvere con i docenti o nel design la questione del log persistente con payload.
6. Aggiungere test end-to-end con `Broker` e client, non solo con il servizio Raft.
7. Eseguire e registrare il runbook su almeno due notebook nella LAN dell'esame.
8. Preparare slide e tabella finale requisito/evidenza.
