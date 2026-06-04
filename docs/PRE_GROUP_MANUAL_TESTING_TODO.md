# Pre-group manual testing TODO

Documento operativo per arrivare al testing manuale del gruppo, senza usare le classi
di test JUnit.

Aggiornato al 2026-06-03.

---

## 1. Documenti rimasti

La cartella `docs` ora contiene solo i documenti utili alla fase finale:

- `PROJECT_SPECIFICATION.md`
  - Documento principale di architettura.
  - Deve essere usato come base per spiegare requisiti, scelte, garanzie e limiti.
  - Ora riflette lo stato reale: membership statica, transport Raft ibrido,
    follower-forwarding, discovery ausiliaria e niente history offline.

- `raft_lan_broadcast_migration_report.md`
  - Non e' piu' un piano di migrazione.
  - Ora e' il documento di giustificazione finale sulla scelta
    broadcast/multicast vs unicast.
  - Serve per rispondere direttamente alla mail del professore.

- `PRE_GROUP_MANUAL_TESTING_TODO.md`
  - Checklist operativa prima del testing manuale del gruppo.
  - Va tenuto fino a quando tutti gli scenari manuali sono stati provati.

Documenti storici come report iniziali, TODO personali e dynamic-membership report non
sono piu' necessari nella documentazione finale. Le motivazioni rilevanti sono state
integrate nei due documenti principali.

---

## 2. Cose da sistemare prima del testing manuale

### P0 - Correttezza funzionale

- Sistemare o validare il retry client con vector clock.
  - Problema: il broker incrementa il vector clock quando costruisce il
    `ChatReqMessage`, anche se la proposta puo' poi essere rifiutata o ritentata.
  - Rischio: la hold-back queue puo' bloccarsi se vede un salto nel clock del broker
    sorgente.
  - Soluzione consigliata: cache lato broker per `(username, clientTimestamp)`, cosi'
    ogni retry riusa lo stesso comando, lo stesso vector clock e lo stesso
    `localMsgId`.

- Chiarire deduplicazione effettiva.
  - Oggi Raft deduplica usando `(username, MSG timestamp)`.
  - Non scrivere `(clientId, clientSeq)` come meccanismo implementato finche' non
    esiste davvero nel codice.
  - Se si vuole la versione piu' pulita, aggiungere un vero sequence number client.

- Decidere cosa fare con la discovery LAN.
  - Opzione A: correggerla usando una porta discovery comune per tutti i broker.
  - Opzione B: dichiararla come discovery ausiliaria e non usarla come parte della
    demo principale.
  - Non dire che la discovery LAN risolve davvero gli endpoint se in demo ogni broker
    usa una porta discovery diversa.

### P1 - Demo manuale

- Preparare terminali separati per Directory, tre broker e almeno due client.
- Tenere visibili i log di leader election, proposta, commit, delivery e reconnect.
- Decidere prima se pulire lo stato Raft o testare esplicitamente il restart con log
  persistito.
- Fare almeno un giro completo senza crash prima di provare il failover.

### P2 - Documentazione/slides

- Inserire nelle slide la scelta di membership statica.
- Inserire nelle slide la tabella broadcast/unicast:
  - `RequestVote` via UDP broadcast;
  - heartbeat vuoti via UDP broadcast;
  - entry reali via TCP unicast;
  - client traffic via TCP.
- Inserire una sezione "limiti noti":
  - no dynamic membership;
  - no history/offline replay;
  - no Byzantine behavior;
  - no network partitions;
  - Raft log come storage tecnico, non cronologia chat.

---

## 3. Runbook manuale

Build:

```powershell
mvn -q -DskipTests package
```

Directory:

```powershell
java -cp target/classes it.polimi.ds.chat.directory.DirectoryService
```

Broker 0:

```powershell
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 0 7000 "0@127.0.0.1:7000:50000,1@127.0.0.1:7001:50001,2@127.0.0.1:7002:50002" 50000 7100 demo-cluster 1400
```

Broker 1:

```powershell
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 1 7001 "0@127.0.0.1:7000:50000,1@127.0.0.1:7001:50001,2@127.0.0.1:7002:50002" 50001 7100 demo-cluster 1400
```

Broker 2:

```powershell
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 2 7002 "0@127.0.0.1:7000:50000,1@127.0.0.1:7001:50001,2@127.0.0.1:7002:50002" 50002 7100 demo-cluster 1400
```

Client:

```powershell
java -cp target/classes it.polimi.ds.chat.client.ClientMain 127.0.0.1 60001
```

Note:

- Il jar prodotto non espone un `Main-Class`, quindi il runbook usa
  `java -cp target/classes`.
- Per evitare stato vecchio durante una demo pulita, valutare la cancellazione della
  directory di persistenza Raft prima dell'avvio. Non farlo durante i test di restart.

---

## 4. Scenari di testing manuale

### Scenario A - Avvio base

- Avviare DirectoryService.
- Avviare broker 0, 1 e 2 con stesso `votersCSV`, stesso `raftBroadcastPort` e stesso
  `clusterId`.
- Verificare dai log che venga eletto un solo leader.
- Verificare che i follower conoscano il leader dopo heartbeat.

Esito atteso:

- un solo broker risulta leader;
- gli altri restano follower;
- nessuna election continua in loop.

### Scenario B - Chat multi-client su broker diversi

- Avviare almeno due client.
- Farli collegare possibilmente a broker diversi.
- Inviare messaggi alternati dai client.

Esito atteso:

- tutti i client connessi ricevono i messaggi nello stesso ordine;
- non compaiono gap permanenti nella hold-back queue;
- non compaiono duplicati nella delivery.

### Scenario C - Client collegato a follower

- Identificare un follower.
- Collegare un client a quel broker.
- Inviare un messaggio.

Esito atteso:

- il follower inoltra la proposta al leader;
- il leader committa;
- tutti i broker applicano la stessa entry;
- il client riceve ACK solo dopo commit.

### Scenario D - Crash leader

- Con cluster attivo e client connessi, terminare il processo del leader.
- Attendere una nuova election.
- Inviare nuovi messaggi.

Esito atteso:

- viene eletto un nuovo leader;
- il cluster continua a committare con 2 broker su 3;
- i client collegati al broker morto rilevano il failure e si riconnettono.

### Scenario E - Restart del broker fermato

- Riavviare il vecchio leader con stesso `nodeId`, stesse porte e stesso storage dir.
- Inviare nuovi messaggi dopo il restart.

Esito atteso:

- il broker rientra come follower;
- recupera il log via `AppendEntries`;
- non diventa leader con stato vecchio;
- applica le entry nello stesso ordine degli altri.

### Scenario F - Retry client

- Inviare un messaggio durante una finestra instabile, per esempio subito dopo il kill
  del leader.
- Verificare che il client ritenti finche' non riceve ACK.

Esito atteso:

- il messaggio viene committato una sola volta;
- non ci sono duplicati nella delivery;
- non si crea un gap causale permanente.

---

## 5. Cose da non fare prima della demo

- Non implementare dynamic membership adesso.
- Non passare a full UDP per `AppendEntries` con entry reali.
- Non aggiungere refactor grandi non necessari.
- Non presentare DirectoryService come parte del consenso.
- Non presentare PeerRegistry/discovery come sorgente del quorum.

---

## 6. Frasi pronte per l'orale

Membership:

> La membership votante e' statica per preservare la safety del quorum Raft. La
> discovery puo' aiutare a trovare broker o indirizzi, ma non puo' cambiare chi vota.
> Dynamic membership richiederebbe config entries replicate, learner e promozione
> controllata.

Broadcast:

> Usiamo UDP broadcast dove il messaggio e' piccolo e destinato a tutti: `RequestVote`
> e heartbeat vuoti. Manteniamo TCP per `AppendEntries` con payload per evitare
> frammentazione UDP, ACK/NACK applicativi e retry selettivo manuale.

Storage dei messaggi:

> I client non ricevono history e i broker non offrono storage applicativo dei
> messaggi offline. Il log Raft persistito e' storage tecnico necessario alla safety
> del consenso; non viene usato come cronologia per riconnettere client disconnessi.
