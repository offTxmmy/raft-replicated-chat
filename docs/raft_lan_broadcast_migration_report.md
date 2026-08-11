# Raft LAN broadcast: design e stato di validazione

Aggiornato il **2026-08-11** sul commit
`c77dbb981a3db837c50ef17bf9bd279278c04998`.

Questo documento spiega la scelta del trasporto. Non e' un tracker: azioni e gate
sono in [`PRE_GROUP_MANUAL_TESTING_TODO.md`](PRE_GROUP_MANUAL_TESTING_TODO.md).

## 1. Obiettivo

La specifica colloca i broker sulla stessa LAN e richiede di sfruttare il broadcast
disponibile. Il chiarimento del professore non impone un solo protocollo per tutti i
messaggi: richiede una scelta motivata rispetto a destinatari, affidabilita', payload
e traffico.

Il design corrente usa quindi:

- UDP broadcast per piccoli messaggi comuni ai voter;
- UDP unicast per piccole risposte peer-specific;
- TCP unicast per replica con payload e forwarding di proposal;
- TCP per client e Directory, che non devono dipendere dalla LAN broadcast.

Con le socket Java, “link-layer broadcast” viene sfruttato inviando datagram IPv4 agli
indirizzi broadcast delle interfacce LAN idonee. Non viene usato raw Ethernet.

## 2. Matrice implementata

| Messaggio | Mezzo | Destinatari | Motivo |
|---|---|---|---|
| `RequestVote` request | UDP broadcast | voter della LAN | Piccolo, identico e one-to-many. |
| `RequestVote` response | UDP unicast | candidate | Risposta specifica. |
| `AppendEntries` vuoto | UDP broadcast quando identico | follower | Heartbeat comune e piccolo. |
| `AppendEntries` vuoto non identico | TCP/UDP peer-specific secondo il transport | singolo follower | `prevLog*` o commit view differiscono. |
| `AppendEntries` con entry | TCP unicast | singolo follower | Il suffisso dipende da `nextIndex`; payload e affidabilita' favoriscono TCP. |
| `AppendEntries` response | UDP unicast | leader | Risposta piccola; puo' essere duplicata/riordinata. |
| Forward proposal | TCP unicast | leader noto | Richiesta/risposta sincrona. |
| Client-broker | TCP | broker scelto | I client possono essere fuori LAN. |
| Directory | TCP | endpoint centrale | Bootstrap, registration, lookup e heartbeat. |
| Discovery ausiliaria | UDP broadcast | peer locali | Non modifica voter set o quorum. |

Classi principali:

- `RaftHybridTransport` seleziona UDP/TCP;
- `RaftUdpBroadcastTransport` enumera interfacce, invia/riceve envelope e filtra;
- `RaftRpcClient`/`RaftRpcServer` gestiscono TCP one-shot;
- `RaftReplicationManager` raggruppa heartbeat realmente identici e mantiene stato
  peer-specific per le repliche con payload;
- `RaftElectionManager` emette RequestVote e heartbeat round;
- `RaftConfig` contiene voter statici, porta UDP comune, `clusterId` e payload massimo.

## 3. Perche' AppendEntries con payload non e' broadcast

Ogni follower puo' avere `nextIndex` diverso. Ne conseguono valori differenti di:

- `prevLogIndex` e `prevLogTerm`;
- suffisso da inviare;
- conflict recovery e retry;
- progresso `matchIndex` usato per il commit.

Broadcastare indiscriminatamente il log richiederebbe frammentazione, riordino,
reassembly, ritrasmissione e controllo payload sopra UDP. Aumenterebbe traffico e
complessita' senza migliorare la correttezza. TCP peer-to-peer conserva la semantica
per-follower naturale di Raft.

## 4. Garanzie e recupero dalle perdite UDP

UDP non garantisce consegna, ordine o unicita'. Il codice applica queste difese:

- envelope con `clusterId`, `messageId`, sender, target e voter metadata;
- filtro di self, cluster errato, sender non voter e target non locale;
- deduplica bounded per **`messageId`**, non per il campo applicativo `sequence`;
- term e ruolo controllati nel core Raft;
- election future recuperano una RequestVote request persa;
- heartbeat successivi e replica TCP recuperano una heartbeat/response persa;
- AppendEntries con payload usa TCP e conflict hints.

Non va affermato che ogni datagram perso venga ritentato nella stessa election o che
le response siano correlate a una specifica RPC. Oggi le response sono riconosciute
per tipo, term, responder/target e stato corrente; non esiste una send generation.
Response obsolete possono far regredire `nextIndex` (`CODE-14`), pur lasciando
`matchIndex` monotono.

Restano inoltre due finding Raft indipendenti dal mezzo:

- `CODE-02`: role/term/append non sono atomici rispetto allo step-down;
- `CODE-03`: alcune higher-term response sono filtrate prima di osservare il term o
  non completano il lifecycle follower.

## 5. Membership statica

Broadcast e membership sono concetti separati. Il voter set configurato determina:

- chi puo' votare e inviare messaggi Raft accettati;
- il denominatore della maggioranza;
- gli endpoint TCP per replica/forwarding.

La discovery e la reachability non aggiungono/rimuovono voter. Un broker non viene
escluso dal quorum perche' non risponde e un HELLO discovery non crea membership.
Dynamic membership e joint consensus restano fuori scope.

## 6. Limiti correnti di discovery

La discovery ausiliaria non e' una prova di broadcast Raft e oggi non scopre
correttamente peer con id diversi:

- `BrokerMain` assegna `udpPort = 50002 + nodeId`;
- ogni `LanDiscoveryService` ascolta e trasmette sulla propria porta;
- un HELLO di node 0 a 50002 non raggiunge node 1 in ascolto su 50003;
- gli annunci avvengono soltanto poche volte all'avvio, penalizzando peer tardivi.

Poiche' Raft usa una porta broadcast comune separata e voter statici, questo difetto e'
`OPT-05`, non un blocker del consenso. La demo non deve dipendere dal PeerRegistry.

## 7. Deployment corrente

Argomenti supportati da `BrokerMain`:

```text
raft <nodeId> <rpcPort> [clientPort] [raftBroadcastPort] [clusterId] [udpMaxPayloadBytes]
```

Non esistono oggi argomenti per host/porta Directory. Bootstrap, registration e
heartbeat del broker usano `localhost:60000`. Di conseguenza non esiste ancora un
runbook corretto per un unico Directory Service su notebook A e un broker su notebook
B; non vanno documentati flag inesistenti. `CODE-10` deve essere chiuso prima della
prova fisica.

Configurazione da mantenere identica fra voter:

- voter CSV e mapping id -> host/RPC/client port;
- `clusterId`;
- porta UDP Raft comune;
- payload UDP massimo compatibile con la rete;
- IP LAN effettivi, non loopback.

Porte da consentire nel firewall:

- TCP `60000` broker/Directory e `60001` client/Directory;
- TCP RPC Raft configurate;
- TCP client port dei broker;
- UDP porta Raft comune.

La porta discovery e' opzionale. VPN e interfacce virtuali possono produrre broadcast
su reti sbagliate; AP/client isolation puo' bloccare datagram fra notebook.

Per lo smoke con piu' broker sullo stesso host, tutti bindano la stessa porta UDP con
reuse. Il demultiplexing delle unicast e' OS-dependent: l'integrazione corrente passa
su Windows, ma il setup locale va pre-validato. La demo consigliata distribuisce i
broker sui notebook e riduce questa ambiguita'.

## 8. Evidenza automatica disponibile

Il 2026-08-11:

```text
mvn test
Tests run: 186, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Sei test JUnit 4 compilati non sono inclusi da Maven; esecuzione diretta:

```text
JUnit version 4.13.1
OK (6 tests)
```

La suite prova routing del transport, filtri envelope, round-trip loopback e un
cluster Raft in-process. Lo smoke fresco ha eletto tre processi broker same-host, ma
ha incontrato il blocker applicativo `seq=2/expected=1`. Nessuna di queste evidenze
dimostra che il broadcast attraversi la LAN fisica della presentazione.

## 9. Gate LAN su due notebook

La validazione e' completa soltanto quando `LAN-01..03` sono `VERIFIED`:

1. almeno due notebook sulla stessa LAN con processi realmente distribuiti;
2. unico voter set e Directory raggiungibile secondo CODE-10;
3. RequestVote broadcast osservata sull'altro notebook;
4. heartbeat broadcast stabile senza election spurie;
5. payload AppendEntries/catch-up via TCP;
6. almeno due client destinatari su broker fisicamente diversi con transcript uguale
   per i messaggi comuni; il sender puo' ricevere soltanto ACK;
7. leader crash-stop, nuova election e chat successiva;
8. evidenza conservata: commit, comandi, log e/o breve packet capture.

## 10. Formulazione sicura per la presentazione

Dopo la chiusura dei gate si puo' affermare:

> I broker usano UDP broadcast sulla LAN per RequestVote e heartbeat vuoti comuni.
> Le risposte sono unicast; AppendEntries con payload e forwarding delle proposal
> usano TCP perche' dipendono dallo stato del singolo follower. La membership Raft
> resta statica: broadcast non significa reconfiguration.

Prima della prova fisica non va usato il verbo “validato”; la formulazione corretta e'
“implementato e verificato in loopback/same-host, in attesa di validazione LAN”.
