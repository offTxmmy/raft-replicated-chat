# Raft LAN broadcast: design e stato di validazione

Analisi originale: **2026-08-11**, commit
`c77dbb981a3db837c50ef17bf9bd279278c04998`. Stato aggiornato il **2026-08-13**.
Baseline precedente agli ultimi fix mirati:
`b5bf3857ea85be8f8d1758261cc428b0c5fba09b` (`master`).

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
Le response obsolete non possono piu' far regredire `matchIndex`; `nextIndex` resta
sempre almeno `matchIndex + 1` (`CODE-14`). I finding Raft `CODE-02/03` sono gia'
chiusi e i regression test higher-term/step-down restano nella suite ordinaria.

## 5. Membership statica

Broadcast e membership sono concetti separati. Il voter set configurato determina:

- chi puo' votare e inviare messaggi Raft accettati;
- il denominatore della maggioranza;
- gli endpoint TCP per replica/forwarding.

La reachability runtime non aggiunge o rimuove voter. Un broker non viene escluso
dal quorum perche' non risponde. Dynamic membership e joint consensus restano fuori
scope.

## 6. Separazione della configurazione UDP

Non esiste piu' una seconda porta UDP per-node nel `BrokerConfig`. L'unica
configurazione UDP broker-to-broker e' quella del transport Raft: porta broadcast
comune al cluster, `clusterId` e limite del payload sono tutti contenuti in
`RaftConfig`. Il voter set statico resta indipendente dalla reachability runtime.

## 7. Deployment corrente

Argomenti supportati da `BrokerMain`:

```text
raft <nodeId> <rpcPort> [clientPort] [raftBroadcastPort] [clusterId]
     [udpMaxPayloadBytes] [directoryHost] [directoryBrokerPort]
```

Host e porta broker della Directory sono configurabili e la stessa configurazione e'
usata per bootstrap, registration, heartbeat e re-registration. I default restano
`localhost:60000`. La porta RPC CLI viene validata rispetto al voter locale e la
porta client pubblicizzata coincide con quella realmente in ascolto. Questo chiude
`CODE-10` in codice; indirizzi LAN e firewall restano da verificare fisicamente.

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

Il 2026-08-13:

```text
mvn clean test
Tests run: 290, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

I sei casi legacy JUnit 4 sono migrati a Jupiter e inclusi nel conteggio. La suite
prova routing del transport, filtri envelope, round-trip loopback, cluster Raft e
percorso applicativo completo. Uno smoke aggiuntivo con Directory, tre JVM broker e
tre JVM client `LOCAL_TCP` ha verificato chat esattamente una volta, leader kill,
rielezione e reconnect. Nessuna di queste evidenze dimostra che il broadcast HYBRID
attraversi la LAN fisica della presentazione.

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
