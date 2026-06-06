# Raft LAN Broadcast Transport - Final Report

Aggiornato al 2026-06-03.

Questo documento sostituisce il vecchio piano di migrazione. Non va letto come una
lista di TODO: descrive la scelta finale di trasporto broker-broker e la
giustificazione da portare all'orale.

---

## 1. Requisito da soddisfare

La specifica dice che i broker sono sulla stessa LAN e che il broadcast di livello
link e' disponibile. Quindi non basta dire "Raft funziona su TCP": dobbiamo spiegare
dove sfruttiamo broadcast/multicast e dove scegliamo invece unicast per ottenere
garanzie migliori.

La risposta del professore richiede due giustificazioni:

- membership statica o dinamica, in funzione delle garanzie;
- scelta dei singoli messaggi, broadcast/multicast o unicast, in funzione di garanzie
  e traffico su LAN.

---

## 2. Scelta finale

La scelta implementativa e' un trasporto Raft ibrido.

| Messaggio | Trasporto | Motivazione |
| --- | --- | --- |
| `RequestVote` request | UDP LAN broadcast | Messaggio piccolo, destinato naturalmente a tutti i voter. Riduce traffico ripetitivo rispetto a N connessioni separate. |
| `RequestVote` response | Unicast verso il candidato | La risposta ha un destinatario specifico e contiene il voto di un singolo broker. |
| `AppendEntries` vuoto, cioe' heartbeat | UDP LAN broadcast | Messaggio piccolo, periodico, destinato a tutti i follower. E' il caso piu' adatto al broadcast. |
| `AppendEntries` con log entries | TCP unicast | Payload potenzialmente grande, serve affidabilita', ordine, retry e backtracking per follower. |
| Proposal forwarding follower -> leader | TCP unicast | La proposta deve arrivare a un leader specifico. |
| Client <-> broker | TCP | I client non sono sulla LAN dei broker, quindi il broadcast link-layer non e' applicabile. |

Questa scelta sfrutta davvero la LAN broadcast dove porta vantaggio senza spostare su
UDP la parte piu' rischiosa: la replica affidabile delle entry di log.

---

## 3. Componenti coinvolti

- `RaftUdpBroadcastTransport`
  - gestisce i datagram UDP broadcast per messaggi Raft piccoli;
  - filtra messaggi del cluster sbagliato, messaggi locali e sender non votanti;
  - evita di trasformare discovery o broadcast in membership dinamica.

- `RaftHybridTransport`
  - decide quale canale usare;
  - usa broadcast per vote request e heartbeat vuoti;
  - delega al TCP path per append con payload.

- `RaftRpcClient` / `RaftRpcServer`
  - restano necessari per TCP;
  - gestiscono entry replication con payload e proposal forwarding.

- `RaftConfig`
  - contiene il set statico dei voter;
  - contiene i parametri di broadcast Raft, per esempio porta comune, cluster id e
    limite payload.

- `LanDiscoveryService` / `PeerRegistry`
  - sono separati dal trasporto Raft;
  - non sono sorgente del quorum;
  - sono ausiliari e non vanno presentati come requisito di safety.

---

## 4. Perche' non full broadcast?

Portare anche `AppendEntries` con log entries su UDP broadcast richiederebbe una
reliability layer applicativa:

- limite massimo del datagram;
- frammentazione e riassemblaggio;
- ACK/NACK per frammenti o entry;
- retry selettivo;
- deduplica;
- backpressure;
- gestione di follower lenti o appena riavviati.

Raft tollera perdita di messaggi, ma la replica efficiente del log dipende da retry,
conflict hints, `nextIndex`, `matchIndex` e catch-up. TCP e' piu' adatto per questa
parte perche' fornisce stream affidabile e ordinato. In una demo universitaria, full
UDP per il payload aumenterebbe molto il rischio di bug senza migliorare le garanzie
richieste.

---

## 5. Membership e broadcast non sono la stessa cosa

Il broadcast non cambia chi vota.

La membership votante resta statica e viene letta da `RaftConfig.getVoters()`. Un
broker ricevuto via discovery LAN o visto su una porta broadcast non entra nel quorum
automaticamente. Se non e' nel voter set configurato, i suoi messaggi non devono
contare per Raft.

Questa distinzione e' fondamentale per l'orale:

> Usiamo broadcast come mezzo di comunicazione LAN, non come meccanismo di
> reconfiguration. Il quorum deve restare identico su tutti i broker.

---

## 6. Nota sulla discovery LAN

La discovery LAN non viene usata per costruire il cluster dei broker. La membership
votante e' statica: il `votersCSV` viene passato alla `DirectoryService` all'avvio e
i broker recuperano da li' la stessa topologia tramite `GetClusterRequestMessage`.

Quindi non bisogna presentare `LanDiscoveryService` o `PeerRegistry` come sorgente
degli endpoint Raft, della membership o del quorum. La LAN resta sfruttata per il
trasporto broadcast dei messaggi Raft piccoli.

Il broadcast Raft invece deve usare una porta comune di cluster, per esempio il
parametro `raftBroadcastPort`.

---

## 7. Runbook minimo per demo locale

Build:

```powershell
mvn -q -DskipTests package
```

Directory:

```powershell
java -cp target/classes it.polimi.ds.chat.directory.DirectoryService "0@127.0.0.1:7000:50000,1@127.0.0.1:7001:50001,2@127.0.0.1:7002:50002"
```

Broker 0:

```powershell
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 0 7000 50000 7100 demo-cluster 1400
```

Broker 1:

```powershell
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 1 7001 50001 7100 demo-cluster 1400
```

Broker 2:

```powershell
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 2 7002 50002 7100 demo-cluster 1400
```

I valori importanti da tenere allineati tra broker sono:

- stesso `votersCSV` configurato nella `DirectoryService`;
- stesso `raftBroadcastPort`, qui `7100`;
- stesso `clusterId`, qui `demo-cluster`;
- porte client e Raft TCP diverse per ogni processo.

---

## 8. Cosa verificare manualmente

- Un solo leader viene eletto.
- I follower ricevono heartbeat e non partono in election continua.
- Un client collegato a un follower riesce a inviare tramite forwarding al leader.
- Messaggi inviati da client su broker diversi vengono consegnati nello stesso ordine.
- Dopo crash del leader, i due broker rimasti eleggono un nuovo leader.
- Dopo restart, il vecchio leader rientra come follower e recupera il log.

---

## 9. Frase pronta per l'orale

> Abbiamo scelto membership statica per mantenere stabile il calcolo del quorum Raft.
> Il `votersCSV` viene configurato nella Directory all'avvio e non viene scoperto
> dinamicamente via LAN.
> Usiamo UDP broadcast sulla LAN per i messaggi piccoli e destinati a tutti, cioe'
> `RequestVote` e heartbeat vuoti. Usiamo TCP unicast per le entry di log perche'
> richiedono affidabilita', ordine, retry e catch-up. In questo modo sfruttiamo la
> proprieta' della LAN indicata dalla specifica senza introdurre una reliability layer
> UDP complessa e rischiosa.
