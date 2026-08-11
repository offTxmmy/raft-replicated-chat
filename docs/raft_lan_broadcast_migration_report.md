# Raft LAN Broadcast Transport - Design and Compliance Report

Aggiornato al 2026-08-11 dopo il confronto con la specifica ufficiale.

Questo non e' un piano di migrazione. Descrive il trasporto implementato, la sua
motivazione e le prove ancora necessarie per dimostrare che il progetto sfrutta
realmente il broadcast di livello link tra broker sulla stessa LAN.

---

## 1. Requisito ufficiale

La specifica del progetto Replicated Chat Infrastructure stabilisce che:

- i broker sono collegati alla stessa LAN;
- il broadcast di livello link e' disponibile tra i broker;
- il progetto deve assumere e sfruttare questa disponibilita';
- i client possono essere su Internet e quindi non devono dipendere dal broadcast
  della LAN dei broker.

La regola generale del corso aggiunge che un progetto Java puo' usare soltanto socket
TCP/UDP, unicast/multicast, oppure RMI. L'implementazione corrente usa esclusivamente
socket Java TCP/UDP.

Una demo solo su `127.0.0.1` non dimostra questo requisito: la prova finale deve
coinvolgere almeno due notebook fisici sulla stessa LAN.

---

## 2. Scelta implementata

Il trasporto Raft e' ibrido:

| Messaggio | Trasporto corrente | Destinatario | Motivazione |
| --- | --- | --- | --- |
| `RequestVote` request | UDP broadcast | Tutti i voter della LAN | Messaggio piccolo e naturalmente one-to-many. |
| `RequestVote` response | UDP unicast | Candidato | Risposta peer-specifica. |
| `AppendEntries` senza entry | UDP broadcast | Tutti i follower | Heartbeat piccolo, periodico e comune. |
| Response a heartbeat vuoto | UDP unicast | Leader | Stato di un singolo follower. |
| `AppendEntries` con entry | TCP unicast | Singolo follower | Payload affidabile e ordinato, catch-up e retry specifici per follower. |
| Response a append con payload | TCP sulla stessa RPC | Leader | Risultato specifico del follower. |
| Proposal forwarding | TCP unicast | Leader noto | Richiesta punto-punto che attende l'esito del commit. |
| Client-broker | TCP unicast | Broker scelto | Il client puo' trovarsi fuori dalla LAN. |
| Broker/directory e client/directory | TCP unicast | Directory | Registrazione, heartbeat e lookup punto-punto. |
| Discovery ausiliaria | UDP broadcast | Peer sulla LAN | Non modifica membership o quorum. |

Componenti principali:

- `RaftHybridTransport` sceglie UDP broadcast per vote request e heartbeat vuoti e
  TCP per append con payload;
- `RaftUdpBroadcastTransport` enumera le interfacce di rete attive, ignora loopback e
  invia all'indirizzo broadcast di ogni interfaccia idonea;
- `RaftRpcClient` e `RaftRpcServer` gestiscono le RPC TCP;
- `RaftConfig` contiene voter set statico, porta broadcast comune, `clusterId` e
  limite del payload UDP;
- `RaftUdpEnvelope` include cluster, sender, target, tipo, term, message id e sequence
  per filtrare traffico estraneo e duplicato.

---

## 3. Perche' non usare broadcast per tutto

`AppendEntries` con payload non e' un unico messaggio identico per tutti i follower.
Ogni follower ha il proprio `nextIndex`, puo' richiedere backtracking diverso e puo'
essere in una fase differente di catch-up. Un broadcast indiscriminato sarebbe poco
adatto anche prima di considerare l'affidabilita'.

Una replica completa via datagram richiederebbe inoltre:

- frammentazione e riassemblaggio oltre il limite del datagram;
- ACK/NACK, timeout e ritrasmissioni selettive;
- deduplica e ordinamento dei frammenti;
- backpressure;
- gestione separata dei follower lenti o appena riavviati.

TCP fornisce uno stream affidabile e ordinato per la parte follower-specifica. UDP
broadcast resta vantaggioso per i piccoli messaggi comuni a tutti i voter. Questa
scelta sfrutta la caratteristica LAN richiesta senza introdurre una nuova reliability
layer per i log payload.

---

## 4. Semantica in caso di perdita

UDP broadcast non garantisce consegna. La correttezza non deve dipendere da un singolo
datagram:

- una vote request persa puo' essere seguita da retry o da una nuova election;
- un heartbeat perso puo' essere seguito dal prossimo heartbeat;
- le response sono correlate e i duplicati recenti sono filtrati;
- la replica effettiva delle entry e il catch-up restano sul canale TCP unicast.

L'assunzione ufficiale permette link failure ma esclude network partition. Quindi la
garanzia dichiarabile e' convergenza dopo perdite/interruzioni transitorie quando la
rete resta o torna connessa; non e' disponibilita' durante una partition.

---

## 5. Membership e broadcast sono concetti separati

La membership votante resta statica e identica in tutti i broker:

- `RaftConfig.getVoters()` e' la sola sorgente del quorum;
- il quorum e' `floor(N/2) + 1` sul voter set statico;
- ricevere un datagram da un broker non lo rende voter;
- `LanDiscoveryService` e `PeerRegistry` non possono aggiungere o rimuovere voti;
- il `clusterId` evita che cluster diversi sulla stessa LAN elaborino reciprocamente
  i propri datagram.

Il broadcast e' un mezzo di comunicazione one-to-many, non un protocollo di dynamic
membership. Il progetto non richiede membership dinamica e l'implementazione non la
offre.

---

## 6. Configurazione della LAN reale

Tutti i broker devono usare:

- lo stesso voter set con IP LAN reali;
- la stessa `raftBroadcastPort`, ad esempio `7100`;
- lo stesso `clusterId`, ad esempio `demo-cluster`;
- porte TCP Raft e chat raggiungibili dagli altri host;
- interfacce di rete per cui Java possa ricavare un indirizzo broadcast.

Prima della demo:

1. disabilitare VPN/interfacce virtuali che possano ricevere il broadcast per errore;
2. verificare che l'access point non abiliti client isolation;
3. autorizzare Java e la porta UDP comune nel firewall della rete privata;
4. verificare che piu' processi sullo stesso host possano condividere la porta UDP
   secondo il comportamento del sistema operativo scelto;
5. osservare nei log o con una packet capture che il datagram inviato da un notebook
   viene ricevuto dall'altro.

Il blocker attuale del deployment e' esterno al transport Raft: `BrokerMain` e
`Broker` contattano la Directory su `localhost`. Prima della prova su due notebook
host e porte Directory devono diventare configurabili. Il runbook target e' in
`PRE_GROUP_MANUAL_TESTING_TODO.md`.

---

## 7. Evidenza automatica e limite dell'evidenza

Il 2026-08-11 `mvn test` ha completato:

```text
Tests run: 186, Failures: 0, Errors: 0, Skipped: 0
```

La suite include test del transport ibrido e integrazioni Raft in-process. Non prova
che il broadcast attraversi la LAN fisica usata alla presentazione e non sostituisce
la demo obbligatoria su almeno due notebook.

Test manuali necessari:

- election con vote request ricevuta via broadcast sull'altro notebook;
- heartbeat broadcast stabile senza election spurie;
- append con payload osservato come TCP unicast;
- perdita di uno o piu' heartbeat e successivo recupero;
- crash leader, nuova election e catch-up del broker riavviato;
- nessun uso del broadcast da parte dei client.

---

## 8. Traccia per la presentazione

Una formulazione precisa e difendibile e':

> I broker sono sulla stessa LAN. Usiamo UDP link-layer broadcast per i messaggi
> piccoli e identici destinati a tutti i voter: RequestVote e AppendEntries vuoti di
> heartbeat. Le response sono UDP unicast. Le entry di log sono replicate via TCP
> unicast perche' il catch-up e' specifico per follower e richiede un canale
> affidabile e ordinato. I client usano TCP e non dipendono dalla LAN dei broker.
> La membership Raft resta statica: broadcast non significa reconfiguration.

Accompagnare questa spiegazione con:

- diagramma di deployment sui notebook reali;
- tabella dei messaggi della sezione 2;
- log o packet capture della prova LAN;
- dichiarazione esplicita: link failure transitori in scope, network partition fuori
  dall'assunzione del progetto.
