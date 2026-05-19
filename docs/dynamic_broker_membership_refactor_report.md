# Dynamic Broker Membership Refactor Report

Aggiornato al 2026-05-19.

## Obiettivo

Questo report descrive il refactor necessario se non possiamo piu'
assumere un cluster statico di broker. In altre parole: il sistema deve
supportare una set di broker dinamica, con broker che possono apparire
dopo l'avvio, rientrare dopo crash, ed eventualmente diventare membri
votanti del gruppo Raft.

Il punto critico e' questo: **scoprire un broker via LAN o Directory non
puo' automaticamente cambiare il quorum Raft**. Se ogni nodo calcola il
quorum usando la propria vista locale dei peer scoperti, due nodi possono
vedere cluster diversi e prendere decisioni incompatibili. La membership
dinamica deve quindi essere governata da Raft stesso, tramite entry di
configurazione replicate nel log.

## Stato attuale

### Assunzioni statiche nel codice

Oggi il progetto assume che la membership votante sia nota all'avvio:

- `BrokerMain` in modalita' Raft richiede:

```text
raft <nodeId> <rpcPort> <votersCSV> [clientPort]
```

- `RaftConfig` contiene una `Map<Integer, RaftPeerEndpoint> voters`.
- `RaftConfig.getVoters()` e' la sorgente di verita' per membership e
  quorum.
- `RaftOrderingService` rifiuta l'avvio se il nodo locale non e' dentro
  `raftConfig.getVoters()`.
- `RaftElectionManager` riceve `allVotingNodeIds` nel costruttore,
  calcola `majority` una sola volta e mantiene `peerVotingNodeIds`
  immutabile.
- `RaftReplicationManager` fa la stessa cosa per replication state e
  commit majority.
- `RaftRpcClient` riceve una mappa immutabile di endpoint votanti.
- `PeerRegistry` scopre peer via LAN, ma non e' usato per cambiare
  quorum o membership Raft.
- `DirectoryService` registra broker vivi per aiutare i client, ma non
  partecipa al consenso.

### Assunzioni statiche nei documenti

I documenti attuali dichiarano esplicitamente:

- cluster membership statica per MVP;
- discovery solo come address resolution;
- dynamic membership rinviata a futura estensione.

Se il professore non ha mai autorizzato questa assunzione, questa parte
va corretta sia nel codice sia nella documentazione.

## Problema tecnico

In Raft, la membership non e' un dettaglio di discovery: determina chi
puo' votare e quanti voti servono per:

- eleggere un leader;
- committare entry;
- considerare sicura una configurazione;
- garantire che due leader diversi non possano essere eletti in modo
  incompatibile.

Con membership dinamica fatta male, il rischio e':

- nodo A pensa che il cluster sia `{A,B,C}`;
- nodo D entra e alcuni nodi pensano che il cluster sia `{A,B,C,D}`;
- quorum diversi vengono calcolati in modo diverso;
- due maggioranze possono non sovrapporsi;
- si puo' rompere la safety del log.

Quindi **non basta** aggiornare `PeerRegistry` o `RaftConfig.voters` a
runtime quando arriva un beacon LAN.

## Architettura target

La membership dinamica va trattata come stato replicato.

Servono tre concetti:

1. **Discovered peer**
   - broker visto su LAN o Directory;
   - non vota;
   - non conta nel quorum;
   - serve solo come endpoint candidato.

2. **Learner**
   - broker riconosciuto dal leader;
   - riceve log entries per catch-up;
   - non vota;
   - non conta nel quorum;
   - puo' diventare voter solo quando e' aggiornato.

3. **Voter**
   - broker dentro la configurazione Raft committata;
   - vota alle election;
   - conta nel quorum;
   - riceve log replication normale.

La sorgente di verita' diventa una `RaftClusterConfiguration` committata
nel log, non la CLI e non la discovery.

## Strategia Raft per cambiare membership

Ci sono due approcci principali.

### Opzione A - Joint consensus

E' il metodo Raft piu' classico e piu' sicuro per cambiare piu' nodi o
configurazioni arbitrarie.

Passi:

1. Configurazione corrente `C_old`.
2. Il leader propone una configurazione congiunta `C_old,new`.
3. Durante la fase congiunta, una entry e' committata solo se raggiunge
   maggioranza sia in `C_old` sia in `C_new`.
4. Quando `C_old,new` e' committata, il leader propone `C_new`.
5. Quando `C_new` e' committata, la nuova membership e' attiva.

Pro:

- sicura per cambi multipli;
- aderente al paper Raft;
- evita quorum non sovrapposti.

Contro:

- piu' lunga da implementare;
- impatta election, commit, replication e persistence;
- test piu' complessi.

### Opzione B - Single-server changes

Approccio piu' semplice: si aggiunge o rimuove **un solo voter alla
volta**. E' descritto come semplificazione pratica di Raft.

Regola:

- non si passa mai da `C` a una configurazione con piu' di un cambio;
- ogni cambio deve essere committato prima del successivo.

Esempio:

```text
{A,B,C} -> {A,B,C,D}
{A,B,C,D} -> {A,B,C,D,E}
```

Pro:

- molto piu' semplice;
- sufficiente per un progetto universitario;
- compatibile con "broker che si aggiungono uno alla volta".

Contro:

- meno generale;
- serve comunque trattare config come log entry;
- serve bloccare cambi concorrenti.

### Raccomandazione

Per questo progetto conviene implementare **single-server changes con
learner**:

1. nuovo broker entra come learner;
2. il leader lo porta in catch-up;
3. quando e' aggiornato, il leader propone una config entry che lo
   promuove a voter;
4. solo dopo commit della config entry il nuovo broker conta nel quorum.

E' il compromesso migliore tra correttezza, costo e difendibilita'.

## Nuovi modelli dati

### `RaftClusterConfiguration`

Package consigliato:

```text
it.polimi.ds.chat.ordering.raft.membership
```

Campi:

- `long configIndex`
  - indice log dell'entry che ha installato questa config;
- `long configTerm`
  - term dell'entry di config;
- `Set<Integer> voters`
  - broker votanti;
- `Set<Integer> learners`
  - broker non votanti in catch-up;
- `Map<Integer, RaftPeerEndpoint> endpoints`
  - endpoint noti per voters e learners;
- `boolean joint`
  - solo se si implementa joint consensus;
- `Set<Integer> oldVoters`
  - solo joint consensus;
- `Set<Integer> newVoters`
  - solo joint consensus.

### `RaftMembershipChangeCommand`

Nuovo tipo di comando nel log Raft.

Campi:

- `MembershipChangeType type`
  - `ADD_LEARNER`;
  - `PROMOTE_LEARNER_TO_VOTER`;
  - `REMOVE_VOTER`;
  - opzionale `JOINT_CONFIG`;
  - opzionale `FINAL_CONFIG`;
- `int brokerId`;
- `RaftPeerEndpoint endpoint`;
- `long requestedAtMillis`;

### `RaftLogEntry` e `ChatCommand`

Oggi `RaftLogEntry` contiene un `ChatCommand`.

Per supportare config entries, il log deve poter contenere piu' tipi di
comando:

```java
interface RaftCommand extends Serializable {}
```

Implementazioni:

- `ChatCommand implements RaftCommand`;
- `RaftMembershipChangeCommand implements RaftCommand`.

Poi `RaftLogEntry` deve contenere:

```java
RaftCommand command;
```

In alternativa rapida:

- aggiungere campo opzionale `membershipChangeCommand`;
- ma e' meno pulito.

## Modifiche ai file esistenti

### `RaftConfig`

Oggi e' una configurazione statica.

Refactor:

- rinominare concettualmente `voters` in `initialVoters`;
- chiarire che serve solo per bootstrap del primo cluster;
- aggiungere flag/config:
  - `dynamicMembershipEnabled`;
  - `bootstrapMode`;
  - `clusterId`;
  - opzionale `joinAddress` o `seedBrokers`.

Nuovo significato:

- il primo cluster parte con `initialVoters`;
- dopo il bootstrap, la membership corrente viene dal log Raft;
- i nodi che si uniscono non devono essere gia' in `initialVoters`.

### `BrokerMain`

La CLI attuale richiede `votersCSV`. Con membership dinamica servono due
modalita':

1. Bootstrap cluster:

```text
raft-bootstrap <nodeId> <rpcPort> <initialVotersCSV> [clientPort]
```

2. Join dinamico:

```text
raft-join <host> <rpcPort> <clientPort> <seedBrokerHost:seedPort>
```

Il nodo joiner non dovrebbe scegliere da solo un voter id definitivo.
Possibilita':

- usa un `brokerId` temporaneo negativo;
- chiede un id al leader;
- oppure usa un id configurato localmente ma viene accettato solo se il
  leader lo propone nel log.

Per semplicità:

- il joiner passa un `brokerId` richiesto;
- il leader rifiuta se gia' occupato;
- il broker diventa effettivo solo dopo config commit.

### `RaftOrderingService`

Oggi costruisce tutti i manager con `raftConfig.getVoters().keySet()`.

Refactor:

- introdurre un `RaftMembershipManager`;
- caricare la configurazione corrente da persistence/log;
- passare ai manager un provider dinamico invece di un set immutabile.

Esempio:

```java
RaftMembershipView membershipView;
```

Da usare per:

- current voters;
- current learners;
- majority;
- endpoint lookup;
- controllo se `localNodeId` e' voter o learner.

### `RaftElectionManager`

Oggi:

- `allVotingNodeIds` e' final;
- `peerVotingNodeIds` e' final;
- `majority` e' final.

Refactor:

- non tenere piu' majority finale;
- leggere la membership corrente quando parte una election;
- impedire a un learner di candidarsi;
- accettare voti solo da membri della config valida per quel term/log.

Modifiche:

- sostituire `Set<Integer> allVotingNodeIds` con
  `RaftMembershipView membershipView`;
- in `onElectionTimeoutFired()`:
  - se local node non e' voter, non partire come candidate;
  - snapshot dei voters correnti;
  - majority calcolata sullo snapshot;
  - inviare `RequestVote` solo ai voters dello snapshot.
- in `onRequestVoteResponse()`:
  - ignorare risposte da nodi non votanti nella config snapshot
    dell'election corrente.

Serve memorizzare nello stato election:

```java
private Set<Integer> currentElectionVoters;
private int currentElectionMajority;
```

### `RaftReplicationManager`

Oggi:

- peer voters finali;
- replication state solo per peer voters;
- majority finale.

Refactor:

- replication state deve includere voters e learners;
- learners ricevono entries ma non contano per commit;
- quando la config cambia:
  - aggiungere replication state per nuovi peer;
  - rimuovere o disattivare peer rimossi;
  - aggiornare majority;
  - se il leader non e' piu' voter nella nuova config, deve step-down.

Modifiche:

- usare `membershipView`;
- calcolare commit majority dalla config corrente;
- includere learners in `sendAppendEntries`, ma non nei match indexes
  usati per commit;
- aggiungere metodo:

```java
onMembershipChanged(RaftClusterConfiguration newConfig)
```

### `RaftCommitManager`

Oggi riceve:

```java
tryAdvanceCommitIndex(Collection<Long> matchIndexes, int majority, long currentTerm)
```

Questo puo' restare quasi invariato per single-server changes, se il
caller passa solo i match indexes dei voters correnti.

Per joint consensus invece serve estendere:

```java
tryAdvanceCommitIndex(
    Map<Integer, Long> matchIndexes,
    RaftClusterConfiguration config,
    long currentTerm
)
```

e verificare:

- majority in old voters;
- majority in new voters.

Per la raccomandazione single-server changes, basta mantenere il metodo
attuale ma aggiornare correttamente il `majority` nel replication manager.

### `RaftLog`

Il log deve accettare entry di configurazione, non solo chat command.

Modifiche:

- `RaftLogEntry` deve contenere `RaftCommand`;
- i metodi di append/truncate non cambiano concettualmente;
- quando una config entry viene committata, va applicata al
  `RaftMembershipManager`.

### `RaftStateMachineAdapter`

Oggi trasforma entry committate in `ChatDeliverMessage`.

Refactor:

- se l'entry contiene `ChatCommand`, comportamento attuale;
- se contiene `RaftMembershipChangeCommand`, non produrre messaggio chat;
- notificare/applicare cambio config al membership manager.

Possibile struttura:

```java
if (entry.getCommand() instanceof ChatCommand c) {
    deliverChat(c);
} else if (entry.getCommand() instanceof RaftMembershipChangeCommand m) {
    membershipManager.apply(m, entry.getIndex(), entry.getTerm());
}
```

### `RaftRpcClient`

Oggi ha mappa immutabile `voters`.

Refactor:

- endpoint lookup dinamico;
- non solo voters, anche learners;
- aggiornabile quando discovery o config cambia.

Opzioni:

1. `RaftRpcClient` riceve `RaftEndpointProvider`.
2. `RaftRpcClient` espone `updateEndpoints(...)`.

Meglio provider:

```java
interface RaftEndpointProvider {
    Optional<RaftPeerEndpoint> endpointFor(int brokerId);
}
```

### `RaftRpcServer`

Puo' restare quasi invariato.

Da aggiungere:

- gestione di messaggi join, se si decide di farli passare dallo stesso
  server RPC;
- filtro `clusterId`;
- rifiuto di RPC da broker sconosciuti se non sono join requests.

### `PeerRegistry`

Oggi ha `getQuorumSize()`, ma con dynamic membership questa cosa e'
pericolosa.

Refactor:

- rimuovere o deprecare `getQuorumSize()`;
- rinominare mentalmente in `DiscoveredPeerRegistry`;
- contiene solo endpoint/liveness osservata;
- non calcola mai majority Raft;
- notifica `RaftMembershipManager` o endpoint provider quando compaiono
  nuovi peer.

### `LanDiscoveryService`

Deve diventare discovery per join dinamico.

Nuovo payload HELLO:

```text
CHAT_DISCOVERY;HELLO;clusterId;brokerId;host;rpcPort;clientPort;role;membershipStatus
```

Dove `membershipStatus` puo' essere:

- `DISCOVERED`;
- `LEARNER`;
- `VOTER`.

Un broker scoperto non diventa voter automaticamente.

### `DirectoryService`

Oggi aiuta i client a trovare broker vivi.

Con membership dinamica puo' restare fuori dal consenso, ma deve
distinguere:

- broker scoperto ma non ancora membro;
- learner;
- voter;
- leader noto.

Per i client, idealmente la Directory restituisce:

- leader se noto;
- altrimenti un voter;
- evitare learner se non accetta client messages.

Messaggi nuovi possibili:

- `GetClusterMembersRequest`;
- `GetClusterMembersResponse`;
- `UpdateBrokerMembershipStatusMessage`.

## Join flow consigliato

### 1. Broker nuovo parte come joiner

Il broker avvia:

- RPC server;
- LAN discovery;
- storage locale vuoto;
- non partecipa alle election;
- non accetta client chat oppure accetta solo dopo promozione.

### 2. Joiner trova leader o seed broker

Possibili canali:

- LAN discovery;
- DirectoryService;
- seed broker passato via CLI.

Se trova un follower, il follower risponde con leader noto o inoltra la
richiesta.

### 3. Join request

Nuovo messaggio:

```java
BrokerJoinClusterRequest
```

Campi:

- requestedBrokerId;
- host;
- rpcPort;
- clientPort;
- storageEpoch o nodeUuid;
- lastLogIndex/lastLogTerm se e' un nodo che rientra.

### 4. Leader aggiunge come learner

Il leader:

- valida id e endpoint;
- aggiunge il nodo alla lista learners in memoria;
- inizia a replicare log al learner;
- non cambia quorum.

Possibile config entry:

- `ADD_LEARNER`.

Oppure per semplificare:

- learner e' stato volatile leader-side fino alla promozione.

Meglio persistente:

- `ADD_LEARNER` come entry di log, cosi' tutti sanno che il learner
  esiste.

### 5. Catch-up

Il leader invia `AppendEntries` al learner fino a quando:

```text
learner.matchIndex >= leader.lastLogIndex
```

o almeno fino all'indice corrente al momento della promozione.

### 6. Promozione a voter

Quando il learner e' aggiornato, il leader propone:

```text
PROMOTE_LEARNER_TO_VOTER(brokerId)
```

Questa entry deve essere committata usando la vecchia configurazione
votante.

Dopo commit:

- il nuovo broker vota;
- il nuovo broker conta nel quorum;
- tutti aggiornano `RaftClusterConfiguration`.

### 7. Client traffic

Solo dopo promozione:

- il broker puo' essere scelto dalla Directory per nuovi client;
- il broker puo' accettare client messages;
- se non leader, deve fare redirect/forward come gli altri.

## Removal flow

Serve anche rimuovere broker morti definitivamente, altrimenti il quorum
puo' diventare impossibile.

### Rimozione manuale consigliata

Per il progetto, evitare rimozione automatica solo per heartbeat mancati:
un crash temporaneo non deve far perdere membership.

Flow:

1. operatore chiede remove broker X;
2. leader propone `REMOVE_VOTER(X)`;
3. entry committata con la configurazione corrente;
4. dopo commit, X non conta piu' per quorum.

Se X ritorna, deve fare join da capo.

### Rimozione automatica

Possibile ma piu' rischiosa:

- richiede failure detector;
- puo' interagire male con crash/restart;
- anche se la spec esclude partizioni, un broker lento potrebbe essere
  rimosso inutilmente.

Raccomandazione: solo rimozione manuale o controllata.

## Persistence

Con membership dinamica bisogna persistere:

- log entries di configurazione;
- ultima configurazione applicata;
- node UUID stabile;
- brokerId assegnato;
- stato learner/voter locale;
- endpoint locale.

`FileRaftPersistence` oggi ha term/vote e log support, ma il log non e'
ancora cablato pienamente. Prima o insieme a membership dinamica va
risolto B1, altrimenti un nodo potrebbe dimenticare config entries dopo
restart.

Dipendenza forte:

```text
Dynamic membership richiede persistenza log funzionante.
```

Senza persistenza log, una config committata potrebbe sparire dopo crash,
rompendo quorum e safety.

## Impatto sulla safety

Regole da rispettare:

- una config diventa attiva solo quando la relativa log entry e'
  committata e applicata;
- un learner non vota mai;
- un nuovo voter viene promosso solo dopo catch-up;
- i quorum devono essere calcolati dalla config committata, non dalla
  discovery;
- non devono esserci due config changes concorrenti;
- se il leader viene rimosso dalla nuova config, deve fare step-down
  dopo aver committato/applicato la rimozione.

## Test necessari

### Unit test membership

Nuovi test:

- `RaftClusterConfigurationTest`
  - majority corretta;
  - voters/learners separati;
  - endpoint lookup;
  - config index monotono.

- `RaftMembershipManagerTest`
  - apply `ADD_LEARNER`;
  - apply `PROMOTE_LEARNER_TO_VOTER`;
  - apply `REMOVE_VOTER`;
  - rifiuta promozione di learner non esistente;
  - rifiuta due cambi concorrenti.

### Election tests

Aggiornare `RaftElectionManagerTest`:

- learner non parte come candidate;
- learner non conta come voter;
- nuovo voter conta solo dopo config commit;
- vote response da nodo rimosso viene ignorata.

### Replication tests

Aggiornare `RaftReplicationManagerTest`:

- leader replica anche ai learners;
- learner matchIndex non conta per commit;
- dopo promozione, matchIndex del nuovo voter conta;
- rimozione voter aggiorna majority.

### Integration tests

Nuovi test:

- cluster 3 nodi -> join quarto broker come learner -> catch-up ->
  promote -> nuovo quorum 3 su 4;
- rimozione broker da 4 a 3 -> quorum torna 2;
- restart dopo config change -> config recuperata da log;
- leader rimosso -> step-down;
- nuovo broker non riceve chat history vecchia come client, ma riceve
  log Raft necessario come replica broker.

## Roadmap consigliata

### Fase 0 - Prerequisito obbligatorio

- Completare B1: persistenza log Raft.
- Senza log persistito, dynamic membership non e' sicura.

### Fase 1 - Separare discovery da membership

- Rinominare/ridefinire `PeerRegistry` come discovery-only.
- Rimuovere o deprecare `PeerRegistry.getQuorumSize()`.
- Documentare che discovery non cambia quorum.
- Aggiungere endpoint provider dinamico per transport.

### Fase 2 - Introdurre `RaftClusterConfiguration`

- Creare modello config;
- creare `RaftMembershipManager`;
- inizializzare da `initialVoters`;
- expose `membershipView` ai manager.

### Fase 3 - Rendere election/replication membership-aware

- `RaftElectionManager` usa membership snapshot per ogni election.
- `RaftReplicationManager` aggiorna voters/learners dinamicamente.
- `RaftCommitManager` riceve majority aggiornata.

### Fase 4 - Config entries nel log

- introdurre `RaftCommand`;
- convertire `ChatCommand`;
- aggiungere `RaftMembershipChangeCommand`;
- applicare config entries senza generare `ChatDeliverMessage`.

### Fase 5 - Join come learner

- aggiungere `BrokerJoinClusterRequest/Response`;
- nuovo broker parte learner;
- leader replica log al learner;
- learner non vota.

### Fase 6 - Promozione e rimozione

- implementare `PROMOTE_LEARNER_TO_VOTER`;
- implementare `REMOVE_VOTER`;
- bloccare cambi concorrenti;
- aggiornare Directory per non assegnare client ai learner.

### Fase 7 - Test e demo

- integration test join quarto broker;
- integration test rimozione broker;
- restart dopo config change;
- demo controllata:
  - avvio 3 broker;
  - aggiungo quarto broker;
  - mostro catch-up/promozione;
  - invio messaggi;
  - rimuovo un broker.

## Impatto sui ruoli

### Person A

Coinvolta in:

- election con membership dinamica;
- learner che non puo' candidarsi;
- vote responses filtrate dalla config corrente;
- leader step-down se rimosso;
- test election dopo config change.

### Person B

Coinvolta in:

- replication verso learner;
- matchIndex per voters vs learners;
- commit majority dinamica;
- config entries nel log;
- catch-up.

### Person C

Coinvolta in:

- transport endpoint dinamico;
- `RaftOrderingService`;
- `BrokerMain`;
- Directory/LAN discovery;
- join protocol;
- integration tests.

## Rischi principali

- Refactor ampio: tocca election, replication, commit, log, persistence,
  transport e broker startup.
- Senza persistenza log completa e' pericoloso.
- Joint consensus completo e' lungo.
- Anche single-server changes richiede test accurati.
- Directory/LAN discovery non devono essere confuse con membership
  votante.
- Se la demo ha poco tempo, dynamic membership puo' introdurre instabilita'
  maggiore rispetto al beneficio.

## Raccomandazione finale

Se bisogna davvero abbandonare l'assunzione di cluster statico, la
soluzione corretta non e' "aggiornare la lista peer quando arriva un
HELLO". La soluzione corretta e':

1. mantenere discovery come meccanismo per trovare broker;
2. introdurre learners;
3. rappresentare i cambi membership come entry Raft;
4. promuovere un broker a voter solo dopo catch-up;
5. calcolare quorum solo dalla configurazione Raft committata.

Per il progetto, consiglierei **single-server changes con learner**,
non joint consensus completo, a meno che il professore chieda
esplicitamente cambi multipli arbitrari.

Frase difendibile:

> I broker possono essere scoperti dinamicamente, ma non diventano
> automaticamente votanti. Entrano prima come learner, recuperano il log,
> e solo una entry di configurazione committata da Raft li promuove a
> voter. In questo modo la membership e il quorum cambiano dinamicamente
> senza perdere le garanzie di safety.

