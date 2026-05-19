# Revisione di conformita alla specifica del professore

Aggiornato al 2026-05-19 dopo la rimozione delle modalita legacy.

## Executive summary

Il progetto ora espone una sola architettura di ordering: Raft.

- `BrokerMain` avvia solo broker Raft.
- `BrokerConfig` contiene solo configurazione Raft.
- `OrderingMode`, `SequencerOrderingService`, `HandlerState` e i messaggi
  broker-broker dedicati al sequencer sono stati rimossi.
- Il trasporto Raft e' sempre `HYBRID`: `RequestVote` e heartbeat vuoti
  usano UDP LAN broadcast; gli `AppendEntries` con entry reali restano su
  TCP per affidabilita e payload potenzialmente grande.

Questa impostazione e' piu coerente con la specifica: niente single point
of failure da sequencer centralizzato e uso esplicito della LAN broadcast
nel consenso Raft.

## Conformita ai punti principali

- **Broker sulla stessa LAN:** soddisfatto tramite voter set statico e LAN
  discovery.
- **Uso della LAN broadcast:** soddisfatto per discovery, `RequestVote` e
  heartbeat Raft vuoti.
- **Client su Internet connessi a un broker:** soddisfatto tramite TCP
  client-broker.
- **Ordine totale:** soddisfatto dall'indice del log Raft.
- **Ordine causale:** preservato dal total order Raft e verificato dalla
  hold-back queue/vector clock lato broker/client.
- **Crash-recovery:** term/vote/log Raft sono persistiti.
- **Dynamic membership:** non implementata, deliberatamente fuori scope;
  il quorum resta basato sul voter set statico.

## Scelta di trasporto

La scelta finale e':

- UDP LAN broadcast per messaggi piccoli e destinati a tutti i voter:
  `RequestVote` e heartbeat `AppendEntries` vuoti.
- TCP per `AppendEntries` con entry di log, per evitare frammentazione UDP,
  ACK/NACK applicativi e retry selettivo.

Risposta sintetica per l'orale:

> Usiamo broadcast dove e' naturale e conveniente: election e leadership
> activity. Manteniamo TCP per la replica payload perche' e' il punto in
> cui affidabilita, dimensione e backtracking sono critici.

## Test correnti

La suite Maven passa con test unitari e integrazione su:

- election, vote handling, term/role transitions;
- log, persistence, commit, replication;
- RPC TCP e trasporto ibrido;
- cluster Raft a tre nodi, inclusa modalita HYBRID;
- deduplicazione retry client su leader e via follower-forward;
- vector clock e hold-back queue.

## Rischi residui

- Manca ancora un test end-to-end completo con `Broker` reali + client reali
  + failover sotto carico.
- La `DirectoryService` resta un helper di bootstrap/load balancing, non fa
  parte del consenso. Va dichiarata come dipendenza soft.
- La deduplicazione client e' ricostruita dalla vita del servizio, ma va
  rivalutata in scenari di retry client dopo restart completo del cluster.
