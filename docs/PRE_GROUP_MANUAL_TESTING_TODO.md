# Pre-group Manual Testing and Presentation Checklist

Aggiornato al 2026-08-11 in base alla specifica ufficiale.

Obiettivo: produrre evidenza che il progetto funzioni come sistema realmente
distribuito. I test localhost sono preparatori; la demo valida richiede almeno due
notebook degli studenti collegati alla stessa LAN wired o wireless.

---

## 1. Gate prima del test manuale

Non iniziare la prova finale finche' questi punti non sono chiusi:

- [ ] sequenza applicativa corretta in presenza delle entry Raft no-op e al restart;
- [ ] host/porte della Directory configurabili nei broker, senza `localhost`
  hard-coded per il deployment distribuito;
- [ ] writer unico o lock condiviso per tutte le scritture sullo stesso
  `ObjectOutputStream` client;
- [ ] stato causale aggiornato prima della delivery osservabile dai client;
- [ ] test automatico end-to-end del percorso client/broker/hold-back queue;
- [ ] decisione documentata sul log Raft persistente che contiene payload chat;
- [ ] `mvn test` verde.

Stato automatico rilevato il 2026-08-11:

```text
Tests run: 186, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Questo risultato non chiude i gate: la suite corrente non riproduce la demo completa
su due notebook e non intercetta il gap causato dal no-op nel livello applicativo.

---

## 2. Topologia minima conforme

Usare IP LAN reali e stabili per tutta la prova. Esempio da sostituire con gli IP
effettivi:

| Notebook | IP di esempio | Processi |
| --- | --- | --- |
| A | `192.168.1.10` | DirectoryService, broker 0, broker 1, client A |
| B | `192.168.1.11` | broker 2, client B |

Una distribuzione 2+1 dei tre broker e' sufficiente a mostrare processi su due host.
Per una demo piu' chiara, se e' disponibile un terzo notebook, eseguire un broker per
host. In ogni caso almeno un canale broker-broker e almeno un canale client-broker
devono attraversare davvero la LAN.

Voter set di esempio:

```text
0@192.168.1.10:7000:50000,1@192.168.1.10:7001:50001,2@192.168.1.11:7002:50002
```

Attenzione: nell'implementazione corrente la Directory usa le porte fisse `60000`
per broker/cluster e `60001` per client. Se broker 1 espone la chat su `50001`, non
c'e' conflitto con la Directory. Verificare comunque tutte le porte prima della demo.

---

## 3. Preparazione della LAN

- [ ] Disabilitare VPN e interfacce virtuali non necessarie, oppure verificare quale
  interfaccia viene usata dal broadcast.
- [ ] Verificare che i notebook siano sulla stessa subnet e che l'access point non
  abiliti client isolation.
- [ ] Fare ping tra gli IP LAN, se ICMP e' consentito.
- [ ] Aprire nel firewall Java e le porte TCP/UDP necessarie solo sulla rete privata
  della demo.
- [ ] Verificare TCP tra host sulle porte Raft `7000-7002`, chat `50000-50002` e
  Directory `60000-60001`.
- [ ] Verificare UDP sulla porta broadcast comune `7100`.
- [ ] Usare lo stesso `clusterId`, la stessa porta broadcast e lo stesso voter set per
  tutti i broker.
- [ ] Sincronizzare lo stesso commit/build su tutti i notebook.
- [ ] Decidere se la prova parte con directory `raft-data` vuote oppure testa recovery
  da stato noto. Non mescolare i due casi.

---

## 4. Build e avvio

Build su ogni notebook:

```powershell
mvn -q -DskipTests package
```

Il jar non espone attualmente un `Main-Class`; usare `target/classes`.

### 4.1 Smoke test locale, non valido come demo finale

Il seguente schema resta utile per diagnosticare il software su un solo host:

```powershell
java -cp target/classes it.polimi.ds.chat.directory.DirectoryService "0@127.0.0.1:7000:50000,1@127.0.0.1:7001:50001,2@127.0.0.1:7002:50002"
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 0 7000 50000 7100 demo-cluster 1400
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 1 7001 50001 7100 demo-cluster 1400
java -cp target/classes it.polimi.ds.chat.broker.core.BrokerMain raft 2 7002 50002 7100 demo-cluster 1400
java -cp target/classes it.polimi.ds.chat.client.ClientMain 127.0.0.1 60001
```

Non presentarlo come soddisfacimento della regola dei due notebook.

### 4.2 Avvio distribuito target

Questi comandi sono il runbook obiettivo dopo aver reso configurabile la Directory
nei broker. La sintassi esatta dei nuovi argomenti deve essere aggiornata qui insieme
al relativo fix; non inventare parametri non ancora supportati dal codice.

Notebook A, Directory:

```powershell
java -cp target/classes it.polimi.ds.chat.directory.DirectoryService "0@192.168.1.10:7000:50000,1@192.168.1.10:7001:50001,2@192.168.1.11:7002:50002"
```

Notebook A, broker 0 e 1; notebook B, broker 2:

```text
BrokerMain raft <nodeId> <rpcPort> <clientPort> 7100 demo-cluster 1400 <directoryHost> <directoryBrokerPort>
```

La riga precedente e' intenzionalmente un contratto target: oggi `BrokerMain` non
accetta ancora gli ultimi due argomenti. Dopo il fix, sostituirla con i tre comandi
reali e verificati.

Client su entrambi i notebook:

```powershell
java -cp target/classes it.polimi.ds.chat.client.ClientMain 192.168.1.10 60001
```

`ClientMain` supporta gia' host e porta della Directory da riga di comando.

---

## 5. Scenari obbligatori

Per ogni scenario salvare log con timestamp e annotare PASS/FAIL. Confrontare la
sequenza dei soli messaggi chat globali, escludendo JOIN/QUIT locali.

### Scenario A - Avvio distribuito e uso effettivo della LAN

1. Avviare Directory e tre broker secondo la topologia.
2. Verificare un solo leader e due follower stabili.
3. Acquisire log che mostrino `RequestVote`/heartbeat sul transport UDP broadcast.
4. Confermare che almeno un follower sia sull'altro notebook.

PASS se il cluster si forma tramite IP LAN, il broadcast attraversa la LAN e non ci
sono election continue.

### Scenario B - Client su broker diversi, stesso ordine

1. Avviare almeno due client sui due notebook.
2. Assicurarsi che siano connessi a broker diversi; se necessario bilanciare il
   numero di client o documentare la selezione Directory.
3. Inviare messaggi alternati e raffiche concorrenti.
4. Registrare per ogni client la lista `(seq, sender, text)`.

PASS se tutti i destinatari connessi condividono lo stesso ordine relativo, senza
duplicati e senza gap permanenti. Il mittente puo' non ricevere l'echo del proprio
messaggio.

### Scenario C - Catena causale cross-broker

1. Client A invia `m1` tramite broker 0.
2. Dopo aver ricevuto `m1`, client B collegato a broker 2 invia immediatamente `m2`.
3. Un terzo destinatario, oppure i log di delivery dei due broker, osserva entrambe.

PASS se nessun destinatario osserva `m2` prima di `m1` e i metadata mostrano la
dipendenza causale.

### Scenario D - Proposta attraverso un follower

1. Collegare un client a un follower.
2. Inviare un messaggio.

PASS se il follower inoltra al leader, il comando viene committato e l'ACK arriva
solo dopo commit.

### Scenario E - Crash del leader senza partition

1. Terminare il processo leader, lasciando connessi gli altri due broker.
2. Attendere la nuova elezione.
3. Inviare nuovi messaggi.

PASS se viene eletto un solo nuovo leader, i due voter rimasti formano la maggioranza
e i messaggi successivi sono consegnati nello stesso ordine. Verificare esplicitamente
che la no-op del nuovo term non blocchi la sequenza chat.

### Scenario F - Restart e catch-up

1. Riavviare il broker fermato con stesso id, endpoint e storage directory.
2. Inviare nuovi messaggi.

PASS se rientra senza violare l'ordine, recupera il log e la delivery applicativa non
attende indici gia' applicati o entry no-op.

### Scenario G - Client failure e connected-only delivery

1. Disconnettere completamente un client.
2. Inviare `offline-1` e `offline-2` mentre e' assente.
3. Riconnetterlo e inviare `online-1`.

PASS se il client non riceve `offline-1`/`offline-2` come history e riceve solo il
traffico successivo alla nuova connessione.

### Scenario H - Failure transitorio di un link, senza partition

Introdurre una perdita o interruzione breve che non separi stabilmente il cluster in
due componenti. Ripristinare la connettivita' e osservare retry/catch-up.

PASS se, dopo il ripristino, il cluster converge e continua senza ordini divergenti.
Non usare questo scenario per dichiarare partition tolerance.

### Scenario I - Directory e reconnect

1. Far fallire il broker di un client.
2. Attendere che la Directory lo rimuova.
3. Verificare riconnessione a un altro broker e retry delle richieste pendenti.
4. Ripetere facendo fallire il primo tentativo di reconnect.

PASS se il client continua a ritentare con backoff e non resta permanentemente senza
heartbeat manager. Questo richiede il fix indicato in `CODE_ISSUES_BY_GROUP.md`.

---

## 6. Evidenze da conservare

- [ ] commit hash e output completo di `mvn test`;
- [ ] tabella notebook/IP/processi/porte;
- [ ] log dell'elezione e del cambio leader;
- [ ] log o packet capture minima che provi UDP broadcast tra notebook;
- [ ] sequenze di delivery confrontabili per almeno due client su broker diversi;
- [ ] risultato della catena causale;
- [ ] risultato della disconnessione senza history;
- [ ] risultato di crash, reconnect e restart;
- [ ] elenco dei limiti osservati.

---

## 7. Slide richieste

Preparare poche slide, preferibilmente 6-8:

1. requisiti ufficiali e assunzioni;
2. software architecture;
3. run-time architecture sui notebook della demo;
4. Raft, total order e causal-order argument;
5. tabella broadcast/unicast e motivazioni;
6. failure handling e connected-only delivery;
7. evidenze dei test;
8. limiti e interpretazione concordata dello storage Raft.

Nelle slide non confondere:

- log order con prova completa della delivery end-to-end;
- discovery LAN con membership Raft;
- log Raft tecnico con una feature di history;
- link failure transitorio con network partition;
- smoke test localhost con demo distribuita su almeno due notebook.
