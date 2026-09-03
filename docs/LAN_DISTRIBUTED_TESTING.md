# Distributed LAN Testing

The repository already contains the automated regression suite and a real-socket
application integration test. It also has a multi-process smoke test, but that
smoke test uses `LOCAL_TCP` and loopback. It does not prove that UDP broadcast
works between physical machines.

This runbook is the missing cross-host test layer. It uses the production
`HYBRID` transport and the existing `DirectoryService`, `BrokerMain`, and
`ClientMain` entry points.

## 1. Topology

Use one Directory host and three broker voters. One broker per machine is less
ambiguous for UDP broadcast.

Example with the desktop as Directory host:

```text
desktop  192.168.1.10  Directory :60000/:60001, broker 0 :7000/:51000
laptop   192.168.1.11  broker 1 :7001/:51001
server   192.168.1.12  broker 2 :7002/:51002
```

Replace these examples with the actual LAN addresses and use the same voter CSV,
UDP port, and cluster ID on every machine:

```text
0@192.168.1.10:7000:51000,1@192.168.1.11:7001:51001,2@192.168.1.12:7002:51002
```

Build the same commit and JDK major version on all three machines:

```text
mvn clean package -DskipTests
```

On Ubuntu, make the launcher executable once after checkout:

```bash
chmod +x scripts/lan/start-lan-node.sh
```

Before starting Java, verify reachability. On Windows use
`Test-NetConnection HOST -Port PORT`; on Ubuntu use `nc -vz HOST PORT`.

## 2. Firewall

Allow inbound traffic on every relevant host:

| Protocol | Port | Purpose |
|---|---:|---|
| TCP | 60000 | Directory broker bootstrap/registration |
| TCP | 60001 | Directory client lookup |
| TCP | 7000-7002 | Raft RPC, according to voter ID |
| TCP | 51000-51002 | Client connections, according to voter ID |
| UDP | 7100 | Raft RequestVote and empty heartbeat broadcast |

Disable AP/client isolation for the test network. Do not use `localhost` or
`127.0.0.1` in the voter CSV. The Directory host must be reachable from every
broker and client.

## 3. Start the processes

Run the Directory on the desktop. Windows:

```powershell
.\scripts\lan\Start-LanNode.ps1 -Role directory `
  -Voters "0@192.168.1.10:7000:51000,1@192.168.1.11:7001:51001,2@192.168.1.12:7002:51002"
```

Ubuntu:

```bash
./scripts/lan/start-lan-node.sh --role directory \
  --voters '0@192.168.1.10:7000:51000,1@192.168.1.11:7001:51001,2@192.168.1.12:7002:51002'
```

Start one `HYBRID` broker on each host. The `DirectoryHost` is the desktop IP:

```powershell
.\scripts\lan\Start-LanNode.ps1 -Role broker -NodeId 0 -RpcPort 7000 -ClientPort 51000 `
  -DirectoryHost 192.168.1.10 -Voters "0@192.168.1.10:7000:51000,1@192.168.1.11:7001:51001,2@192.168.1.12:7002:51002"
```

```bash
./scripts/lan/start-lan-node.sh --role broker --node-id 1 --rpc-port 7001 --client-port 51001 \
  --directory-host 192.168.1.10 \
  --voters '0@192.168.1.10:7000:51000,1@192.168.1.11:7001:51001,2@192.168.1.12:7002:51002'
```

Use the same command for node 2 with ports `7002` and `51002`.

Start at least three clients, preferably one on each machine. The client opens
an interactive console; enter a distinct username, then type chat text:

```powershell
.\scripts\lan\Start-LanNode.ps1 -Role client -DirectoryHost 192.168.1.10
```

```bash
./scripts/lan/start-lan-node.sh --role client --directory-host 192.168.1.10
```

## 4. Test scenarios and pass criteria

Run the normal automated suite first:

```text
mvn clean test
```

Then execute these scenarios manually while saving each process output to a
separate log file:

1. **Basic replication:** send four messages from clients connected to different
   brokers. Every other connected client must receive each message exactly once,
   in the same order. The sender receives an ACK and must not receive an echo.
2. **Causal order:** client A sends `m1`; after client B observes `m1`, B sends
   `m2`. Every observer must display `m1` before `m2`.
3. **Leader failure:** identify the leader in broker output, terminate only
   that broker, wait for a new leader among the remaining two, reconnect the
   affected client, and send another message. The cluster must continue with no
   duplicate delivery.
4. **Broadcast proof:** record that RequestVote and empty heartbeat traffic is
   received across hosts on UDP `7100`; log-bearing AppendEntries and forwarded
   proposals must succeed over the configured TCP RPC ports.

Record the date, commit hash, host/IP topology, voter CSV, cluster ID, firewall
rules, process logs, and (for the broadcast proof) a short packet capture or
equivalent OS/network log. A same-host `LOCAL_TCP` run is useful regression
evidence but cannot mark the LAN test passed.

## 5. Result interpretation

This procedure complements, rather than replaces, JUnit. JUnit covers deterministic
logic and same-process socket integration; this runbook covers real interfaces,
firewalls, routing, UDP broadcast, and cross-host client delivery. A failure in
the LAN run should first be classified as endpoint, firewall, broadcast, or TCP
reachability before changing Raft code.