# Distributed LAN Testing

The repository already contains the automated regression suite and a real-socket
application integration test. It also has a multi-process smoke test, but that
smoke test uses `LOCAL_TCP` and loopback. It does not prove that UDP broadcast
works between physical machines.

This runbook is the missing cross-host test layer. It uses the production
`HYBRID` transport and the existing `DirectoryService`, `BrokerMain`, and
`ClientMain` entry points. Follow the sections in order: the JUnit suite proves
the software locally, while this procedure proves the real interfaces,
firewalls, routing, and UDP broadcast between machines.

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

## 2. Prepare the machines

Install the same JDK major version, Maven, and Git on the desktop, laptop, and
Ubuntu server. Checkout the same commit on every machine:

```text
git clone <repository-url>
cd DS-Project2025-2026
git checkout <commit-or-branch>
mvn clean package -DskipTests
```

On Windows, find the LAN address with:

```powershell
ipconfig
```

On Ubuntu, use:

```bash
hostname -I
```

Replace every `192.168.1.x` value in this document with the actual addresses.
Do not use `localhost` or `127.0.0.1` in the voter CSV. On Ubuntu, make the
launcher executable once after checkout:

```bash
chmod +x scripts/lan/start-lan-node.sh
```

Before starting Java, verify reachability. On Windows use
`Test-NetConnection HOST -Port PORT`; on Ubuntu use `nc -vz HOST PORT`.

## 3. Firewall and network

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

On Windows, run PowerShell as Administrator on the desktop and laptop:

```powershell
New-NetFirewallRule -DisplayName "DS Raft TCP" `
  -Direction Inbound -Protocol TCP `
  -LocalPort 60000,60001,7000,7001,7002,51000,51001,51002 `
  -Action Allow

New-NetFirewallRule -DisplayName "DS Raft UDP" `
  -Direction Inbound -Protocol UDP -LocalPort 7100 -Action Allow
```

On Ubuntu:

```bash
sudo ufw allow 60000/tcp
sudo ufw allow 60001/tcp
sudo ufw allow 7002/tcp
sudo ufw allow 51002/tcp
sudo ufw allow 7100/udp
sudo ufw reload
sudo ufw status
```

The wireless access point must not enable client/AP isolation. If the network
has multiple interfaces or a VPN, use the physical LAN address and later confirm
that UDP traffic is visible on the expected interface.

Before starting brokers, test TCP reachability. From the laptop:

```powershell
Test-NetConnection 192.168.1.10 -Port 60000
Test-NetConnection 192.168.1.10 -Port 7000
```

From Ubuntu:

```bash
nc -vz 192.168.1.10 60000
nc -vz 192.168.1.10 7000
nc -vz 192.168.1.11 7001
```

The checks for broker and client ports can be repeated after those processes
start. A failed TCP check is a network or firewall problem, not a Raft election
problem.

## 4. Start the processes

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

Use distinct usernames such as `alice`, `bob`, and `charlie`. Keep one terminal
per process open so that election, registration, ACK, and delivery output remains
visible. Save the output of each process to a separate file or capture it with
the terminal logging facilities of the operating system.

## 5. Test scenarios and pass criteria

Run the normal automated suite first:

```text
mvn clean test
```

The command must finish successfully with zero failures and zero errors before
the physical LAN run is considered meaningful. This command is run once on any
machine with the source checkout; it does not replace the cross-host test.

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

On Ubuntu, observe the broadcast while the brokers are running:

```bash
sudo tcpdump -ni any udp port 7100
```

The expected result is traffic for RequestVote and empty heartbeats. AppendEntries
containing log entries and forwarded proposals are expected on TCP, not UDP.

For a leader-failure test, identify the leader from the broker output and stop
only that broker with `Ctrl+C` on Windows or `kill <pid>` on Ubuntu. Wait for the
remaining two brokers to elect a leader, reconnect the affected client, and send
another message. Do not stop two brokers at once: a three-node Raft cluster needs
two live voters for a quorum.

Record the date, commit hash, host/IP topology, voter CSV, cluster ID, firewall
rules, process logs, and (for the broadcast proof) a short packet capture or
equivalent OS/network log. A same-host `LOCAL_TCP` run is useful regression
evidence but cannot mark the LAN test passed.

## 6. Cleanup and troubleshooting

After the scenarios, close the clients, then brokers, then the Directory. Remove
the temporary `raft-data` directories before repeating a fresh-cluster election:

```text
rm -rf raft-data
```

On Windows, use `Remove-Item -Recurse -Force raft-data`. Do not delete it when
specifically testing restart recovery.

If a broker cannot start, check that its RPC and client ports are unused and that
its voter CSV entry matches its `nodeId`. If the Directory is unreachable, check
TCP `60000` and that every broker uses the desktop LAN IP. If there is no leader,
check TCP RPC connectivity between all broker pairs, UDP `7100`, the cluster ID,
and firewall/AP isolation. If clients do not connect, check TCP `60001` and the
broker client ports advertised in the voter CSV.

## 7. Result interpretation

This procedure complements, rather than replaces, JUnit. JUnit covers deterministic
logic and same-process socket integration; this runbook covers real interfaces,
firewalls, routing, UDP broadcast, and cross-host client delivery. A failure in
the LAN run should first be classified as endpoint, firewall, broadcast, or TCP
reachability before changing Raft code.