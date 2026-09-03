#!/usr/bin/env bash
set -Eeuo pipefail

role=""
voters=""
directory_host=""
directory_broker_port=60000
directory_client_port=60001
node_id=""
rpc_port=""
client_port=""
broadcast_port=7100
cluster_id="lan-demo"
udp_max_payload=1300

usage() {
    echo "Usage: $0 --role directory|broker|client [options]"
    echo "  --voters id@host:rpc[:client],..."
    echo "  --directory-host HOST --node-id ID --rpc-port PORT --client-port PORT"
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --role) role="$2"; shift 2 ;;
        --voters) voters="$2"; shift 2 ;;
        --directory-host) directory_host="$2"; shift 2 ;;
        --directory-broker-port) directory_broker_port="$2"; shift 2 ;;
        --directory-client-port) directory_client_port="$2"; shift 2 ;;
        --node-id) node_id="$2"; shift 2 ;;
        --rpc-port) rpc_port="$2"; shift 2 ;;
        --client-port) client_port="$2"; shift 2 ;;
        --broadcast-port) broadcast_port="$2"; shift 2 ;;
        --cluster-id) cluster_id="$2"; shift 2 ;;
        --udp-max-payload) udp_max_payload="$2"; shift 2 ;;
        *) usage ;;
    esac
done

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
classes="$project_root/target/classes"
[[ -d "$classes" ]] || { echo "Missing target/classes. Run 'mvn clean package -DskipTests' first." >&2; exit 1; }

case "$role" in
    directory)
        [[ -n "$voters" ]] || usage
        exec java -cp "$classes" it.polimi.ds.chat.directory.DirectoryService \
            "$voters" "$directory_broker_port" "$directory_client_port"
        ;;
    broker)
        [[ -n "$voters" && -n "$directory_host" && -n "$node_id" && -n "$rpc_port" && -n "$client_port" ]] || usage
        exec java -cp "$classes" it.polimi.ds.chat.broker.core.BrokerMain \
            raft "$node_id" "$rpc_port" "$client_port" "$broadcast_port" "$cluster_id" \
            "$udp_max_payload" "$directory_host" "$directory_broker_port"
        ;;
    client)
        [[ -n "$directory_host" ]] || usage
        exec java -cp "$classes" it.polimi.ds.chat.client.ClientMain \
            "$directory_host" "$directory_client_port"
        ;;
    *) usage ;;
esac