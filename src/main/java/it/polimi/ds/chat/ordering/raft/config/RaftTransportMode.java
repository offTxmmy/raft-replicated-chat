package it.polimi.ds.chat.ordering.raft.config;

/**
 * Selects how broker-to-broker Raft RPCs are transported.
 */
public enum RaftTransportMode {
    /**
     * LAN-aware behavior: small broadcast-friendly Raft messages travel over
     * UDP broadcast, while payload-bearing replication remains TCP.
     */
    HYBRID,

    /**
     * Local development behavior: every Raft RPC is sent via TCP unicast to
     * the endpoints in the static voter set. This avoids sharing one UDP
     * broadcast port among multiple broker processes on the same host.
     */
    LOCAL_TCP
}
