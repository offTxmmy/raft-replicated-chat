package it.polimi.ds.chat.ordering.raft.config;

/**
 * Selects how broker-to-broker Raft RPCs are transported.
 */
public enum RaftTransportMode {
    /**
     * LAN-aware behavior: small broadcast-friendly Raft messages travel over
     * UDP broadcast, while payload-bearing replication remains TCP.
     */
    HYBRID
}
