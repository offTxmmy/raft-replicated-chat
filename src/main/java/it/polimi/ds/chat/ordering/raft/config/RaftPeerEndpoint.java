package it.polimi.ds.chat.ordering.raft.config;

import java.io.Serializable;
import java.util.Objects;

/**
 * Addressing record for one Raft voting peer.
 *
 * <p>Used by {@link RaftConfig} to declare the static voter set: each entry maps
 * a Raft {@code brokerId} to the host/port pair where its RPC server listens.
 *
 * <p>This record represents <strong>addressing</strong> only, not membership.
 * Membership (which ids are voters) is the set of keys of
 * {@link RaftConfig#getVoters()} and is fixed at startup (Contract A).
 */
public record RaftPeerEndpoint(int brokerId, String host, int rpcPort, int clientPort)
        implements Serializable {

    public RaftPeerEndpoint {
        if (brokerId < 0) {
            throw new IllegalArgumentException("brokerId must be >= 0, got " + brokerId);
        }
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must be non-blank");
        }
        if (rpcPort < 1 || rpcPort > 65535) {
            throw new IllegalArgumentException("rpcPort out of range: " + rpcPort);
        }
        if (clientPort <= 0 || clientPort > 65535) {
            throw new IllegalArgumentException("clientPort must be in range 1..65535, got " + clientPort);
        }
    }
}
