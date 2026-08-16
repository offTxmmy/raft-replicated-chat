package it.polimi.ds.chat.ordering.raft.config;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable Raft-specific configuration block, owned by the broker configuration
 * when Raft ordering is selected.
 *
 * <p>The {@link #getVoters() voter map} is the <strong>source of truth</strong>
 * for cluster membership and quorum (Contract A). It must include every voting
 * broker — including the local one — and must be identical on every node of
 * the same cluster. Runtime reachability information must NOT alter it.
 *
 * <p>Timeouts follow the standard Raft convention:
 * <ul>
 *   <li>{@code electionTimeoutMinMs} / {@code electionTimeoutMaxMs} define the
 *       randomized window used by {@link
 *       it.polimi.ds.chat.ordering.raft.RaftElectionManager} (Contract D).
 *   <li>{@code heartbeatIntervalMs} should be comfortably smaller than
 *       {@code electionTimeoutMinMs} (typically a factor of 5–10).
 * </ul>
 */
public final class RaftConfig {

    public static final RaftTransportMode DEFAULT_TRANSPORT_MODE = RaftTransportMode.HYBRID;
    public static final int DEFAULT_RAFT_BROADCAST_PORT = 7100;
    public static final int DEFAULT_UDP_MAX_PAYLOAD_BYTES = 1400;
    public static final String DEFAULT_CLUSTER_ID = "default-raft-cluster";

    private final long electionTimeoutMinMs;
    private final long electionTimeoutMaxMs;
    private final long heartbeatIntervalMs;
    private final int rpcPort;
    private final RaftTransportMode transportMode;
    private final int raftBroadcastPort;
    private final int udpMaxPayloadBytes;
    private final String clusterId;
    private final Path storageDir;
    private final Map<Integer, RaftPeerEndpoint> voters;

    public RaftConfig(long electionTimeoutMinMs,
                  long electionTimeoutMaxMs,
                  long heartbeatIntervalMs,
                  int rpcPort,
                  Path storageDir,
                  Map<Integer, RaftPeerEndpoint> voters) {
        this(electionTimeoutMinMs,
                electionTimeoutMaxMs,
                heartbeatIntervalMs,
                rpcPort,
                DEFAULT_TRANSPORT_MODE,
                DEFAULT_RAFT_BROADCAST_PORT,
                DEFAULT_UDP_MAX_PAYLOAD_BYTES,
                DEFAULT_CLUSTER_ID,
                storageDir,
                voters);
    }

    public RaftConfig(long electionTimeoutMinMs,
                  long electionTimeoutMaxMs,
                  long heartbeatIntervalMs,
                  int rpcPort,
                  RaftTransportMode transportMode,
                  int raftBroadcastPort,
                  int udpMaxPayloadBytes,
                  String clusterId,
                  Path storageDir,
                  Map<Integer, RaftPeerEndpoint> voters) {
        if (electionTimeoutMinMs <= 0L) {
            throw new IllegalArgumentException("electionTimeoutMinMs must be > 0");
        }
        if (electionTimeoutMaxMs < electionTimeoutMinMs) {
            throw new IllegalArgumentException(
                    "electionTimeoutMaxMs must be >= electionTimeoutMinMs");
        }
        if (heartbeatIntervalMs <= 0L) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be > 0");
        }
        if (heartbeatIntervalMs >= electionTimeoutMinMs) {
            throw new IllegalArgumentException(
                    "heartbeatIntervalMs must be < electionTimeoutMinMs to avoid spurious elections");
        }
        if (rpcPort < 1 || rpcPort > 65535) {
            throw new IllegalArgumentException("rpcPort out of range: " + rpcPort);
        }
        Objects.requireNonNull(transportMode, "transportMode");
        if (raftBroadcastPort < 1 || raftBroadcastPort > 65535) {
            throw new IllegalArgumentException("raftBroadcastPort out of range: " + raftBroadcastPort);
        }
        if (udpMaxPayloadBytes <= 0) {
            throw new IllegalArgumentException("udpMaxPayloadBytes must be > 0");
        }
        Objects.requireNonNull(clusterId, "clusterId");
        if (clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId must not be blank");
        }
        Objects.requireNonNull(storageDir, "storageDir");
        Objects.requireNonNull(voters, "voters");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters set must be non-empty (Contract A)");
        }
        for (Map.Entry<Integer, RaftPeerEndpoint> e : voters.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) {
                throw new IllegalArgumentException("voters must not contain null keys/values");
            }
            if (e.getKey() != e.getValue().brokerId()) {
                throw new IllegalArgumentException(
                        "voters key " + e.getKey() + " does not match endpoint brokerId "
                                + e.getValue().brokerId());
            }
        }

        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs  = heartbeatIntervalMs;
        this.rpcPort              = rpcPort;
        this.transportMode        = transportMode;
        this.raftBroadcastPort    = raftBroadcastPort;
        this.udpMaxPayloadBytes   = udpMaxPayloadBytes;
        this.clusterId            = clusterId;
        this.storageDir           = storageDir;
        // defensive immutable copy with deterministic iteration order
        this.voters = Collections.unmodifiableMap(new TreeMap<>(voters));
    }

    public long getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public long getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public RaftTransportMode getTransportMode() {
        return transportMode;
    }

    /**
     * UDP port used by every broker in the same Raft cluster for LAN broadcast
     * traffic. Unlike {@link #getRpcPort()}, this port is common across nodes.
     */
    public int getRaftBroadcastPort() {
        return raftBroadcastPort;
    }

    /**
     * Maximum serialized UDP payload accepted by the Raft UDP transport.
     */
    public int getUdpMaxPayloadBytes() {
        return udpMaxPayloadBytes;
    }

    /**
     * Logical cluster identifier used to ignore UDP packets from unrelated
     * clusters sharing the same LAN.
     */
    public String getClusterId() {
        return clusterId;
    }

    public Path getStorageDir() {
        return storageDir;
    }

    /**
     * Static voter set. Keys are Raft broker ids, values their RPC endpoints.
     * Unmodifiable; iteration order is by broker id ascending.
     */
    public Map<Integer, RaftPeerEndpoint> getVoters() {
        return voters;
    }

    /**
     * Quorum size: {@code floor(voters/2) + 1}. Computed from the static set,
     * never from runtime discovery (Contract A).
     */
    public int getQuorumSize() {
        return (voters.size() / 2) + 1;
    }
}
