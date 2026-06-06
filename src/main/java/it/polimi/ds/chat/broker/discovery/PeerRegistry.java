package it.polimi.ds.chat.broker.discovery;

import it.polimi.ds.chat.protocol.broker.PeerInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Registry that maintains the list of broker peers discovered on the LAN.
 *
 * <p>This registry is auxiliary discovery state. It is not the source of Raft
 * membership or quorum decisions, which are defined by RaftConfig.
 */
public class PeerRegistry {
    private final int localBrokerId;

    // Map of brokerId -> PeerInfo for all known peers
    private final Map<Integer, PeerInfo> peers = new ConcurrentHashMap<>();

    // Listeners notified when peer list changes
    private final List<Consumer<List<PeerInfo>>> peerChangeListeners = new ArrayList<>();

    /**
     * Constructs a PeerRegistry for a broker.
     *
     * @param localBrokerId the ID of the local broker
     */
    public PeerRegistry(int localBrokerId) {
        this.localBrokerId = localBrokerId;
    }

    /**
     * Registers a listener to be notified when the peer list changes.
     *
     * @param listener a Consumer that accepts the updated list of PeerInfo
     */
    public void addPeerChangeListener(Consumer<List<PeerInfo>> listener) {
        peerChangeListeners.add(listener);
    }

    /**
     * Returns all known peers, excluding the local broker itself.
     *
     * @return list of PeerInfo for all peers except self
     */
    public List<PeerInfo> getPeers() {
        List<PeerInfo> result = new ArrayList<>();
        for (PeerInfo peer : peers.values()) {
            if (peer.getBrokerId() != localBrokerId) {
                result.add(peer);
            }
        }
        return result;
    }

    /**
     * Returns all known peers, including the local broker.
     *
     * @return list of PeerInfo for all peers
     */
    public List<PeerInfo> getAllPeers() {
        return new ArrayList<>(peers.values());
    }

    /**
     * Returns the PeerInfo for a specific broker ID, if present.
     *
     * @param brokerId the broker ID to look up
     * @return Optional containing PeerInfo if found, otherwise empty
     */
    public Optional<PeerInfo> getPeer(int brokerId) {
        return Optional.ofNullable(peers.get(brokerId));
    }

    /**
     * Returns the number of known peers, excluding the local broker.
     *
     * @return peer count excluding self
     */
    public int getPeerCount() {
        return (int) peers.values().stream()
                .filter(p -> p.getBrokerId() != localBrokerId)
                .count();
    }

    /**
     * Returns the total cluster size, including the local broker.
     *
     * @return total number of brokers in the cluster
     */
    public int getClusterSize() {
        return peers.size();
    }

    /**
     * Adds or updates a peer discovered via LAN broadcast.
     * If the peer list effectively changes, notifies listeners.
     *
     * @param peer the discovered PeerInfo
     */
    public synchronized void upsertFromDiscovery(PeerInfo peer) {
        int id = peer.getBrokerId();

        // Ignore our own id (sanity check – already do this in LanDiscoveryService)
        if (id == localBrokerId) {
            return;
        }

        PeerInfo previous = peers.put(id, peer);

        boolean changed = (previous == null)
                || !Objects.equals(previous.getHost(), peer.getHost())
                || previous.getPort() != peer.getPort();

        if (changed) {
            System.out.println("[PeerRegistry] Updated peer from LAN discovery: " + peer);
            notifyPeerChange();
        }
    }

    /**
     * Notifies all registered listeners of a peer list change.
     */
    private void notifyPeerChange() {
        List<PeerInfo> currentPeers = getPeers();
        for (Consumer<List<PeerInfo>> listener : peerChangeListeners) {
            try {
                listener.accept(currentPeers);
            } catch (Exception e) {
                System.err.println("[PeerRegistry] Error notifying peer change listener: " + e.getMessage());
            }
        }
    }

    /**
     * Returns a string representation of the PeerRegistry, including discovered cluster size.
     *
     * @return string describing the PeerRegistry
     */
    @Override
    public String toString() {
        return "PeerRegistry{" +
                "localBrokerId=" + localBrokerId +
                ", peers=" + peers.values() +
                ", discoveredClusterSize=" + getClusterSize() +
                '}';
    }
}
