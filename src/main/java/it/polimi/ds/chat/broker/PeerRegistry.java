package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.messages.GetPeerListRequestMessage;
import it.polimi.ds.chat.messages.GetPeerListResponseMessage;
import it.polimi.ds.chat.messages.PeerInfo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Registry that maintains the list of known broker peers.
 * Periodically refreshes the peer list from the Directory Service.
 *
 * This is essential for Raft implementation where brokers need to
 * communicate directly with each other for:
 * - Leader election (RequestVote)
 * - Log replication (AppendEntries)
 */
public class PeerRegistry {

    private final int localBrokerId;
    private final String directoryHost;
    private final int directoryPort;

    // Map of brokerId -> PeerInfo for all known peers
    private final Map<Integer, PeerInfo> peers = new ConcurrentHashMap<>();

    // Listeners notified when peer list changes
    private final List<Consumer<List<PeerInfo>>> peerChangeListeners = new ArrayList<>();

    // Scheduled executor for periodic refresh
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    // Refresh interval in seconds
    private static final long REFRESH_INTERVAL_SECONDS = 10;

    public PeerRegistry(int localBrokerId, String directoryHost, int directoryPort) {
        this.localBrokerId = localBrokerId;
        this.directoryHost = directoryHost;
        this.directoryPort = directoryPort;
    }

    /**
     * Start the peer registry and begin periodic refresh.
     */
    public void start() {
        running = true;

        // Initial fetch
        refreshPeerList();

        // Schedule periodic refresh
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PeerRegistry-Refresh-" + localBrokerId);
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(
                this::refreshPeerList,
                REFRESH_INTERVAL_SECONDS,
                REFRESH_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /**
     * Stop the peer registry.
     */
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * Register a listener to be notified when the peer list changes.
     */
    public void addPeerChangeListener(Consumer<List<PeerInfo>> listener) {
        peerChangeListeners.add(listener);
    }

    /**
     * Get all known peers (excluding self).
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
     * Get all known peers including self.
     */
    public List<PeerInfo> getAllPeers() {
        return new ArrayList<>(peers.values());
    }

    /**
     * Get a specific peer by ID.
     */
    public Optional<PeerInfo> getPeer(int brokerId) {
        return Optional.ofNullable(peers.get(brokerId));
    }

    /**
     * Get the sequencer/leader peer.
     */
    public Optional<PeerInfo> getSequencer() {
        return peers.values().stream()
                .filter(PeerInfo::isSequencer)
                .findFirst();
    }

    /**
     * Get the number of known peers (excluding self).
     */
    public int getPeerCount() {
        return (int) peers.values().stream()
                .filter(p -> p.getBrokerId() != localBrokerId)
                .count();
    }

    /**
     * Get the total cluster size (including self).
     */
    public int getClusterSize() {
        return peers.size();
    }

    /**
     * Calculate the quorum size (majority) for Raft.
     */
    public int getQuorumSize() {
        return (getClusterSize() / 2) + 1;
    }

    /**
     * Refresh the peer list from the Directory Service.
     */
    private void refreshPeerList() {
        try {
            List<PeerInfo> newPeers = fetchPeerListFromDirectory();

            if (newPeers != null) {
                // Check for changes
                Set<Integer> oldIds = new HashSet<>(peers.keySet());
                Set<Integer> newIds = new HashSet<>();

                for (PeerInfo peer : newPeers) {
                    newIds.add(peer.getBrokerId());
                    peers.put(peer.getBrokerId(), peer);
                }

                // Remove peers that are no longer in the list
                for (Integer oldId : oldIds) {
                    if (!newIds.contains(oldId)) {
                        peers.remove(oldId);
                    }
                }

                // Notify listeners if there were changes
                if (!oldIds.equals(newIds)) {
                    notifyPeerChange();
                }
            }
        } catch (Exception e) {
            System.err.println("[PeerRegistry] Failed to refresh peer list: " + e.getMessage());
        }
    }

    /**
     * Fetch the peer list from the Directory Service.
     */
    private List<PeerInfo> fetchPeerListFromDirectory() {
        try (Socket socket = new Socket(directoryHost, directoryPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // Send request
            GetPeerListRequestMessage request = new GetPeerListRequestMessage(localBrokerId);
            out.writeObject(request);
            out.flush();

            // Read response
            Object response = in.readObject();
            if (response instanceof GetPeerListResponseMessage resp) {
                if (resp.isSuccess()) {
                    System.out.println("[PeerRegistry] Received " + resp.getPeers().size() + " peers from Directory Service");
                    return resp.getPeers();
                }
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[PeerRegistry] Error fetching peer list: " + e.getMessage());
        }

        return null;
    }

    /**
     * Manually trigger a refresh of the peer list.
     */
    public void forceRefresh() {
        refreshPeerList();
    }

    /**
     * Notify all listeners of peer list change.
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

    @Override
    public String toString() {
        return "PeerRegistry{" +
                "localBrokerId=" + localBrokerId +
                ", peers=" + peers.values() +
                ", clusterSize=" + getClusterSize() +
                ", quorum=" + getQuorumSize() +
                '}';
    }
}

