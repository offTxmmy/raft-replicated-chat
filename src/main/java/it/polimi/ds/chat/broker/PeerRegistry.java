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
 * <p>
 * Essential for Raft implementation where brokers need to communicate directly
 * for leader election and log replication.
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

    /**
     * Constructs a PeerRegistry for a broker.
     *
     * @param localBrokerId the ID of the local broker
     * @param directoryHost the host of the Directory Service
     * @param directoryPort the port of the Directory Service
     */
    public PeerRegistry(int localBrokerId, String directoryHost, int directoryPort) {
        this.localBrokerId = localBrokerId;
        this.directoryHost = directoryHost;
        this.directoryPort = directoryPort;
    }

    /**
     * Starts the peer registry and begins periodic refresh of the peer list.
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
     * Stops the peer registry and terminates periodic refresh.
     */
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
        }
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
     * Returns the PeerInfo for the sequencer/leader broker, if present.
     *
     * @return Optional containing PeerInfo of the sequencer, otherwise empty
     */
    public Optional<PeerInfo> getSequencer() {
        return peers.values().stream()
                .filter(PeerInfo::isSequencer)
                .findFirst();
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
     * Calculates the quorum size (majority) for Raft consensus.
     *
     * @return quorum size
     */
    public int getQuorumSize() {
        return (getClusterSize() / 2) + 1;
    }

    /**
     * Refreshes the peer list from the Directory Service.
     * Notifies listeners if the peer list has changed.
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
     * Fetches the peer list from the Directory Service via TCP.
     *
     * @return list of PeerInfo received from the Directory Service, or null on failure
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
     * Manually triggers a refresh of the peer list from the Directory Service.
     */
    public void forceRefresh() {
        refreshPeerList();
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
     * Returns a string representation of the PeerRegistry, including cluster size and quorum.
     *
     * @return string describing the PeerRegistry
     */
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
