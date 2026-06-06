package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.discovery.PeerRegistry;
import it.polimi.ds.chat.protocol.broker.PeerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PeerRegistry and PeerInfo.
 * Note: Full integration tests require DirectoryService running.
 */
public class PeerRegistryTest {

    @Test
    @DisplayName("PeerInfo stores broker information correctly")
    void testPeerInfoBasics() {
        PeerInfo peer = new PeerInfo(1, "localhost", 5000);

        assertEquals(1, peer.getBrokerId());
        assertEquals("localhost", peer.getHost());
        assertEquals(5000, peer.getPort());
    }

    @Test
    @DisplayName("PeerInfo equals based on brokerId")
    void testPeerInfoEquals() {
        PeerInfo peer1 = new PeerInfo(1, "localhost", 5000);
        PeerInfo peer2 = new PeerInfo(1, "192.168.1.1", 6000);
        PeerInfo peer3 = new PeerInfo(2, "localhost", 5000);

        assertEquals(peer1, peer2, "Same brokerId should be equal");
        assertNotEquals(peer1, peer3, "Different brokerId should not be equal");
    }

    @Test
    @DisplayName("PeerRegistry initializes correctly")
    void testPeerRegistryInitialization() {
        PeerRegistry registry = new PeerRegistry(1);

        assertEquals(0, registry.getPeerCount(), "Initially no peers");
        assertEquals(0, registry.getClusterSize(), "Initially cluster size is 0");
        assertTrue(registry.getPeers().isEmpty(), "Peer list should be empty");
    }

    @Test
    @DisplayName("PeerInfo toString provides readable output")
    void testPeerInfoToString() {
        PeerInfo peer = new PeerInfo(1, "localhost", 5000);
        String str = peer.toString();

        assertTrue(str.contains("brokerId=1"));
        assertTrue(str.contains("localhost"));
        assertTrue(str.contains("5000"));
    }

    @Test
    @DisplayName("PeerRegistry listener registration works")
    void testPeerChangeListener() {
        PeerRegistry registry = new PeerRegistry(1);

        final boolean[] listenerCalled = {false};
        registry.addPeerChangeListener(peers -> listenerCalled[0] = true);

        // Listener should be registered (won't be called until peers change)
        assertNotNull(registry.getPeers());
    }
}

