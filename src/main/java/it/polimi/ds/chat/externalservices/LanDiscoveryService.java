package it.polimi.ds.chat.externalservices;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.PeerRegistry;
import it.polimi.ds.chat.messages.PeerInfo;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LAN broadcast-based discovery between brokers.
 *
 * Responsibilities:
 * - Listen on UDP port (config.getUdpPort()) for discovery packets.
 * - Parse HELLO messages and update PeerRegistry.
 * - On startup, broadcast our presence so others can discover us.
 */
public class LanDiscoveryService {

    private static final String MAGIC = "CHAT_DISCOVERY";
    private static final String TYPE_HELLO = "HELLO";

    private final BrokerConfig brokerConfig;
    private final PeerRegistry peerRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private DatagramSocket socket;
    private Thread listenerThread;

    public LanDiscoveryService(BrokerConfig brokerConfig, PeerRegistry peerRegistry) {
        this.brokerConfig = brokerConfig;
        this.peerRegistry = peerRegistry;
    }

    /**
     * Starts the UDP listener on the configured udpPort.
     */
    public void start() throws SocketException {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        int port = brokerConfig.getUdpPort();

        DatagramSocket ds = new DatagramSocket(null);
        ds.setReuseAddress(true);
        ds.bind(new java.net.InetSocketAddress(port));

        this.socket = ds;

        listenerThread = new Thread(this::listenLoop, "LanDiscoveryListener-" + brokerConfig.getBrokerId());
        listenerThread.setDaemon(true);
        listenerThread.start();

        System.out.println("[LanDiscovery] Listening for discovery packets on UDP port " + port);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (socket != null && !socket.isClosed()) {
            socket.close();
        }

        if (listenerThread != null) {
            listenerThread.interrupt();
        }

        System.out.println("[LanDiscovery] Stopped");
    }

    private void listenLoop() {
        byte[] buffer = new byte[1024];

        while (running.get()) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String payload = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8).trim();

                handlePacket(payload);
            } catch (IOException e) {
                if (running.get()) {
                    System.err.println("[LanDiscovery] Error receiving packet: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Expected format:
     * CHAT_DISCOVERY;HELLO;brokerId;host;port;isSequencer
     */
    private void handlePacket(String payload) {
        String[] parts = payload.split(";");
        if (parts.length < 6) {
            return;
        }

        if (!MAGIC.equals(parts[0])) {
            return;
        }

        String type = parts[1];
        if (!TYPE_HELLO.equals(type)) {
            return;
        }

        int brokerId = Integer.parseInt(parts[2]);

        // Ignore our own broadcasts
        if (brokerId == brokerConfig.getBrokerId()) {
            return;
        }

        String host = parts[5];
        int port = Integer.parseInt(parts[4]);
        boolean isSequencer = Boolean.parseBoolean(parts[5]);

        PeerInfo peer = new PeerInfo(brokerId, host, port, isSequencer);

        // Update PeerRegistry with this pear
        peerRegistry.upsertFromDiscovery(peer);
    }

    /**
     * Broadcasts a HELLO message on the LAN so other brokers can discover this one.
     */
    public void announcePresence() {
        String msg = String.format(
            "%s;%s;%d;%s;%d;%s",
            MAGIC,
            TYPE_HELLO,
            brokerConfig.getBrokerId(),
            brokerConfig.getBrokerHost(),
            brokerConfig.getBrokerPort(),
            brokerConfig.isSequencer()
        );
        sendBroadcast(msg);
    }

    private void sendBroadcast(String msg) {
        try (DatagramSocket ds = new DatagramSocket()) {
            ds.setBroadcast(true);
            byte[] data = msg.getBytes(StandardCharsets.UTF_8);

            // Simple global broadcast
            DatagramPacket packet = new DatagramPacket(
                    data,
                    data.length,
                    InetAddress.getByName("255.255.255.255"),
                    brokerConfig.getUdpPort()
            );

            ds.send(packet);
            System.out.println("[LanDiscovery] Sent HELLO broadcast: " + msg);
        } catch (IOException e) {
            System.err.println("[LanDiscovery] Failed to send broadcast: " + e.getMessage());
        }
    }
}
