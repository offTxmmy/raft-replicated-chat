package it.polimi.ds.chat.discovery;

import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.broker.discovery.PeerRegistry;
import it.polimi.ds.chat.protocol.broker.PeerInfo;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
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

    private volatile int currentBrokerId;

    private DatagramSocket socket;
    private Thread listenerThread;

    public LanDiscoveryService(BrokerConfig brokerConfig, PeerRegistry peerRegistry) {
        this.brokerConfig = brokerConfig;
        this.peerRegistry = peerRegistry;
        this.currentBrokerId = brokerConfig.getBrokerId();
    }

    public void setBrokerId(int brokerId) {
        this.currentBrokerId = brokerId;
        System.out.println("[LanDiscovery] Updated local brokerId to " + brokerId);
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
     * CHAT_DISCOVERY;HELLO;brokerId;host;port
     */
    private void handlePacket(String payload) {
        String[] parts = payload.split(";");
        if (parts.length < 5) {
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

        String host = parts[3];
        int port = Integer.parseInt(parts[4]);

        PeerInfo peer = new PeerInfo(brokerId, host, port);

        // Update PeerRegistry with this pear
        peerRegistry.upsertFromDiscovery(peer);
    }

    /**
     * Broadcasts a HELLO message on the LAN so other brokers can discover this one.
     */
    public void announcePresence() {
        String msg = String.format(
            "%s;%s;%d;%s;%d",
            MAGIC,
            TYPE_HELLO,
            this.currentBrokerId,
            brokerConfig.getBrokerHost(),
            brokerConfig.getClientPort()
        );
        sendBroadcast(msg);
    }

    private void sendBroadcast(String msg) {
        try (DatagramSocket ds = new DatagramSocket()) {
            ds.setBroadcast(true);
            byte[] data = msg.getBytes(StandardCharsets.UTF_8);

            // Iterate over all network interfaces
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();

                // Skip loopback (localhost) or down interfaces
                if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                    continue;
                }

                // Iterate over addresses associated with this interface
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress broadcast = interfaceAddress.getBroadcast();

                    // If this interface has a valid broadcast address, send the packet
                    if (broadcast != null) {
                        try {
                            DatagramPacket packet = new DatagramPacket(
                                    data,
                                    data.length,
                                    broadcast,
                                    brokerConfig.getUdpPort()
                            );
                            ds.send(packet);
                        } catch (IOException e) {
                            System.err.println("[LanDiscovery] Failed to send to " + broadcast + ": " + e.getMessage());
                        }
                    }
                }
            }
            System.out.println("[LanDiscovery] Sent HELLO broadcast to all interfaces: " + msg);
        } catch (IOException e) {
            System.err.println("[LanDiscovery] Failed to initialize broadcast socket: " + e.getMessage());
        }
    }
}
