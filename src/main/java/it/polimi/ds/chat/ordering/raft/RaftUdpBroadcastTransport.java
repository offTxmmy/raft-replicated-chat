package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.RaftUdpEnvelope;
import it.polimi.ds.chat.protocol.raft.RaftUdpMessageType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * UDP LAN transport for broadcast-friendly Raft RPCs.
 *
 * <p>In hybrid mode this transport carries broadcast {@code RequestVote}
 * requests and empty {@code AppendEntries} heartbeats. Log-bearing
 * {@code AppendEntries} requests stay on TCP.
 */
public final class RaftUdpBroadcastTransport implements RaftTransport {

    public static final long SEEN_MESSAGE_TTL_MS = 30_000L;

    private final int localNodeId;
    private final RaftConfig raftConfig;

    private Consumer<RequestVoteResponseMessage> voteResponseHandler;
    private BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler;
    private Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteRequestHandler;
    private Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendRequestHandler;

    private final ConcurrentHashMap<String, Long> recentlySeenMessageIds = new ConcurrentHashMap<>();

    // Monotonic per-transport sequence number used as envelope creation marker.
    private final AtomicLong envelopeSeq = new AtomicLong(0);

    private DatagramSocket socket;
    private ExecutorService receiveExecutor;

    private volatile boolean running;

    public RaftUdpBroadcastTransport(int localNodeId, RaftConfig raftConfig) {
        this.localNodeId = localNodeId;
        this.raftConfig = Objects.requireNonNull(raftConfig, "raftConfig");
        if (!raftConfig.getVoters().containsKey(localNodeId)) {
            throw new IllegalArgumentException(
                    "Local node id " + localNodeId + " is not in the static voter set");
        }
    }

    @Override
    public synchronized void attachHandlers(
            Consumer<RequestVoteResponseMessage> voteResponseHandler,
            BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler) {
        this.voteResponseHandler = Objects.requireNonNull(voteResponseHandler, "voteResponseHandler");
        this.appendResponseHandler = Objects.requireNonNull(appendResponseHandler, "appendResponseHandler");
    }

    /**
     * Installs handlers for Raft requests received on the UDP socket.
     *
     * <p>TCP handles inbound requests through {@link RaftRpcServer}. UDP broadcast
     * receives requests and responses on the same socket, so this transport needs
     * the request handlers directly.
     */
    public synchronized void attachRequestHandlers(
            Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteRequestHandler,
            Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendRequestHandler) {
        this.voteRequestHandler = Objects.requireNonNull(voteRequestHandler, "voteRequestHandler");
        this.appendRequestHandler = Objects.requireNonNull(appendRequestHandler, "appendRequestHandler");
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (voteResponseHandler == null || appendResponseHandler == null) {
            throw new IllegalStateException("attachHandlers must be called before start");
        }
        if (voteRequestHandler == null || appendRequestHandler == null) {
            throw new IllegalStateException("attachRequestHandlers must be called before start");
        }

        try {
            socket = openSocket();
        } catch (SocketException e) {
            throw new RuntimeException("Failed to start Raft UDP transport on port "
                    + raftConfig.getRaftBroadcastPort(), e);
        }

        receiveExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-udp-receive-" + localNodeId);
            t.setDaemon(true);
            return t;
        });

        running = true;
        receiveExecutor.submit(this::receiveLoop);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }

        running = false;
        if (socket != null) {
            socket.close();
            socket = null;
        }
        if (receiveExecutor != null) {
            receiveExecutor.shutdownNow();
            receiveExecutor = null;
        }
        recentlySeenMessageIds.clear();
    }

    @Override
    public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
        if (!running) {
            return;
        }

        RaftUdpEnvelope envelope = createEnvelope(
                peerId,
                RaftUdpMessageType.REQUEST_VOTE_REQUEST,
                request.getTerm(),
                request);
        sendEnvelopeToPeer(peerId, envelope);
    }

    @Override
    public void broadcastRequestVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
        if (!running) {
            return;
        }

        RaftUdpEnvelope envelope = createEnvelope(
                RaftUdpEnvelope.BROADCAST_TARGET,
                RaftUdpMessageType.REQUEST_VOTE_REQUEST,
                request.getTerm(),
                request);
        broadcastEnvelope(envelope);
    }

    @Override
    public void sendAppendEntries(int peerId, AppendEntriesRequestMessage request) {
        if (!running) {
            return;
        }
        if (!request.getEntries().isEmpty()) {
            throw new IllegalArgumentException(
                    "RaftUdpBroadcastTransport only supports empty AppendEntries heartbeats");
        }

        RaftUdpEnvelope envelope = createEnvelope(
                peerId,
                RaftUdpMessageType.APPEND_ENTRIES_REQUEST,
                request.getTerm(),
                request);
        sendEnvelopeToPeer(peerId, envelope);
    }

    @Override
    public void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
        if (!running) {
            return;
        }
        if (!request.getEntries().isEmpty()) {
            throw new IllegalArgumentException(
                    "RaftUdpBroadcastTransport only supports empty AppendEntries heartbeats");
        }

        // UDP broadcast is addressed to all voters. The replication manager only
        // calls this when the empty heartbeat is valid for every follower.
        RaftUdpEnvelope envelope = createEnvelope(
                RaftUdpEnvelope.BROADCAST_TARGET,
                RaftUdpMessageType.APPEND_ENTRIES_REQUEST,
                request.getTerm(),
                request);
        broadcastEnvelope(envelope);
    }

    private void receiveLoop() {
        byte[] buffer = new byte[raftConfig.getUdpMaxPayloadBytes()];
        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                RaftUdpEnvelope envelope = deserialize(packet.getData(), packet.getLength());
                handleEnvelope(envelope);
            } catch (SocketException e) {
                if (running) {
                    System.err.println("[RaftUdpBroadcastTransport] receive socket error: " + e.getMessage());
                }
            } catch (IOException | ClassNotFoundException e) {
                if (running) {
                    System.err.println("[RaftUdpBroadcastTransport] failed to decode UDP packet: " + e.getMessage());
                }
            }
        }
    }

    private void handleEnvelope(RaftUdpEnvelope envelope) {
        if (!raftConfig.getClusterId().equals(envelope.getClusterId())) {
            return;
        }
        if (envelope.getSenderId() == localNodeId) {
            return;
        }
        if (!envelope.isAddressedTo(localNodeId)) {
            return;
        }
        if (!raftConfig.getVoters().containsKey(envelope.getSenderId())) {
            return;
        }
        if (isDuplicate(envelope.getMessageId())) {
            return;
        }
        dispatchEnvelope(envelope);
    }

    private void dispatchEnvelope(RaftUdpEnvelope envelope) {
        switch (envelope.getType()) {
            case REQUEST_VOTE_RESPONSE -> {
                if (envelope.getPayload() instanceof RequestVoteResponseMessage response) {
                    voteResponseHandler.accept(response);
                }
            }
            case APPEND_ENTRIES_RESPONSE -> {
                if (envelope.getPayload() instanceof AppendEntriesResponseMessage response) {
                    appendResponseHandler.accept(envelope.getSenderId(), response);
                }
            }
            case REQUEST_VOTE_REQUEST -> {
                if (envelope.getPayload() instanceof RequestVoteRequestMessage request) {
                    RequestVoteResponseMessage response = voteRequestHandler.apply(request);
                    sendEnvelopeToPeer(
                            envelope.getSenderId(),
                            createEnvelope(
                                    envelope.getSenderId(),
                                    RaftUdpMessageType.REQUEST_VOTE_RESPONSE,
                                    response.getTerm(),
                                    response));
                }
            }
            case APPEND_ENTRIES_REQUEST -> {
                if (envelope.getPayload() instanceof AppendEntriesRequestMessage request) {
                    if (!request.getEntries().isEmpty()) {
                        System.err.println("[RaftUdpBroadcastTransport] ignoring non-empty AppendEntries over UDP");
                        return;
                    }

                    AppendEntriesResponseMessage response = appendRequestHandler.apply(request);
                    sendEnvelopeToPeer(
                            envelope.getSenderId(),
                            createEnvelope(
                                    envelope.getSenderId(),
                                    RaftUdpMessageType.APPEND_ENTRIES_RESPONSE,
                                    response.getTerm(),
                                    response));
                }
            }
        }
    }

    private boolean isDuplicate(String messageId) {
        long now = System.currentTimeMillis();
        purgeExpiredSeenMessages(now);

        Long previous = recentlySeenMessageIds.putIfAbsent(messageId, now);
        return previous != null && now - previous <= SEEN_MESSAGE_TTL_MS;
    }

    private void purgeExpiredSeenMessages(long now) {
        recentlySeenMessageIds.entrySet().removeIf(
                entry -> now - entry.getValue() > SEEN_MESSAGE_TTL_MS);
    }

    private void broadcastEnvelope(RaftUdpEnvelope envelope) {
        if (socket == null || socket.isClosed()) {
            return;
        }

        try {
            byte[] data = serialize(envelope);
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }

                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress broadcast = interfaceAddress.getBroadcast();
                    if (broadcast == null) {
                        continue;
                    }

                    DatagramPacket packet = new DatagramPacket(
                            data,
                            data.length,
                            broadcast,
                            raftConfig.getRaftBroadcastPort());
                    socket.send(packet);
                }
            }
        } catch (IOException e) {
            System.err.println("[RaftUdpBroadcastTransport] failed to broadcast UDP envelope: "
                    + e.getMessage());
        }
    }

    private RaftUdpEnvelope createEnvelope(int targetId,
                                           RaftUdpMessageType type,
                                           long term,
                                           Object payload) {
        return new RaftUdpEnvelope(
                raftConfig.getClusterId(),
                UUID.randomUUID().toString(),
                localNodeId,
                targetId,
                type,
                term,
                payload,
                envelopeSeq.incrementAndGet());
    }

    private void sendEnvelopeToPeer(int peerId, RaftUdpEnvelope envelope) {
        if (socket == null || socket.isClosed()) {
            return;
        }

        RaftPeerEndpoint endpoint = raftConfig.getVoters().get(peerId);
        if (endpoint == null) {
            return;
        }

        try {
            byte[] data = serialize(envelope);
            InetAddress address = InetAddress.getByName(endpoint.host());
            DatagramPacket packet = new DatagramPacket(
                    data,
                    data.length,
                    address,
                    raftConfig.getRaftBroadcastPort());
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("[RaftUdpBroadcastTransport] failed to send UDP envelope to peer "
                    + peerId + ": " + e.getMessage());
        }
    }

    private DatagramSocket openSocket() throws SocketException {
        DatagramSocket udpSocket = new DatagramSocket(null);
        udpSocket.setReuseAddress(true);
        udpSocket.setBroadcast(true);
        udpSocket.bind(new InetSocketAddress(raftConfig.getRaftBroadcastPort()));
        return udpSocket;
    }

    private byte[] serialize(RaftUdpEnvelope envelope) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(envelope);
        }

        byte[] data = bytes.toByteArray();
        if (data.length > raftConfig.getUdpMaxPayloadBytes()) {
            throw new IOException("Serialized Raft UDP envelope exceeds max payload: "
                    + data.length + " > " + raftConfig.getUdpMaxPayloadBytes());
        }
        return data;
    }

    private RaftUdpEnvelope deserialize(byte[] data, int length) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data, 0, length))) {
            Object value = in.readObject();
            if (!(value instanceof RaftUdpEnvelope envelope)) {
                throw new IOException("Unexpected UDP payload type: "
                        + (value == null ? "null" : value.getClass().getName()));
            }
            return envelope;
        }
    }

    boolean isRunningForTesting() {
        return running;
    }
}