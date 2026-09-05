package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * UDP LAN transport for broadcast-friendly Raft RPCs.
 *
 * <p>In hybrid mode this transport carries broadcast {@code PreVote}/{@code RequestVote}
 * requests and empty {@code AppendEntries} heartbeats. Log-bearing
 * {@code AppendEntries} requests stay on TCP.
 */
public final class RaftUdpBroadcastTransport implements RaftTransport {

    public static final long SEEN_MESSAGE_TTL_MS = 30_000L;

    private final int localNodeId;
    private final RaftConfig raftConfig;

    private Consumer<RequestVoteResponseMessage> voteResponseHandler;
    private Consumer<PreVoteResponseMessage> preVoteResponseHandler;
    private Function<PreVoteRequestMessage, PreVoteResponseMessage> preVoteRequestHandler;
    private BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler;
    private Function<RequestVoteRequestMessage, RequestVoteResponseMessage> voteRequestHandler;
    private Function<AppendEntriesRequestMessage, AppendEntriesResponseMessage> appendRequestHandler;

    private final ConcurrentHashMap<String, Long> recentlySeenMessageIds = new ConcurrentHashMap<>();

    // Monotonic per-transport sequence number used as envelope creation marker.
    private final AtomicLong envelopeSeq = new AtomicLong(0);

    private volatile DatagramSocket socket;
    private ExecutorService receiveExecutor;

    private volatile boolean running;
    private final AtomicLong receivedPackets = new AtomicLong();
    private volatile long lastReceiveNanos;

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

    @Override
    public synchronized void attachPreVoteHandlers(
            Function<PreVoteRequestMessage, PreVoteResponseMessage> requestHandler,
            Consumer<PreVoteResponseMessage> responseHandler) {
        this.preVoteRequestHandler = Objects.requireNonNull(requestHandler, "requestHandler");
        this.preVoteResponseHandler = Objects.requireNonNull(responseHandler, "responseHandler");
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
        DatagramSocket receiveSocket = socket;
        ExecutorService executor = receiveExecutor;
        // execute leaves uncaught Errors visible instead of hiding them in an ignored Future.
        executor.execute(() -> receiveLoop(receiveSocket, executor));
        System.out.println("[RaftUdpBroadcastTransport] node=" + localNodeId
                + " receiver started port=" + raftConfig.getRaftBroadcastPort());
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

    /** Whether the current receive task is active, including unexpected task termination. */
    public boolean isRunning() {
        return running;
    }

    @Override
    public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
        if (!running) {
            return;
        }

        logElectionSend("RequestVote", request.getTerm());
        RaftUdpEnvelope envelope = createEnvelope(
                peerId,
                RaftUdpMessageType.REQUEST_VOTE_REQUEST,
                request.getTerm(),
                request);
        sendEnvelopeToPeer(peerId, envelope);
    }

    @Override
    public void sendPreVote(int peerId, PreVoteRequestMessage request) {
        if (!running) {
            return;
        }
        logElectionSend("PreVote", request.getTerm());
        sendEnvelopeToPeer(peerId, createEnvelope(
                peerId, RaftUdpMessageType.PRE_VOTE_REQUEST, request.getTerm(), request));
    }

    @Override
    public void broadcastPreVote(PreVoteRequestMessage request, Set<Integer> peerIds) {
        if (!running) {
            return;
        }
        logElectionSend("PreVote", request.getTerm());
        broadcastEnvelope(createEnvelope(
                RaftUdpEnvelope.BROADCAST_TARGET, RaftUdpMessageType.PRE_VOTE_REQUEST,
                request.getTerm(), request));
    }

    @Override
    public void broadcastRequestVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
        if (!running) {
            return;
        }

        logElectionSend("RequestVote", request.getTerm());
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

    private void receiveLoop(DatagramSocket receiveSocket, ExecutorService executor) {
        try {
            byte[] buffer = new byte[raftConfig.getUdpMaxPayloadBytes()];
            while (running && !receiveSocket.isClosed()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                RaftUdpEnvelope envelope = null;
                try {
                    receiveSocket.receive(packet);
                    receivedPackets.incrementAndGet();
                    lastReceiveNanos = System.nanoTime();
                    envelope = deserialize(packet.getData(), packet.getLength());
                    handleEnvelope(envelope);
                } catch (SocketException e) {
                    if (running && !receiveSocket.isClosed()) {
                        System.err.println("[RaftUdpBroadcastTransport] node=" + localNodeId
                                + " receive socket error: " + e);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    if (running && !receiveSocket.isClosed()) {
                        logPacketFailure("failed to decode UDP packet", packet, envelope, e);
                    }
                } catch (FileRaftPersistence.RaftPersistenceException e) {
                    // A failed durable write is not a recoverable packet error.
                    // Do not turn it into permission to keep processing Raft RPCs.
                    logPacketFailure("unrecoverable Raft storage failure", packet, envelope, e);
                    throw e;
                } catch (RuntimeException e) {
                    // A failed RPC is lost, as with an omitted UDP datagram. Later RPCs
                    // must still be received. Errors deliberately escape this boundary.
                    logPacketFailure("UDP packet processing failed", packet, envelope, e);
                }
            }
        } finally {
            synchronized (this) {
                // An old task finishing after stop/start must not disable its replacement.
                if (socket == receiveSocket) {
                    boolean unexpected = running;
                    running = false;
                    receiveSocket.close();
                    socket = null;
                    receiveExecutor = null;
                    recentlySeenMessageIds.clear();
                    if (unexpected) {
                        System.err.println("[RaftUdpBroadcastTransport] node=" + localNodeId
                                + " receiver stopped unexpectedly; transport disabled"
                                + " rxPackets=" + receivedPackets.get());
                    }
                }
            }
            executor.shutdown();
        }
    }

    private void logPacketFailure(String action, DatagramPacket packet,
                                  RaftUdpEnvelope envelope, Exception failure) {
        System.err.println("[RaftUdpBroadcastTransport] node=" + localNodeId + " " + action
                + " type=" + (envelope == null ? "unknown" : envelope.getType())
                + " sender=" + (envelope == null ? "unknown" : envelope.getSenderId())
                + " term=" + (envelope == null ? "unknown" : envelope.getTerm())
                + " source=" + packet.getSocketAddress()
                + " rxPackets=" + receivedPackets.get() + ": " + failure);
        failure.printStackTrace(System.err);
    }

    private void logElectionSend(String type, long term) {
        long lastReceive = lastReceiveNanos;
        String receiveAge = lastReceive == 0L ? "never"
                : Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReceive));
        System.out.println("[RaftUdpBroadcastTransport] node=" + localNodeId
                + " TX " + type + " term=" + term
                + " rxPackets=" + receivedPackets.get() + " lastRxAgeMs=" + receiveAge
                + " receiverRunning=" + running);
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
            case PRE_VOTE_RESPONSE -> {
                if (envelope.getPayload() instanceof PreVoteResponseMessage response) {
                    preVoteResponseHandler.accept(response);
                }
            }
            case PRE_VOTE_REQUEST -> {
                if (envelope.getPayload() instanceof PreVoteRequestMessage request) {
                    PreVoteResponseMessage response = preVoteRequestHandler.apply(request);
                    sendEnvelopeToPeer(envelope.getSenderId(), createEnvelope(
                            envelope.getSenderId(), RaftUdpMessageType.PRE_VOTE_RESPONSE,
                            response.getTerm(), response));
                }
            }
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
        DatagramSocket sendSocket = socket;
        if (sendSocket == null || sendSocket.isClosed()) {
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
                    sendSocket.send(packet);
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
        DatagramSocket sendSocket = socket;
        if (sendSocket == null || sendSocket.isClosed()) {
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
            sendSocket.send(packet);
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

}
