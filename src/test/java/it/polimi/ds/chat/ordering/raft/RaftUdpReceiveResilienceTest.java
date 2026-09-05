package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.ordering.raft.config.RaftConfig;
import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;
import it.polimi.ds.chat.ordering.raft.config.RaftTransportMode;
import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;
import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.RaftUdpEnvelope;
import it.polimi.ds.chat.protocol.raft.RaftUdpMessageType;
import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Component tests through the production socket, decoder and dispatch path. */
@ResourceLock("SYSTEM_ERR")
class RaftUdpReceiveResilienceTest {

    @TempDir
    Path tempDir;

    @Test
    void voteRequestFailureShouldBeVisibleAndNotKillReception() throws Exception {
        assertHandlerFailureIsIsolated(RaftUdpMessageType.REQUEST_VOTE_REQUEST);
    }

    @Test
    void voteResponseFailureShouldBeVisibleAndNotKillReception() throws Exception {
        assertHandlerFailureIsIsolated(RaftUdpMessageType.REQUEST_VOTE_RESPONSE);
    }

    @Test
    void appendRequestFailureShouldBeVisibleAndNotKillReception() throws Exception {
        assertHandlerFailureIsIsolated(RaftUdpMessageType.APPEND_ENTRIES_REQUEST);
    }

    @Test
    void appendResponseFailureShouldBeVisibleAndNotKillReception() throws Exception {
        assertHandlerFailureIsIsolated(RaftUdpMessageType.APPEND_ENTRIES_RESPONSE);
    }

    @Test
    void preVoteRequestFailureShouldBeVisibleAndNotKillReception() throws Exception {
        assertHandlerFailureIsIsolated(RaftUdpMessageType.PRE_VOTE_REQUEST);
    }

    @Test
    void preVoteResponseFailureShouldBeVisibleAndNotKillReception() throws Exception {
        assertHandlerFailureIsIsolated(RaftUdpMessageType.PRE_VOTE_RESPONSE);
    }

    @Test
    void runtimeFailureDuringDeserializationShouldNotKillReception() throws Exception {
        int port = availableUdpPort();
        RaftUdpBroadcastTransport transport = new RaftUdpBroadcastTransport(0, config(port));
        CountDownLatch processed = new CountDownLatch(1);
        attachHandlers(transport, RaftUdpMessageType.REQUEST_VOTE_RESPONSE, processed::countDown);
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream previousErr = System.err;
        try (PrintStream capturedErr = new PrintStream(errors, true, StandardCharsets.UTF_8);
             DatagramSocket sender = new DatagramSocket()) {
            System.setErr(capturedErr);
            transport.start();
            send(sender, port, new FailingDeserialization());
            send(sender, port, envelope(RaftUdpMessageType.REQUEST_VOTE_RESPONSE));
            assertTrue(processed.await(3, TimeUnit.SECONDS), "valid packet after decode failure was not processed");
            assertTrue(transport.isRunning());
            String diagnostics = errors.toString(StandardCharsets.UTF_8);
            assertTrue(diagnostics.contains("injected decode failure"), diagnostics);
            assertTrue(diagnostics.contains("type=unknown"), diagnostics);
            assertTrue(diagnostics.contains("source="), diagnostics);
        } finally {
            transport.stop();
            System.setErr(previousErr);
        }
    }

    @Test
    void uncaughtErrorShouldDisableReceiverVisiblyAndAllowExplicitRestart() throws Exception {
        int port = availableUdpPort();
        RaftUdpBroadcastTransport transport = new RaftUdpBroadcastTransport(0, config(port));
        AssertionError injected = new AssertionError("injected unrecoverable error");
        CountDownLatch uncaught = new CountDownLatch(1);
        AtomicReference<Throwable> escaped = new AtomicReference<>();
        attachHandlers(transport, RaftUdpMessageType.REQUEST_VOTE_RESPONSE, () -> {
            Thread.currentThread().setUncaughtExceptionHandler((thread, failure) -> {
                escaped.set(failure);
                uncaught.countDown();
            });
            throw injected;
        });
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream previousErr = System.err;
        try (PrintStream capturedErr = new PrintStream(errors, true, StandardCharsets.UTF_8);
             DatagramSocket sender = new DatagramSocket()) {
            System.setErr(capturedErr);
            transport.start();
            send(sender, port, envelope(RaftUdpMessageType.REQUEST_VOTE_RESPONSE));
            assertTrue(uncaught.await(3, TimeUnit.SECONDS), "Error must escape to the thread uncaught handler");
            assertSame(injected, escaped.get());
            assertFalse(transport.isRunning(), "a dead receiver must not leave running=true");
            assertTrue(errors.toString(StandardCharsets.UTF_8).contains("receiver stopped unexpectedly"));

            CountDownLatch processed = new CountDownLatch(1);
            attachHandlers(transport, RaftUdpMessageType.REQUEST_VOTE_RESPONSE, processed::countDown);
            transport.start();
            send(sender, port, envelope(RaftUdpMessageType.REQUEST_VOTE_RESPONSE));
            assertTrue(processed.await(3, TimeUnit.SECONDS), "restart must recreate a working receiver");
            assertTrue(transport.isRunning());
        } finally {
            transport.stop();
            System.setErr(previousErr);
        }
    }

    @Test
    void oldReceiverExitAfterStopStartShouldNotDisableReplacement() throws Exception {
        int port = availableUdpPort();
        RaftUdpBroadcastTransport transport = new RaftUdpBroadcastTransport(0, config(port));
        CountDownLatch oldHandlerEntered = new CountDownLatch(1);
        CountDownLatch releaseOldHandler = new CountDownLatch(1);
        CountDownLatch oldReceiverExited = new CountDownLatch(1);
        AtomicReference<Throwable> escaped = new AtomicReference<>();
        AssertionError oldFailure = new AssertionError("old receiver finishing after restart");
        attachHandlers(transport, RaftUdpMessageType.REQUEST_VOTE_RESPONSE, () -> {
            Thread.currentThread().setUncaughtExceptionHandler((thread, failure) -> {
                escaped.set(failure);
                oldReceiverExited.countDown();
            });
            oldHandlerEntered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    assertTrue(releaseOldHandler.await(3, TimeUnit.SECONDS), "old handler was not released");
                    break;
                } catch (InterruptedException e) {
                    // stop() interrupts this handler, but the old task may still be unwinding.
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            throw oldFailure;
        });
        try (DatagramSocket sender = new DatagramSocket()) {
            transport.start();
            send(sender, port, envelope(RaftUdpMessageType.REQUEST_VOTE_RESPONSE));
            assertTrue(oldHandlerEntered.await(3, TimeUnit.SECONDS));
            transport.stop();
            CountDownLatch processed = new CountDownLatch(1);
            attachHandlers(transport, RaftUdpMessageType.REQUEST_VOTE_RESPONSE, processed::countDown);
            transport.start();
            releaseOldHandler.countDown();
            assertTrue(oldReceiverExited.await(3, TimeUnit.SECONDS));
            assertSame(oldFailure, escaped.get());
            assertTrue(transport.isRunning(), "old task cleanup must not disable the new task");
            send(sender, port, envelope(RaftUdpMessageType.REQUEST_VOTE_RESPONSE));
            assertTrue(processed.await(3, TimeUnit.SECONDS), "new receiver must retain its socket");
        } finally {
            releaseOldHandler.countDown();
            transport.stop();
        }
    }

    @Test
    void knownUnrecoverableStorageFailureMustNotBeTreatedAsRecoverablePacketFailure() throws Exception {
        int port = availableUdpPort();
        RaftUdpBroadcastTransport transport = new RaftUdpBroadcastTransport(0, config(port));
        FileRaftPersistence.RaftPersistenceException injected =
                new FileRaftPersistence.RaftPersistenceException("injected durable write failure", null);
        CountDownLatch uncaught = new CountDownLatch(1);
        AtomicReference<Throwable> escaped = new AtomicReference<>();
        attachHandlers(transport, RaftUdpMessageType.REQUEST_VOTE_RESPONSE, () -> {
            Thread.currentThread().setUncaughtExceptionHandler((thread, failure) -> {
                escaped.set(failure);
                uncaught.countDown();
            });
            throw injected;
        });
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream previousErr = System.err;
        try (PrintStream capturedErr = new PrintStream(errors, true, StandardCharsets.UTF_8);
             DatagramSocket sender = new DatagramSocket()) {
            System.setErr(capturedErr);
            transport.start();
            send(sender, port, envelope(RaftUdpMessageType.REQUEST_VOTE_RESPONSE));
            assertTrue(uncaught.await(3, TimeUnit.SECONDS));
            assertSame(injected, escaped.get());
            assertFalse(transport.isRunning());
            assertTrue(errors.toString(StandardCharsets.UTF_8).contains("unrecoverable Raft storage failure"));
        } finally {
            transport.stop();
            System.setErr(previousErr);
        }
    }

    private void assertHandlerFailureIsIsolated(RaftUdpMessageType type) throws Exception {
        int port = availableUdpPort();
        RaftUdpBroadcastTransport transport = new RaftUdpBroadcastTransport(0, config(port));
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch failedCall = new CountDownLatch(1);
        CountDownLatch processedNext = new CountDownLatch(1);
        Runnable handler = () -> {
            if (calls.incrementAndGet() == 1) {
                failedCall.countDown();
                throw new IllegalStateException("injected handler failure");
            }
            processedNext.countDown();
        };
        attachHandlers(transport, type, handler);
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        PrintStream previousErr = System.err;
        try (PrintStream capturedErr = new PrintStream(errors, true, StandardCharsets.UTF_8);
             DatagramSocket sender = new DatagramSocket()) {
            System.setErr(capturedErr);
            transport.start();
            send(sender, port, envelope(type));
            assertTrue(failedCall.await(3, TimeUnit.SECONDS), "first packet did not reach handler");
            send(sender, port, envelope(type));
            assertTrue(processedNext.await(3, TimeUnit.SECONDS),
                    "a handler failure must not kill reception of later UDP packets");
            assertEquals(2, calls.get());
            assertTrue(transport.isRunning());
            String diagnostics = errors.toString(StandardCharsets.UTF_8);
            assertTrue(diagnostics.contains("injected handler failure"), diagnostics);
            assertTrue(diagnostics.contains("type=" + type), diagnostics);
            assertTrue(diagnostics.contains("sender=1"), diagnostics);
            assertTrue(diagnostics.contains("term=7"), diagnostics);
        } finally {
            transport.stop();
            System.setErr(previousErr);
        }
    }

    private void attachHandlers(RaftUdpBroadcastTransport transport,
                                RaftUdpMessageType failingType, Runnable handler) {
        transport.attachPreVoteHandlers(
                request -> {
                    if (failingType == RaftUdpMessageType.PRE_VOTE_REQUEST) {
                        handler.run();
                    }
                    return new PreVoteResponseMessage(7L, request.getTerm(), request.getRoundId(), true, 0);
                },
                response -> {
                    if (failingType == RaftUdpMessageType.PRE_VOTE_RESPONSE) {
                        handler.run();
                    }
                });
        transport.attachHandlers(
                response -> {
                    if (failingType == RaftUdpMessageType.REQUEST_VOTE_RESPONSE) {
                        handler.run();
                    }
                },
                (peerId, response) -> {
                    if (failingType == RaftUdpMessageType.APPEND_ENTRIES_RESPONSE) {
                        handler.run();
                    }
                });
        transport.attachRequestHandlers(
                request -> {
                    if (failingType == RaftUdpMessageType.REQUEST_VOTE_REQUEST) {
                        handler.run();
                    }
                    return new RequestVoteResponseMessage(request.getTerm(), true, 0);
                },
                request -> {
                    if (failingType == RaftUdpMessageType.APPEND_ENTRIES_REQUEST) {
                        handler.run();
                    }
                    return new AppendEntriesResponseMessage(request.getTerm(), true, 0, 0L, -1L, 0L);
                });
    }

    private RaftConfig config(int port) {
        return new RaftConfig(200, 400, 40, 7000,
                RaftTransportMode.HYBRID, port, 8192, "udp-resilience", tempDir,
                Map.of(0, new RaftPeerEndpoint(0, "127.0.0.1", 7000, 8000),
                       1, new RaftPeerEndpoint(1, "127.0.0.1", 7001, 8001)));
    }

    private static RaftUdpEnvelope envelope(RaftUdpMessageType type) {
        Object payload = switch (type) {
            case PRE_VOTE_REQUEST -> new PreVoteRequestMessage(7L, 1, 0L, 0L, "test-round");
            case PRE_VOTE_RESPONSE -> new PreVoteResponseMessage(7L, 7L, "test-round", true, 1);
            case REQUEST_VOTE_REQUEST -> new RequestVoteRequestMessage(7L, 1, 0L, 0L);
            case REQUEST_VOTE_RESPONSE -> new RequestVoteResponseMessage(7L, true, 1);
            case APPEND_ENTRIES_REQUEST -> new AppendEntriesRequestMessage(7L, 1, 0L, 0L, List.of(), 0L);
            case APPEND_ENTRIES_RESPONSE -> new AppendEntriesResponseMessage(7L, true, 1, 0L, -1L, 0L);
            default -> throw new IllegalArgumentException("Unsupported test type: " + type);
        };
        return new RaftUdpEnvelope("udp-resilience", UUID.randomUUID().toString(),
                1, 0, type, 7L, payload, 1L);
    }

    private static int availableUdpPort() throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void send(DatagramSocket sender, int port, Object payload) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(payload);
        }
        byte[] data = bytes.toByteArray();
        sender.send(new DatagramPacket(data, data.length, InetAddress.getLoopbackAddress(), port));
    }

    private static final class FailingDeserialization implements Serializable {
        private static final long serialVersionUID = 1L;

        private void readObject(ObjectInputStream input) {
            throw new IllegalStateException("injected decode failure");
        }
    }
}
