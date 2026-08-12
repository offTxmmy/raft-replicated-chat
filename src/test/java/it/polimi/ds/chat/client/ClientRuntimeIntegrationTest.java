package it.polimi.ds.chat.client;

import it.polimi.ds.chat.client.connection.ClientConnection;
import it.polimi.ds.chat.client.connection.DirectoryAwareClientConnection;
import it.polimi.ds.chat.client.discovery.ClientDirectory;
import it.polimi.ds.chat.client.messaging.ClientMessageSender;
import it.polimi.ds.chat.directory.DirectoryService;
import it.polimi.ds.chat.protocol.client.ClientAckMessages;
import it.polimi.ds.chat.protocol.directory.DirectoryRegisterMessage;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientRuntimeIntegrationTest {

    @Test
    void realDirectoryPostInstallFailureExcludesPreferredBrokerAndPreservesPendingFifo()
            throws Exception {
        DirectoryService directory = new DirectoryService(Map.of());
        try (ScriptedBroker preferred = new ScriptedBroker();
             ScriptedBroker alternate = new ScriptedBroker()) {
            directory.start(0, 0);
            int directoryBrokerPort = boundPort(directory, "getBoundBrokerPortForTesting");
            int directoryClientPort = boundPort(directory, "getBoundClientPortForTesting");

            try (RegistrationConnection first = register(
                    directoryBrokerPort, 1, preferred.port());
                 RegistrationConnection second = register(
                         directoryBrokerPort, 2, alternate.port())) {
                assertTrue(awaitActiveBroker(
                        directory, 1, preferred.port()));
                assertTrue(awaitActiveBroker(
                        directory, 2, alternate.port()));

                ClientDirectory clientDirectory = new ClientDirectory(
                        "127.0.0.1",
                        directoryClientPort
                );
                assertEquals(1, clientDirectory.getBestBroker().getBrokerId(),
                        "A must remain the Directory's preferred broker without exclusions");

                DirectoryAwareClientConnection connection =
                        new DirectoryAwareClientConnection(
                                "127.0.0.1",
                                directoryClientPort
                        );
                ClientMessageSender sender = new ClientMessageSender(
                        60_000L,
                        "directory-post-install"
                );
                sender.sendUserMessage("one");
                sender.sendUserMessage("two");
                List<Long> backoffs = new CopyOnWriteArrayList<>();
                ClientRuntime runtime = new ClientRuntime(
                        connection,
                        sender,
                        "alice",
                        "directory-post-install",
                        10L,
                        20L,
                        backoffs::add
                );

                try {
                    runtime.start();

                    String join = "JOIN_ID directory-post-install alice";
                    String firstMessage = "MSG directory-post-install 1 one";
                    String secondMessage = "MSG directory-post-install 2 two";

                    assertEquals(join,
                            preferred.awaitString(value -> value.equals(join)));
                    assertEquals(firstMessage,
                            preferred.awaitString(value -> value.equals(firstMessage)));
                    assertFalse(preferred.observeString(
                                    value -> value.equals(secondMessage), 100L),
                            "seq2 must remain blocked while seq1 is unacknowledged");

                    // The chat socket fails after a complete installation, but
                    // A's independent Directory registration remains alive.
                    preferred.dropConnection();

                    assertEquals(join,
                            alternate.awaitString(value -> value.equals(join)));
                    assertEquals(firstMessage,
                            alternate.awaitString(value -> value.equals(firstMessage)));
                    assertEquals(List.of(), backoffs,
                            "the first post-failure lookup must already exclude A");
                    assertEquals(1, clientDirectory.getBestBroker().getBrokerId(),
                            "A must still be registered and preferred for callers without exclusions");
                    assertFalse(alternate.observeString(
                                    value -> value.equals(secondMessage), 100L),
                            "seq2 must not overtake the retried seq1");

                    alternate.send(ClientAckMessages.buildAck(
                            "directory-post-install", 1L));
                    assertEquals(secondMessage,
                            alternate.awaitString(value -> value.equals(secondMessage)));
                    alternate.send(ClientAckMessages.buildAck(
                            "directory-post-install", 2L));

                    runtime.shutdown(true);
                    assertEquals("QUIT",
                            alternate.awaitString(value -> value.equals("QUIT")));
                    assertFalse(runtime.isRunning());

                    assertEquals(1, preferred.receivedStringCount(firstMessage));
                    assertEquals(0, preferred.receivedStringCount(secondMessage));
                    assertEquals(1, alternate.receivedStringCount(firstMessage),
                            "seq1 must be retried exactly once on B");
                    assertEquals(1, alternate.receivedStringCount(secondMessage));
                } finally {
                    runtime.shutdown(false);
                }
            }
        } finally {
            directory.stop();
        }
    }

    @Test
    void realDirectoryExclusionRoutesBootstrapAroundAStillRegisteredDeadBroker()
            throws Exception {
        DirectoryService directory = new DirectoryService(Map.of());
        try (RejectingBroker preferredButDead = new RejectingBroker();
             ScriptedBroker alternate = new ScriptedBroker()) {
            directory.start(0, 0);
            int directoryBrokerPort = boundPort(directory, "getBoundBrokerPortForTesting");
            int directoryClientPort = boundPort(directory, "getBoundClientPortForTesting");

            try (RegistrationConnection first = register(
                    directoryBrokerPort, 1, preferredButDead.port());
                 RegistrationConnection second = register(
                         directoryBrokerPort, 2, alternate.port())) {
                awaitDirectorySelection(directoryClientPort, 1);

                DirectoryAwareClientConnection connection =
                        new DirectoryAwareClientConnection("127.0.0.1", directoryClientPort);
                ClientMessageSender sender = new ClientMessageSender(
                        60_000L,
                        "directory-bootstrap"
                );
                sender.sendUserMessage("survives-stale-bootstrap");
                List<Long> backoffs = new CopyOnWriteArrayList<>();
                ClientRuntime runtime = new ClientRuntime(
                        connection,
                        sender,
                        "alice",
                        "directory-bootstrap",
                        10L,
                        20L,
                        backoffs::add
                );

                runtime.start();

                assertEquals("JOIN_ID directory-bootstrap alice",
                        alternate.awaitString(value -> value.startsWith("JOIN_ID ")));
                assertEquals("MSG directory-bootstrap 1 survives-stale-bootstrap",
                        alternate.awaitString(value -> value.startsWith("MSG ")));
                assertEquals(List.of(10L), backoffs);
                runtime.shutdown(false);
            }
        } finally {
            directory.stop();
        }
    }

    @Test
    void initialConnectionFailureUsesEventualReconnectAndRetainsQueuedHead()
            throws Exception {
        try (ScriptedBroker broker = new ScriptedBroker()) {
            AtomicInteger attempts = new AtomicInteger();
            ClientConnection connection = new ClientConnection("127.0.0.1", broker.port()) {
                @Override
                public void open() throws IOException {
                    if (attempts.incrementAndGet() == 1) {
                        throw new IOException("scripted stale bootstrap endpoint");
                    }
                    openTo("127.0.0.1", broker.port());
                }
            };
            ClientMessageSender sender = new ClientMessageSender(60_000L, "bootstrap-client");
            sender.sendUserMessage("queued-before-connect");
            List<Long> backoffs = new CopyOnWriteArrayList<>();
            ClientRuntime runtime = new ClientRuntime(
                    connection,
                    sender,
                    "alice",
                    "bootstrap-client",
                    10L,
                    20L,
                    backoffs::add
            );

            runtime.start();

            assertEquals("JOIN_ID bootstrap-client alice",
                    broker.awaitString(value -> value.startsWith("JOIN_ID ")));
            assertEquals("MSG bootstrap-client 1 queued-before-connect",
                    broker.awaitString(value -> value.startsWith("MSG ")));
            assertEquals(2, attempts.get());
            assertEquals(List.of(10L), backoffs);
            runtime.shutdown(false);
        }
    }

    @Test
    void staleAttemptThenLiveReconnectPreservesJoinAndPendingFifo()
            throws Exception {
        try (ScriptedBroker firstBroker = new ScriptedBroker();
             ScriptedBroker replacementBroker = new ScriptedBroker()) {
            ScriptedClientConnection connection = new ScriptedClientConnection(
                    firstBroker.port(),
                    replacementBroker.port()
            );
            connection.open();

            ClientMessageSender sender = new ClientMessageSender(
                    60_000L,
                    "client-runtime"
            );
            sender.sendUserMessage("one");
            sender.sendUserMessage("two");

            List<Long> backoffs = new CopyOnWriteArrayList<>();
            ClientRuntime runtime = new ClientRuntime(
                    connection,
                    sender,
                    "alice",
                    "client-runtime",
                    10L,
                    20L,
                    backoffs::add
            );

            runtime.start();
            assertEquals(
                    "JOIN_ID client-runtime alice",
                    firstBroker.awaitString(value -> value.startsWith("JOIN_ID "))
            );
            assertEquals(
                    "MSG client-runtime 1 one",
                    firstBroker.awaitString(value -> value.startsWith("MSG "))
            );

            firstBroker.dropConnection();

            assertEquals(
                    "JOIN_ID client-runtime alice",
                    replacementBroker.awaitString(
                            value -> value.startsWith("JOIN_ID ")
                    )
            );
            assertEquals(
                    "MSG client-runtime 1 one",
                    replacementBroker.awaitString(value -> value.startsWith("MSG "))
            );
            assertEquals(3, connection.openAttempts.get());
            assertEquals(List.of(10L), backoffs);

            // Before ACK(1), no later sequence may be sent on the replacement.
            assertFalse(
                    replacementBroker.observeString(
                            value -> value.equals("MSG client-runtime 2 two"),
                            100L
                    )
            );

            replacementBroker.send(
                    ClientAckMessages.buildAck("client-runtime", 1L)
            );
            assertEquals(
                    "MSG client-runtime 2 two",
                    replacementBroker.awaitString(
                            value -> value.equals("MSG client-runtime 2 two")
                    )
            );

            replacementBroker.send(
                    ClientAckMessages.buildAck("client-runtime", 2L)
            );
            runtime.shutdown(true);

            assertEquals(
                    "QUIT",
                    replacementBroker.awaitString(value -> value.equals("QUIT"))
            );
            assertFalse(runtime.isRunning());
        }
    }

    private static final class ScriptedClientConnection extends ClientConnection {
        private final int firstPort;
        private final int replacementPort;
        private final AtomicInteger openAttempts = new AtomicInteger();

        private ScriptedClientConnection(int firstPort, int replacementPort) {
            super("127.0.0.1", firstPort);
            this.firstPort = firstPort;
            this.replacementPort = replacementPort;
        }

        @Override
        public void open() throws IOException {
            int attempt = openAttempts.incrementAndGet();
            if (attempt == 1) {
                openTo("127.0.0.1", firstPort);
                return;
            }
            if (attempt == 2) {
                throw new IOException("Directory still returned stale broker");
            }
            openTo("127.0.0.1", replacementPort);
        }
    }

    private static int boundPort(DirectoryService directory, String methodName) {
        try {
            java.lang.reflect.Method method =
                    DirectoryService.class.getDeclaredMethod(methodName);
            method.setAccessible(true);
            return (int) method.invoke(directory);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to read Directory test port", e);
        }
    }

    private static boolean awaitActiveBroker(
            DirectoryService directory,
            int brokerId,
            int clientPort
    ) {
        try {
            java.lang.reflect.Method method = DirectoryService.class.getDeclaredMethod(
                    "awaitActiveBrokerForTesting",
                    int.class,
                    String.class,
                    int.class,
                    long.class,
                    TimeUnit.class
            );
            method.setAccessible(true);
            return (boolean) method.invoke(
                    directory,
                    brokerId,
                    "127.0.0.1",
                    clientPort,
                    2L,
                    TimeUnit.SECONDS
            );
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Unable to await Directory registration", e);
        }
    }

    private static RegistrationConnection register(
            int directoryPort,
            int brokerId,
            int clientPort
    ) throws IOException {
        Socket socket = new Socket("127.0.0.1", directoryPort);
        ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
        output.writeObject(new DirectoryRegisterMessage(brokerId, "127.0.0.1", clientPort));
        output.flush();
        return new RegistrationConnection(socket, output);
    }

    private static void awaitDirectorySelection(int directoryPort, int expectedBrokerId)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadline) {
            ClientDirectory.BrokerInfo info =
                    new ClientDirectory("127.0.0.1", directoryPort).getBestBroker();
            if (info != null && info.getBrokerId() == expectedBrokerId) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Directory did not publish broker " + expectedBrokerId);
    }

    private record RegistrationConnection(Socket socket, ObjectOutputStream output)
            implements AutoCloseable {
        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static final class RejectingBroker implements AutoCloseable {
        private final ServerSocket serverSocket = new ServerSocket(0);
        private final Thread thread;

        private RejectingBroker() throws IOException {
            thread = new Thread(() -> {
                try (Socket ignored = serverSocket.accept()) {
                    // Close before the object-stream handshake completes.
                } catch (IOException ignored) {
                }
            }, "rejecting-client-broker");
            thread.setDaemon(true);
            thread.start();
        }

        private int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            thread.interrupt();
            thread.join(1_000L);
        }
    }

    private static final class ScriptedBroker implements AutoCloseable {
        private final ServerSocket serverSocket = new ServerSocket(0);
        private final BlockingQueue<Object> received = new LinkedBlockingQueue<>();
        private final List<String> receivedStrings = new CopyOnWriteArrayList<>();
        private final CountDownLatch connected = new CountDownLatch(1);
        private final Thread serverThread;

        private volatile Socket socket;
        private volatile ObjectOutputStream output;

        private ScriptedBroker() throws IOException {
            serverThread = new Thread(this::serve, "scripted-client-broker");
            serverThread.setDaemon(true);
            serverThread.start();
        }

        private int port() {
            return serverSocket.getLocalPort();
        }

        private void serve() {
            try (Socket accepted = serverSocket.accept();
                 ObjectInputStream input = new ObjectInputStream(
                         accepted.getInputStream()
                 );
                 ObjectOutputStream brokerOutput = new ObjectOutputStream(
                         accepted.getOutputStream()
                 )) {
                brokerOutput.flush();
                socket = accepted;
                output = brokerOutput;
                connected.countDown();

                while (true) {
                    Object object = input.readObject();
                    if (object instanceof String value) {
                        receivedStrings.add(value);
                    }
                    received.put(object);
                }
            } catch (EOFException ignored) {
            } catch (Exception ignored) {
            } finally {
                connected.countDown();
            }
        }

        private String awaitString(Predicate<String> predicate)
                throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
            while (true) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0L, "expected client object was not received");
                Object object = received.poll(remaining, TimeUnit.NANOSECONDS);
                assertNotNull(object, "expected client object was not received");
                if (object instanceof String value && predicate.test(value)) {
                    return value;
                }
            }
        }

        private boolean observeString(Predicate<String> predicate, long timeoutMs)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                Object object = received.poll(remaining, TimeUnit.NANOSECONDS);
                if (object == null) {
                    return false;
                }
                if (object instanceof String value && predicate.test(value)) {
                    return true;
                }
            }
        }

        private void send(Object object) throws Exception {
            assertTrue(connected.await(1L, TimeUnit.SECONDS));
            synchronized (this) {
                output.writeObject(object);
                output.flush();
            }
        }

        private void dropConnection() throws Exception {
            assertTrue(connected.await(1L, TimeUnit.SECONDS));
            socket.close();
        }

        private int receivedStringCount(String expected) {
            return (int) receivedStrings.stream()
                    .filter(expected::equals)
                    .count();
        }

        @Override
        public void close() throws Exception {
            Socket currentSocket = socket;
            if (currentSocket != null) {
                currentSocket.close();
            }
            serverSocket.close();
            serverThread.interrupt();
            serverThread.join(1_000L);
        }
    }
}
