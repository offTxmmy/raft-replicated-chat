package it.polimi.ds.chat.client;

import it.polimi.ds.chat.client.connection.ClientConnection;
import it.polimi.ds.chat.client.connection.ClientConnectionGeneration;
import it.polimi.ds.chat.client.connection.ClientGenerationTestFactory;
import it.polimi.ds.chat.client.connection.ClientObjectWriter;
import it.polimi.ds.chat.client.messaging.ClientMessageSender;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientRuntimeBlockedWriteTest {

    @Test
    void receiverFailureClosesSocketBeforeDetachingBlockedSender()
            throws Exception {
        try (Scenario scenario = new Scenario()) {
            Future<?> blockedSend = scenario.blockChatWrite();

            // Fail only the receive side. The sender remains blocked until the
            // runtime failure callback closes the generation's socket.
            scenario.input.failRead();

            assertTrue(
                    scenario.socket.closed.await(1L, TimeUnit.SECONDS),
                    "failure teardown waited on sender state before socket close"
            );
            blockedSend.get(1L, TimeUnit.SECONDS);
            assertFalse(scenario.generation.isActive());

            scenario.runtime.shutdown(false);
            assertFalse(scenario.runtime.isRunning());
        }
    }

    @Test
    void manualQuitRemainsBoundedWhenChatWriteIsBlocked() throws Exception {
        try (Scenario scenario = new Scenario()) {
            Future<?> blockedSend = scenario.blockChatWrite();
            Future<?> shutdown = scenario.executor.submit(
                    () -> scenario.runtime.shutdown(true)
            );

            assertTrue(
                    scenario.socket.closed.await(1L, TimeUnit.SECONDS),
                    "bounded QUIT grace did not fall back to socket close"
            );
            shutdown.get(1L, TimeUnit.SECONDS);
            blockedSend.get(1L, TimeUnit.SECONDS);
            assertFalse(scenario.runtime.isRunning());
            assertFalse(scenario.generation.isActive());
        }
    }

    private static final class Scenario implements AutoCloseable {
        private final BlockingMessageOutput output =
                new BlockingMessageOutput();
        private final FailingObjectInput input = new FailingObjectInput();
        private final CloseAwareSocket socket = new CloseAwareSocket(output, input);
        private final ScriptedConnection connection =
                new ScriptedConnection(socket, input, output);
        private final ClientMessageSender sender =
                new ClientMessageSender(60_000L, "blocked-client");
        private final ClientRuntime runtime = new ClientRuntime(
                connection,
                sender,
                "alice",
                "blocked-client"
        );
        private final ExecutorService executor = Executors.newFixedThreadPool(2);
        private final ClientConnectionGeneration generation;

        private Scenario() throws Exception {
            connection.open();
            generation = connection.getCurrentGeneration();
            runtime.start();
        }

        private Future<?> blockChatWrite() throws Exception {
            Future<?> send = executor.submit(
                    () -> sender.sendUserMessage("blocked")
            );
            assertTrue(
                    output.chatWriteEntered.await(1L, TimeUnit.SECONDS),
                    "chat send did not enter the scripted blocking writer"
            );
            return send;
        }

        @Override
        public void close() {
            runtime.shutdown(false);
            socket.releaseAll();
            executor.shutdownNow();
        }
    }

    private static final class ScriptedConnection extends ClientConnection {
        private final CloseAwareSocket socket;
        private final ObjectInputStream input;
        private final ObjectOutputStream output;
        private final AtomicInteger attempts = new AtomicInteger();

        private ScriptedConnection(CloseAwareSocket socket,
                                   ObjectInputStream input,
                                   ObjectOutputStream output) {
            super("broker", 5000);
            this.socket = socket;
            this.input = input;
            this.output = output;
        }

        @Override
        protected ClientConnectionGeneration createGeneration(
                String host,
                int port
        ) throws IOException {
            int attempt = attempts.incrementAndGet();
            if (attempt > 1) {
                throw new IOException("replacement deliberately unavailable");
            }
            return ClientGenerationTestFactory.create(
                    attempt,
                    host,
                    port,
                    socket,
                    input,
                    new ClientObjectWriter(output)
            );
        }
    }

    private static final class BlockingMessageOutput
            extends ObjectOutputStream {
        private final CountDownLatch chatWriteEntered = new CountDownLatch(1);
        private final CountDownLatch transportClosed = new CountDownLatch(1);

        private BlockingMessageOutput() throws IOException {
            super();
        }

        @Override
        protected void writeObjectOverride(Object object) throws IOException {
            if (!(object instanceof String line) || !line.startsWith("MSG ")) {
                return;
            }

            chatWriteEntered.countDown();
            try {
                transportClosed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("scripted send interrupted", e);
            }
            throw new IOException("scripted transport closed");
        }

        @Override
        public void flush() {
            // Override mode has no underlying stream; writes are scripted above.
        }

        private void closeTransport() {
            transportClosed.countDown();
        }
    }

    private static final class FailingObjectInput extends ObjectInputStream {
        private final CountDownLatch failRead = new CountDownLatch(1);

        private FailingObjectInput() throws IOException {
            super();
        }

        @Override
        protected Object readObjectOverride() throws IOException {
            try {
                failRead.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("scripted receive interrupted", e);
            }
            throw new IOException("scripted receive failure");
        }

        private void failRead() {
            failRead.countDown();
        }
    }

    private static final class CloseAwareSocket extends Socket {
        private final BlockingMessageOutput output;
        private final FailingObjectInput input;
        private final CountDownLatch closed = new CountDownLatch(1);

        private CloseAwareSocket(BlockingMessageOutput output,
                                 FailingObjectInput input) {
            this.output = output;
            this.input = input;
        }

        @Override
        public synchronized void close() throws IOException {
            releaseAll();
            super.close();
        }

        private void releaseAll() {
            output.closeTransport();
            input.failRead();
            closed.countDown();
        }
    }
}
