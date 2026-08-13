package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.client.connection.ClientConnection;
import it.polimi.ds.chat.client.connection.ClientConnectionGeneration;
import it.polimi.ds.chat.client.connection.ClientGenerationTestFactory;
import it.polimi.ds.chat.client.connection.ClientObjectWriter;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientMessageReceiverGenerationTest {

    @Test
    void messageReadBeforeInvalidationIsDroppedWhenReplacementDispatchWins()
            throws Exception {
        CountDownLatch oldReadyToDispatch = new CountDownLatch(1);
        CountDownLatch releaseOld = new CountDownLatch(1);
        List<String> visible = Collections.synchronizedList(new ArrayList<>());

        try (ScriptedConnection connection = new ScriptedConnection("m1", "m2")) {
            connection.open();
            ClientConnectionGeneration oldGeneration =
                    connection.getCurrentGeneration();
            Thread oldReceiver = receiverThread(
                    oldGeneration,
                    () -> {
                        oldReadyToDispatch.countDown();
                        await(releaseOld);
                    },
                    visible
            );
            oldReceiver.start();
            assertTrue(oldReadyToDispatch.await(1L, TimeUnit.SECONDS));

            // Models failure invalidation followed by reconnect. The old
            // receiver has already read m1 but has not entered the dispatch
            // gate, so replacing the generation makes that event stale.
            connection.open();
            ClientConnectionGeneration newGeneration =
                    connection.getCurrentGeneration();
            Thread newReceiver = receiverThread(
                    newGeneration,
                    () -> { },
                    visible
            );
            newReceiver.start();
            newReceiver.join(1_000L);
            assertFalse(newReceiver.isAlive());
            assertEquals(List.of("m2"), visible);

            releaseOld.countDown();
            oldReceiver.join(1_000L);
            assertFalse(oldReceiver.isAlive());
            assertEquals(List.of("m2"), visible,
                    "a stale generation must never append m1 after m2");
        } finally {
            releaseOld.countDown();
        }
    }

    @Test
    void replacementDispatchWaitsForAnAlreadyAcceptedOldDispatch()
            throws Exception {
        CountDownLatch oldDispatchEntered = new CountDownLatch(1);
        CountDownLatch releaseOldDispatch = new CountDownLatch(1);
        CountDownLatch newReadyToDispatch = new CountDownLatch(1);
        List<String> visible = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try (ScriptedConnection connection = new ScriptedConnection("m1", "m2")) {
            connection.open();
            ClientConnectionGeneration oldGeneration =
                    connection.getCurrentGeneration();
            Thread oldReceiver = receiverThread(
                    oldGeneration,
                    () -> { },
                    line -> {
                        visible.add(line);
                        oldDispatchEntered.countDown();
                        await(releaseOldDispatch);
                    }
            );
            oldReceiver.start();
            assertTrue(oldDispatchEntered.await(1L, TimeUnit.SECONDS));

            // Invalidation/publication must remain bounded even while the old
            // receiver is inside dispatch; only newer visible dispatch waits.
            Future<?> replacement = executor.submit(() -> {
                try {
                    connection.open();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            replacement.get(1L, TimeUnit.SECONDS);

            ClientConnectionGeneration newGeneration =
                    connection.getCurrentGeneration();
            Thread newReceiver = receiverThread(
                    newGeneration,
                    newReadyToDispatch::countDown,
                    visible
            );
            newReceiver.start();
            assertTrue(newReadyToDispatch.await(1L, TimeUnit.SECONDS));
            assertEquals(List.of("m1"), visible);

            releaseOldDispatch.countDown();
            oldReceiver.join(1_000L);
            newReceiver.join(1_000L);
            assertFalse(oldReceiver.isAlive());
            assertFalse(newReceiver.isAlive());
            assertEquals(List.of("m1", "m2"), visible,
                    "new-generation delivery must follow an accepted old dispatch");
        } finally {
            releaseOldDispatch.countDown();
            executor.shutdownNow();
        }
    }

    private static Thread receiverThread(
            ClientConnectionGeneration generation,
            Runnable beforeDispatch,
            List<String> visible
    ) {
        return receiverThread(generation, beforeDispatch, visible::add);
    }

    private static Thread receiverThread(
            ClientConnectionGeneration generation,
            Runnable beforeDispatch,
            java.util.function.Consumer<String> output
    ) {
        ClientMessageReceiver receiver = new ClientMessageReceiver(
                generation.getInputStream(),
                new ClientMessageSender(60_000L, "generation-client"),
                "generation-client",
                null,
                generation::isActive,
                generation::dispatchInboundIfActive,
                null,
                beforeDispatch,
                output
        );
        Thread thread = new Thread(receiver, "scripted-generation-receiver");
        thread.setDaemon(true);
        return thread;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(1L, TimeUnit.SECONDS)) {
                throw new AssertionError("scripted latch was not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("scripted wait interrupted", e);
        }
    }

    private static final class ScriptedConnection extends ClientConnection
            implements AutoCloseable {
        private final Deque<ObjectInputStream> inputs = new ArrayDeque<>();
        private long nextId;

        private ScriptedConnection(Object... inboundMessages) throws IOException {
            super("broker", 5_000);
            for (Object message : inboundMessages) {
                inputs.addLast(new OneMessageInput(message));
            }
        }

        @Override
        protected ClientConnectionGeneration createGeneration(
                String host,
                int port
        ) throws IOException {
            ObjectInputStream input = inputs.pollFirst();
            if (input == null) {
                throw new IOException("no scripted generation remains");
            }
            return ClientGenerationTestFactory.create(
                    ++nextId,
                    host,
                    port,
                    new Socket(),
                    input,
                    new ClientObjectWriter(new DiscardingObjectOutput())
            );
        }

        @Override
        public void close() {
            super.close();
        }
    }

    private static final class OneMessageInput extends ObjectInputStream {
        private final Object message;
        private boolean returned;

        private OneMessageInput(Object message) throws IOException {
            super();
            this.message = message;
        }

        @Override
        protected Object readObjectOverride() throws IOException {
            if (!returned) {
                returned = true;
                return message;
            }
            throw new EOFException("scripted input complete");
        }
    }

    private static final class DiscardingObjectOutput extends ObjectOutputStream {
        private DiscardingObjectOutput() throws IOException {
            super();
        }

        @Override
        protected void writeObjectOverride(Object object) {
        }

        @Override
        public void flush() {
        }
    }
}
