package it.polimi.ds.chat.client.connection;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientConnectionTest {

    @Test
    void replacementInvalidatesOldGenerationAndStaleCloseCannotCloseNewOne()
            throws Exception {
        FakeClientConnection connection = new FakeClientConnection();

        connection.open();
        ClientConnectionGeneration first = connection.getCurrentGeneration();
        connection.open();
        ClientConnectionGeneration second = connection.getCurrentGeneration();

        assertNotSame(first, second);
        assertFalse(first.isActive());
        assertTrue(second.isActive());
        assertThrows(IOException.class, () -> first.getWriter().send("stale"));

        // A delayed callback retaining generation one must not detach generation two.
        connection.closeGeneration(first);
        assertTrue(second.isActive());
        assertTrue(connection.isOpen());

        second.getWriter().send("current");
        connection.close();
        assertFalse(second.isActive());
    }

    @Test
    void shutdownPreventsInFlightOpenFromPublishingALateGeneration()
            throws Exception {
        BlockingClientConnection connection = new BlockingClientConnection();
        CompletableFuture<Throwable> openResult = CompletableFuture.supplyAsync(() -> {
            try {
                connection.open();
                return null;
            } catch (Throwable failure) {
                return failure;
            }
        });

        assertTrue(connection.generationCreated.await(1L, TimeUnit.SECONDS));
        connection.shutdown();
        connection.releaseGeneration.countDown();

        Throwable failure = openResult.get(1L, TimeUnit.SECONDS);
        assertTrue(failure instanceof IOException);
        assertFalse(connection.isOpen());
        assertFalse(connection.createdGeneration.isActive());
    }

    private static final class FakeClientConnection extends ClientConnection {
        private final AtomicLong ids = new AtomicLong();
        private final List<ByteArrayOutputStream> outputs = new ArrayList<>();

        private FakeClientConnection() {
            super("broker", 5000);
        }

        @Override
        protected ClientConnectionGeneration createGeneration(String host, int port)
                throws IOException {
            try {
                ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
                outputs.add(outputBytes);
                ClientObjectWriter writer = new ClientObjectWriter(
                        new ObjectOutputStream(outputBytes)
                );
                return new ClientConnectionGeneration(
                        ids.incrementAndGet(),
                        host,
                        port,
                        new Socket(),
                        emptyInput(),
                        writer
                );
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        private ObjectInputStream emptyInput() throws Exception {
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            new ObjectOutputStream(header).close();
            return new ObjectInputStream(
                    new ByteArrayInputStream(header.toByteArray())
            );
        }
    }

    private static final class BlockingClientConnection extends ClientConnection {
        private final CountDownLatch generationCreated = new CountDownLatch(1);
        private final CountDownLatch releaseGeneration = new CountDownLatch(1);
        private volatile ClientConnectionGeneration createdGeneration;

        private BlockingClientConnection() {
            super("broker", 5000);
        }

        @Override
        protected ClientConnectionGeneration createGeneration(String host, int port)
                throws IOException {
            try {
                ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
                createdGeneration = new ClientConnectionGeneration(
                        1L,
                        host,
                        port,
                        new Socket(),
                        emptyInput(),
                        new ClientObjectWriter(new ObjectOutputStream(outputBytes))
                );
                generationCreated.countDown();
                releaseGeneration.await();
                return createdGeneration;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        private ObjectInputStream emptyInput() throws Exception {
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            new ObjectOutputStream(header).close();
            return new ObjectInputStream(new ByteArrayInputStream(header.toByteArray()));
        }
    }
}
