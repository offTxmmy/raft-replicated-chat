package it.polimi.ds.chat.client.connection;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientObjectWriterTest {

    @Test
    void concurrentProducersShareOneSerializedObjectStreamBoundary()
            throws Exception {
        int producerCount = 12;
        ConcurrencyDetectingOutputStream transport =
                new ConcurrencyDetectingOutputStream();
        ClientObjectWriter writer = new ClientObjectWriter(
                new ObjectOutputStream(transport)
        );
        CyclicBarrier start = new CyclicBarrier(producerCount + 1);
        ExecutorService executor = Executors.newFixedThreadPool(producerCount);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < producerCount; i++) {
                String payload = "payload-" + i;
                futures.add(executor.submit(() -> {
                    start.await();
                    writer.send(payload);
                    return null;
                }));
            }

            start.await(1L, TimeUnit.SECONDS);
            for (Future<?> future : futures) {
                future.get(2L, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(1, transport.maxConcurrentWrites.get());
        List<Object> objects = readAll(transport.snapshot());
        assertEquals(producerCount, objects.size());
        assertEquals(
                expectedPayloads(producerCount),
                new HashSet<>(objects)
        );
    }

    @Test
    void closingGenerationInvalidatesOnlyItsOwnWriter() throws Exception {
        CapturingOutput oldCapture = new CapturingOutput();
        CapturingOutput newCapture = new CapturingOutput();
        ClientObjectWriter oldWriter = new ClientObjectWriter(oldCapture.output);
        ClientObjectWriter newWriter = new ClientObjectWriter(newCapture.output);
        ClientConnectionGeneration oldGeneration = new ClientConnectionGeneration(
                1L,
                "127.0.0.1",
                5000,
                new Socket(),
                emptyObjectInput(),
                oldWriter
        );

        oldWriter.send("old-before-close");
        oldGeneration.close();

        assertFalse(oldGeneration.isActive());
        assertFalse(oldWriter.isActive());
        assertThrows(IOException.class, () -> oldWriter.send("stale-write"));

        assertTrue(newWriter.isActive());
        newWriter.send("new-generation");
        assertEquals(List.of("old-before-close"), readAll(oldCapture.bytes()));
        assertEquals(List.of("new-generation"), readAll(newCapture.bytes()));
    }

    @Test
    void closingGenerationInterruptsBlockedWriteBeforeWaitingForWriter()
            throws Exception {
        BlockingOutputStream transport = new BlockingOutputStream();
        ClientObjectWriter writer = new ClientObjectWriter(
                new ObjectOutputStream(transport)
        );
        ClientConnectionGeneration generation = new ClientConnectionGeneration(
                1L,
                "127.0.0.1",
                5000,
                new WriteUnblockingSocket(transport),
                emptyObjectInput(),
                writer
        );
        transport.arm();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<IOException> blockedSend = executor.submit(() -> {
                try {
                    writer.send("blocked");
                    return null;
                } catch (IOException e) {
                    return e;
                }
            });
            assertTrue(
                    transport.writeEntered.await(1L, TimeUnit.SECONDS),
                    "send did not enter the scripted blocking transport"
            );

            Future<?> close = executor.submit(generation::close);
            close.get(1L, TimeUnit.SECONDS);

            assertFalse(writer.isActive());
            assertNotNull(
                    blockedSend.get(1L, TimeUnit.SECONDS),
                    "socket close must abort the in-flight write"
            );
            assertThrows(IOException.class, () -> writer.send("after-close"));
        } finally {
            executor.shutdownNow();
        }
    }

    private static ObjectInputStream emptyObjectInput() throws Exception {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        new ObjectOutputStream(header).close();
        return new ObjectInputStream(new ByteArrayInputStream(header.toByteArray()));
    }

    private static Set<Object> expectedPayloads(int count) {
        Set<Object> expected = new HashSet<>();
        for (int i = 0; i < count; i++) {
            expected.add("payload-" + i);
        }
        return expected;
    }

    private static List<Object> readAll(byte[] bytes) throws Exception {
        List<Object> result = new ArrayList<>();
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes)
        )) {
            while (true) {
                result.add(input.readObject());
            }
        } catch (EOFException expected) {
            return result;
        }
    }

    private static final class CapturingOutput {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final ObjectOutputStream output = new ObjectOutputStream(bytes);

        private CapturingOutput() throws IOException {
        }

        private byte[] bytes() {
            return bytes.toByteArray();
        }
    }

    private static final class ConcurrencyDetectingOutputStream
            extends OutputStream {
        private final ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        private final AtomicInteger activeWrites = new AtomicInteger();
        private final AtomicInteger maxConcurrentWrites = new AtomicInteger();

        @Override
        public void write(int value) {
            enter();
            try {
                delegate.write(value);
            } finally {
                activeWrites.decrementAndGet();
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            enter();
            try {
                delegate.write(bytes, offset, length);
            } finally {
                activeWrites.decrementAndGet();
            }
        }

        private void enter() {
            int current = activeWrites.incrementAndGet();
            maxConcurrentWrites.accumulateAndGet(current, Math::max);
            Thread.yield();
        }

        private byte[] snapshot() {
            return delegate.toByteArray();
        }
    }

    private static final class BlockingOutputStream extends OutputStream {
        private final CountDownLatch writeEntered = new CountDownLatch(1);
        private final CountDownLatch transportClosed = new CountDownLatch(1);
        private final AtomicBoolean armed = new AtomicBoolean(false);

        @Override
        public void write(int value) throws IOException {
            blockAfterHeader();
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException {
            blockAfterHeader();
        }

        private void arm() {
            armed.set(true);
        }

        private void releaseBlockedWrite() {
            transportClosed.countDown();
        }

        private void blockAfterHeader() throws IOException {
            if (!armed.get()) {
                return;
            }

            writeEntered.countDown();
            try {
                transportClosed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted scripted write", e);
            }
            throw new IOException("scripted transport closed");
        }
    }

    private static final class WriteUnblockingSocket extends Socket {
        private final BlockingOutputStream transport;

        private WriteUnblockingSocket(BlockingOutputStream transport) {
            this.transport = transport;
        }

        @Override
        public synchronized void close() throws IOException {
            transport.releaseBlockedWrite();
            super.close();
        }
    }
}
