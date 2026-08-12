package it.polimi.ds.chat.client.connection;

import it.polimi.ds.chat.client.discovery.ClientDirectory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectoryAwareClientConnectionTest {

    @Test
    void recentlyFailedDirectoryEndpointIsSkippedUntilQuarantineExpires() {
        AtomicLong clock = new AtomicLong(100L);
        ClientDirectory.BrokerInfo stale = new ClientDirectory.BrokerInfo(
                "dead-broker",
                5000,
                1
        );
        AtomicInteger socketAttempts = new AtomicInteger();
        DirectoryAwareClientConnection connection =
                new DirectoryAwareClientConnection(
                        "directory",
                        60001,
                        excludedBrokerIds -> stale,
                        clock::get,
                        1_000L
                ) {
                    @Override
                    protected ClientConnectionGeneration createGeneration(
                            String host,
                            int port
                    ) throws IOException {
                        socketAttempts.incrementAndGet();
                        throw new IOException("scripted connect failure");
                    }
                };

        connection.markEndpointFailed(1, "dead-broker", 5000);
        assertTrue(connection.isTemporarilyExcluded("dead-broker", 5000));

        IOException excluded = assertThrows(IOException.class, connection::open);
        assertTrue(excluded.getMessage().contains("recently failed"));
        assertEquals(0, socketAttempts.get());

        clock.set(1_101L);
        assertFalse(connection.isTemporarilyExcluded("dead-broker", 5000));
        assertThrows(IOException.class, connection::open);
        assertEquals(1, socketAttempts.get());
        assertTrue(
                connection.isTemporarilyExcluded("dead-broker", 5000),
                "a failed post-expiry connect must renew quarantine"
        );
    }

    @Test
    void excludingDeadEndpointDoesNotExcludeDifferentLiveSelection() {
        AtomicLong clock = new AtomicLong(50L);
        AtomicInteger lookupCount = new AtomicInteger();
        AtomicInteger socketAttempts = new AtomicInteger();
        DirectoryAwareClientConnection connection =
                new DirectoryAwareClientConnection(
                        "directory",
                        60001,
                        excludedBrokerIds -> lookupCount.incrementAndGet() == 1
                                ? new ClientDirectory.BrokerInfo("dead", 5000, 1)
                                : new ClientDirectory.BrokerInfo("live", 5001, 2),
                        clock::get,
                        1_000L
                ) {
                    @Override
                    protected ClientConnectionGeneration createGeneration(
                            String host,
                            int port
                    ) throws IOException {
                        socketAttempts.incrementAndGet();
                        throw new IOException("attempted " + host + ":" + port);
                    }
                };

        assertThrows(IOException.class, connection::open);
        assertTrue(connection.isTemporarilyExcluded("dead", 5000));

        IOException liveAttempt = assertThrows(IOException.class, connection::open);
        assertTrue(liveAttempt.getMessage().contains("live:5001"));
        assertEquals(2, socketAttempts.get());
    }

    @Test
    void failureAfterSuccessfulOpenExcludesThatBrokerOnNextLookup()
            throws Exception {
        AtomicInteger lookupCount = new AtomicInteger();
        AtomicLong generationIds = new AtomicLong();
        AtomicReference<Set<Integer>> retryExclusions = new AtomicReference<>();
        DirectoryAwareClientConnection connection =
                new DirectoryAwareClientConnection(
                        "directory",
                        60001,
                        excludedBrokerIds -> {
                            if (lookupCount.getAndIncrement() == 0) {
                                return new ClientDirectory.BrokerInfo(
                                        "preferred",
                                        5000,
                                        1
                                );
                            }
                            retryExclusions.set(Set.copyOf(excludedBrokerIds));
                            return new ClientDirectory.BrokerInfo(
                                    "alternate",
                                    5001,
                                    2
                            );
                        },
                        () -> 100L,
                        1_000L
                ) {
                    @Override
                    protected ClientConnectionGeneration createGeneration(
                            String host,
                            int port
                    ) throws IOException {
                        return inMemoryGeneration(
                                generationIds.incrementAndGet(),
                                host,
                                port
                        );
                    }
                };

        connection.open();
        ClientConnectionGeneration failed = connection.getCurrentGeneration();
        connection.markEndpointFailed(failed);
        connection.closeGeneration(failed);

        connection.open();

        assertEquals(Set.of(1), retryExclusions.get());
        assertEquals("alternate", connection.getHost());
        assertEquals(5001, connection.getPort());
        connection.close();
    }

    private static ClientConnectionGeneration inMemoryGeneration(
            long id,
            String host,
            int port
    ) throws IOException {
        ByteArrayOutputStream inputHeader = new ByteArrayOutputStream();
        try (ObjectOutputStream headerWriter =
                     new ObjectOutputStream(inputHeader)) {
            headerWriter.flush();
        }
        ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(inputHeader.toByteArray())
        );
        ClientObjectWriter writer = new ClientObjectWriter(
                new ObjectOutputStream(new ByteArrayOutputStream())
        );
        return new ClientConnectionGeneration(
                id,
                host,
                port,
                new Socket(),
                input,
                writer
        );
    }
}
