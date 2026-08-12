package it.polimi.ds.chat.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientReconnectManagerTest {

    @Test
    void retriesUntilSuccessWithBoundedBackoff() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch[] attemptSeen = {
                new CountDownLatch(1),
                new CountDownLatch(1),
                new CountDownLatch(1),
                new CountDownLatch(1)
        };
        ControlledWaiter waiter = new ControlledWaiter();
        ClientReconnectManager manager = new ClientReconnectManager(
                () -> {
                    int attempt = attempts.incrementAndGet();
                    attemptSeen[attempt - 1].countDown();
                    if (attempt < 4) {
                        throw new IOException("scripted failure " + attempt);
                    }
                },
                10L,
                20L,
                waiter
        );

        assertTrue(manager.requestReconnect());

        assertTrue(attemptSeen[0].await(1L, TimeUnit.SECONDS));
        ControlledWaiter.WaitCall first = waiter.next();
        assertEquals(10L, first.delayMillis);
        first.release.countDown();

        assertTrue(attemptSeen[1].await(1L, TimeUnit.SECONDS));
        ControlledWaiter.WaitCall second = waiter.next();
        assertEquals(20L, second.delayMillis);
        second.release.countDown();

        assertTrue(attemptSeen[2].await(1L, TimeUnit.SECONDS));
        ControlledWaiter.WaitCall capped = waiter.next();
        assertEquals(20L, capped.delayMillis);
        capped.release.countDown();

        assertTrue(attemptSeen[3].await(1L, TimeUnit.SECONDS));
        assertTrue(manager.awaitIdle(1_000L));
        assertEquals(4, attempts.get());

        manager.shutdown();
    }

    @Test
    void shutdownCancelsBackoffAndPreventsFutureReconnects() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch firstAttempt = new CountDownLatch(1);
        ControlledWaiter waiter = new ControlledWaiter();
        ClientReconnectManager manager = new ClientReconnectManager(
                () -> {
                    attempts.incrementAndGet();
                    firstAttempt.countDown();
                    throw new IOException("still unavailable");
                },
                10L,
                20L,
                waiter
        );

        assertTrue(manager.requestReconnect());
        assertTrue(firstAttempt.await(1L, TimeUnit.SECONDS));
        assertNotNull(waiter.next());

        manager.shutdown();

        assertTrue(manager.awaitIdle(1_000L));
        assertFalse(manager.requestReconnect());
        assertEquals(1, attempts.get());
    }

    @Test
    void failureOfJustInstalledGenerationQueuesAnotherReconnect() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch firstAttemptEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstAttempt = new CountDownLatch(1);
        CountDownLatch secondAttemptSeen = new CountDownLatch(1);
        ClientReconnectManager manager = new ClientReconnectManager(
                () -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt == 1) {
                        firstAttemptEntered.countDown();
                        try {
                            releaseFirstAttempt.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    } else {
                        secondAttemptSeen.countDown();
                    }
                },
                10L,
                20L,
                delay -> {
                    throw new AssertionError("successful attempts need no backoff");
                }
        );

        assertTrue(manager.requestReconnect());
        assertTrue(firstAttemptEntered.await(1L, TimeUnit.SECONDS));

        // Represents a generation installed by attempt one failing before the
        // active reconnect worker reaches its finally block.
        assertTrue(manager.requestReconnect());
        releaseFirstAttempt.countDown();

        assertTrue(secondAttemptSeen.await(1L, TimeUnit.SECONDS));
        assertTrue(manager.awaitIdle(1_000L));
        assertEquals(2, attempts.get());
        manager.shutdown();
    }

    private static final class ControlledWaiter
            implements ClientReconnectManager.BackoffWaiter {
        private final BlockingQueue<WaitCall> calls = new LinkedBlockingQueue<>();

        @Override
        public void await(long delayMillis) throws InterruptedException {
            WaitCall call = new WaitCall(delayMillis);
            calls.put(call);
            call.release.await();
        }

        private WaitCall next() throws InterruptedException {
            WaitCall call = calls.poll(1L, TimeUnit.SECONDS);
            assertNotNull(call, "reconnect loop did not enter backoff");
            return call;
        }

        private static final class WaitCall {
            private final long delayMillis;
            private final CountDownLatch release = new CountDownLatch(1);

            private WaitCall(long delayMillis) {
                this.delayMillis = delayMillis;
            }
        }
    }
}
