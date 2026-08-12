package it.polimi.ds.chat.broker.session;

import it.polimi.ds.chat.TestConfigs;
import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.ordering.api.OrderingService;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.chat.ChatReqMessage;
import it.polimi.ds.chat.protocol.client.ClientJoinMessage;
import it.polimi.ds.chat.protocol.client.ClientQuitMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientHandlerOutboundTest {

    @Test
    void fullSlowConsumerQueueIsolatesOnlyThatSessionWithoutBlockingFanOut() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        broker.setOrderingService(new ImmediateOrderingService());

        ClientHandler slow = new ClientHandler(new Socket(), broker, 2);
        ClientHandler healthy = new ClientHandler(new Socket(), broker, 8);
        BlockingObjectOutputStream slowOutput = new BlockingObjectOutputStream();
        RecordingObjectOutputStream healthyOutput = new RecordingObjectOutputStream(4);
        slow.startOutboundWorker(slowOutput);
        healthy.startOutboundWorker(healthyOutput);
        assertTrue(broker.activateClient(slow));
        assertTrue(broker.activateClient(healthy));

        broker.onChatDeliver(1L, "alice", null, "one");
        assertTrue(slowOutput.writeStarted.await(1, TimeUnit.SECONDS));

        // WELCOME is blocked in the slow writer while the first two chats fill
        // its bounded queue. The third chat must close only that session.
        broker.onChatDeliver(2L, "alice", null, "two");
        assertEquals(2, slow.pendingOutboundMessages());
        assertTimeout(Duration.ofSeconds(1), () ->
                broker.onChatDeliver(3L, "alice", null, "three"));

        assertTrue(healthyOutput.allWritten.await(1, TimeUnit.SECONDS));
        assertTrue(slow.isSessionClosed());
        assertFalse(healthy.isSessionClosed());
        assertEquals(
                List.of(
                        "Welcome anonymous",
                        "MSG 1 alice:one",
                        "MSG 2 alice:two",
                        "MSG 3 alice:three"),
                healthyOutput.objects);

        slowOutput.releaseWrite.countDown();
        healthy.closeSession();
    }

    @Test
    void concurrentProducersStillUseExactlyOneObjectStreamWriter() throws Exception {
        Broker broker = new Broker(TestConfigs.raftBrokerConfig(1, 5000));
        ClientHandler handler = new ClientHandler(new Socket(), broker, 128);
        GuardedObjectOutputStream output = new GuardedObjectOutputStream(64);
        handler.startOutboundWorker(output);

        int producers = 4;
        int messagesPerProducer = 16;
        CountDownLatch ready = new CountDownLatch(producers);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(producers);
        for (int producer = 0; producer < producers; producer++) {
            int id = producer;
            executor.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < messagesPerProducer; i++) {
                        handler.sendLine(id + ":" + i);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        assertTrue(ready.await(1, TimeUnit.SECONDS));
        start.countDown();
        assertTrue(output.allWritten.await(2, TimeUnit.SECONDS));
        executor.shutdownNow();

        assertFalse(output.concurrentWriteObserved);
        assertEquals(1, output.writerThreads.size());
        assertEquals(64, output.objects.size());
        handler.closeSession();
    }

    @Test
    void joinAndQuitDoNotCreateLocallySequencedChatMessages() {
        RecordingBroker broker = new RecordingBroker();
        ClientHandler handler = new ClientHandler(new Socket(), broker);

        handler.handleCommand(ClientJoinMessage.joinCommand("alice", "client-a"));
        handler.handleCommand(ClientQuitMessage.quitCommand());

        assertEquals(1, broker.activationCount.get());
        assertEquals(0, broker.chatDeliveryCount.get(),
                "JOIN/LEAVE are session control, not globally ordered chat MSG records");
        assertTrue(handler.isSessionClosed());
    }

    private static class RecordingObjectOutputStream extends ObjectOutputStream {
        protected final List<Object> objects = Collections.synchronizedList(new ArrayList<>());
        protected final CountDownLatch allWritten;

        private RecordingObjectOutputStream(int expectedObjects) throws IOException {
            super();
            allWritten = new CountDownLatch(expectedObjects);
        }

        @Override
        protected void writeObjectOverride(Object obj) {
            objects.add(obj);
            allWritten.countDown();
        }

        @Override
        public void flush() {
        }

        @Override
        public void reset() {
        }
    }

    private static final class BlockingObjectOutputStream extends ObjectOutputStream {
        private final CountDownLatch writeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseWrite = new CountDownLatch(1);

        private BlockingObjectOutputStream() throws IOException {
            super();
        }

        @Override
        protected void writeObjectOverride(Object obj) throws IOException {
            writeStarted.countDown();
            try {
                releaseWrite.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("blocked writer interrupted", e);
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void reset() {
        }
    }

    private static final class GuardedObjectOutputStream extends RecordingObjectOutputStream {
        private final Set<String> writerThreads = Collections.synchronizedSet(new HashSet<>());
        private final AtomicBoolean writing = new AtomicBoolean(false);
        private volatile boolean concurrentWriteObserved;

        private GuardedObjectOutputStream(int expectedObjects) throws IOException {
            super(expectedObjects);
        }

        @Override
        protected void writeObjectOverride(Object obj) {
            if (!writing.compareAndSet(false, true)) {
                concurrentWriteObserved = true;
            }
            writerThreads.add(Thread.currentThread().getName());
            try {
                super.writeObjectOverride(obj);
            } finally {
                writing.set(false);
            }
        }
    }

    private static class ImmediateOrderingService implements OrderingService {
        @Override
        public boolean propose(ChatReqMessage request) {
            return true;
        }

        @Override
        public boolean establishDeliveryBoundary(String boundaryId) {
            return true;
        }

        @Override
        public void onDeliver(Consumer<ChatDeliverMessage> callback) {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public int getLeaderId() {
            return 1;
        }
    }

    private static final class RecordingBroker extends Broker {
        private final AtomicInteger activationCount = new AtomicInteger();
        private final AtomicInteger chatDeliveryCount = new AtomicInteger();

        private RecordingBroker() {
            super(TestConfigs.raftBrokerConfig(1, 5000));
        }

        @Override
        public boolean activateClient(ClientHandler handler) {
            activationCount.incrementAndGet();
            return true;
        }

        @Override
        public void onChatDeliver(long seq, String sender, String senderClientId, String text) {
            chatDeliveryCount.incrementAndGet();
        }
    }
}
