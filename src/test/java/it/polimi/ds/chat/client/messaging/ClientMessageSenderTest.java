package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.client.connection.ClientObjectWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ClientMessageSenderTest {

    @Test
    void onlyHeadIsSentRetriedAndAcknowledgedInFifoOrder() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        CapturingWriter capture = new CapturingWriter();
        ClientMessageSender sender = new ClientMessageSender(
                capture.writer,
                500L,
                "client-a",
                clock::get
        );

        sender.sendUserMessage("first");
        sender.sendUserMessage("second");

        assertEquals(List.of("MSG client-a 1 first"), capture.objects());
        assertEquals(2, sender.pendingCount());
        assertEquals(1L, sender.pendingHeadSequence());

        clock.addAndGet(499L);
        sender.retryExpiredHead();
        assertEquals(List.of("MSG client-a 1 first"), capture.objects());

        clock.incrementAndGet();
        sender.retryExpiredHead();
        assertEquals(
                List.of("MSG client-a 1 first", "MSG client-a 1 first"),
                capture.objects()
        );

        // An ACK for a non-head sequence cannot skip the first message.
        sender.handleAck("client-a", 2L);
        assertEquals(2, sender.pendingCount());
        assertEquals(1L, sender.pendingHeadSequence());

        sender.handleAck("client-a", 1L);
        assertEquals(
                List.of(
                        "MSG client-a 1 first",
                        "MSG client-a 1 first",
                        "MSG client-a 2 second"
                ),
                capture.objects()
        );
        assertEquals(1, sender.pendingCount());
        assertEquals(2L, sender.pendingHeadSequence());

        // A duplicate old ACK cannot remove the new head.
        sender.handleAck("client-a", 1L);
        assertEquals(1, sender.pendingCount());

        sender.handleAck("client-a", 2L);
        assertEquals(0, sender.pendingCount());
        assertNull(sender.pendingHeadSequence());
    }

    @Test
    void reconnectResendsOnlyHeadBeforeAllowingNextMessage() throws Exception {
        AtomicLong clock = new AtomicLong(10L);
        CapturingWriter firstGeneration = new CapturingWriter();
        CapturingWriter secondGeneration = new CapturingWriter();
        ClientMessageSender sender = new ClientMessageSender(
                firstGeneration.writer,
                1_000L,
                "client-r",
                clock::get
        );

        sender.sendUserMessage("one");
        sender.sendUserMessage("two");
        sender.attachWriter(secondGeneration.writer);

        assertEquals(
                List.of("MSG client-r 1 one"),
                firstGeneration.objects()
        );
        assertEquals(
                List.of("MSG client-r 1 one"),
                secondGeneration.objects()
        );

        sender.handleAck("client-r", 1L);
        assertEquals(
                List.of("MSG client-r 1 one", "MSG client-r 2 two"),
                secondGeneration.objects()
        );
    }

    @Test
    void disconnectedMessagesRemainQueuedUntilWriterIsAttached() throws Exception {
        AtomicLong clock = new AtomicLong(20L);
        ClientMessageSender sender = new ClientMessageSender(
                null,
                1_000L,
                "client-q",
                clock::get
        );
        CapturingWriter generation = new CapturingWriter();

        sender.sendUserMessage("one");
        sender.sendUserMessage("two");
        assertEquals(2, sender.pendingCount());

        sender.attachWriter(generation.writer);
        assertEquals(List.of("MSG client-q 1 one"), generation.objects());

        sender.handleAck("client-q", 1L);
        assertEquals(
                List.of("MSG client-q 1 one", "MSG client-q 2 two"),
                generation.objects()
        );
    }

    private static final class CapturingWriter {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final ClientObjectWriter writer;

        private CapturingWriter() throws Exception {
            writer = new ClientObjectWriter(new ObjectOutputStream(bytes));
        }

        private List<Object> objects() throws Exception {
            List<Object> result = new ArrayList<>();
            try (ObjectInputStream input = new ObjectInputStream(
                    new ByteArrayInputStream(bytes.toByteArray())
            )) {
                while (true) {
                    result.add(input.readObject());
                }
            } catch (EOFException expected) {
                return result;
            }
        }
    }
}
