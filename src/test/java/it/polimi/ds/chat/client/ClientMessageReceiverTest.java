package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.NotLeaderResponseMessage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ClientMessageReceiverTest {

    @Test
    void notLeaderWithEndpointShouldTriggerRedirectHandler() throws Exception {
        NotLeaderResponseMessage response =
                new NotLeaderResponseMessage(123L, 2, "127.0.0.1", 50002);

        AtomicReference<NotLeaderResponseMessage> redirected = new AtomicReference<>();

        ClientMessageReceiver receiver = new ClientMessageReceiver(
                objectInputContaining(response),
                dummySender(),
                "alice",
                null,
                redirected::set
        );

        receiver.run();

        NotLeaderResponseMessage actual = redirected.get();
        assertNotNull(actual);
        assertEquals(response.getTimestamp(), actual.getTimestamp());
        assertEquals(response.getLeaderId(), actual.getLeaderId());
        assertEquals(response.getLeaderHost(), actual.getLeaderHost());
        assertEquals(response.getLeaderPort(), actual.getLeaderPort());
    }

    @Test
    void notLeaderWithoutEndpointShouldNotTriggerRedirectHandler() throws Exception {
        NotLeaderResponseMessage response =
                new NotLeaderResponseMessage(123L, -1, null, -1);

        AtomicReference<NotLeaderResponseMessage> redirected = new AtomicReference<>();

        ClientMessageReceiver receiver = new ClientMessageReceiver(
                objectInputContaining(response),
                dummySender(),
                "alice",
                null,
                redirected::set
        );

        receiver.run();

        assertNull(redirected.get());
    }

    private static ObjectInputStream objectInputContaining(Object obj) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(obj);
            out.flush();
        }
        return new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private static ClientMessageSender dummySender() throws Exception {
        return new ClientMessageSender(
                new ObjectOutputStream(new ByteArrayOutputStream()),
                2_000L
        );
    }
}