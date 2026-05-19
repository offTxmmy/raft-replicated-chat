package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BrokerCausalDeliveryTest {

    /**
     * Broker in-memory che registra i messaggi consegnati
     * invece di mandarli davvero ai client.
     */
    private static class RecordingBroker extends Broker {
        private final List<String> delivered =
                Collections.synchronizedList(new ArrayList<>());

        RecordingBroker(BrokerConfig config) {
            super(config);
        }

        @Override
        public void onChatDeliver(long seq, String sender, String text) {
            delivered.add(seq + ":" + sender + ":" + text);
        }

        List<String> getDelivered() {
            return delivered;
        }
    }

    private RecordingBroker broker;

    @BeforeEach
    public void setUp() {
        broker = new RecordingBroker(TestConfigs.raftBrokerConfig(1, 5000));
    }

    @Test
    public void outOfOrderMessagesFromSameSenderAreBufferedUntilCausalPredecessorsArrive() {
        // Costruiamo i 3 vector clock per brokerId = 1

        // m1: clock {1:1}
        VectorClock clock1 = new VectorClock();
        clock1.increment(1); // {1:1}

        // m2: clock {1:2}
        VectorClock clock2 = new VectorClock(clock1);
        clock2.increment(1); // {1:2}

        // m3: clock {1:3}
        VectorClock clock3 = new VectorClock(clock2);
        clock3.increment(1); // {1:3}

        // Messaggi con seq globale coerente con l’ordine causale
        ChatDeliverMessage m1 = new ChatDeliverMessage(
                1L,      // seq
                1,       // brokerId sorgente
                "alice", // sender
                "m1",    // text
                new VectorClock(clock1)
        );

        ChatDeliverMessage m2 = new ChatDeliverMessage(
                2L,
                1,
                "alice",
                "m2",
                new VectorClock(clock2)
        );

        ChatDeliverMessage m3 = new ChatDeliverMessage(
                3L,
                1,
                "alice",
                "m3",
                new VectorClock(clock3)
        );

        // 1) Mandiamo PRIMA m3 (più "avanti" nel tempo logico)
        broker.handleOrderedMessage(m3);

        // Non dovrebbe essere consegnato nulla,
        // perché vectorClock locale è {1:0} e messageClock[1] = 3 -> salta eventi
        assertTrue(broker.getDelivered().isEmpty(),
                "No message should be delivered when only m3 has arrived");
        assertEquals(0, broker.getVectorClock().getTimeStamp(1),
                "Local clock should still be 0 for broker 1 after buffering m3");

        // 2) Ora arriva m1 (il primo evento causale)
        broker.handleOrderedMessage(m1);

        // Adesso m1 è deliverable:
        // - vectorClock (ancora vuoto) happensBefore({1:1}) -> true
        // - messageClock[1] == local[1] + 1 -> 1 == 0 + 1 -> ok
        List<String> afterM1 = broker.getDelivered();
        assertEquals(1, afterM1.size(),
                "Exactly one message should have been delivered after m1");
        assertEquals("1:alice:m1", afterM1.get(0),
                "First delivered message should be m1");
        assertEquals(1, broker.getVectorClock().getTimeStamp(1),
                "Local clock for broker 1 should now be 1");

        // Nota: m3 è ancora in holdBackQueue e NON è ancora deliverable:
        // messageClock[1] = 3, local[1] = 1 -> 3 != 1 + 1

        // 3) Ora arriva m2 (il secondo evento causale)
        broker.handleOrderedMessage(m2);

        // Quando m2 arriva:
        // - m2 è deliverable (2 == 1+1) -> vectorClock diventa {1:2}
        // - holdBackQueue rilascia anche m3:
        //   messageClock[1] = 3 == 2+1 -> m3 diventa deliverable
        List<String> finalDelivered = broker.getDelivered();

        // Controlliamo che siano stati consegnati TUTTI i messaggi
        assertEquals(3, finalDelivered.size(),
                "All three messages should be delivered in the end");

        // E soprattutto che l'ordine di consegna sia quello causale m1, m2, m3
        List<String> expectedOrder = List.of(
                "1:alice:m1",
                "2:alice:m2",
                "3:alice:m3"
        );
        assertEquals(expectedOrder, finalDelivered,
                "Messages must be delivered in causal order, not arrival order");

        // Infine il vectorClock locale per broker 1 deve essere 3
        assertEquals(3, broker.getVectorClock().getTimeStamp(1),
                "Local clock for broker 1 should be 3 after all deliveries");
    }
}
