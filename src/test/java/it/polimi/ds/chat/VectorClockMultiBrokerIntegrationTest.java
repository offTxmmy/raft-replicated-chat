package it.polimi.ds.chat;

import it.polimi.ds.chat.broker.core.Broker;
import it.polimi.ds.chat.broker.config.BrokerConfig;
import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.common.clock.VectorClock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorClockMultiBrokerIntegrationTest {

    /**
     * Broker in-memory che registra i messaggi consegnati in una lista
     * per poterli ispezionare nel test.
     */
    private static class TracingBroker extends Broker {
        private final List<String> delivered = Collections.synchronizedList(new ArrayList<>());

        TracingBroker(BrokerConfig config) {
            super(config);
        }

        @Override
        public void onChatDeliver(long seq, String sender, String senderClientId, String text) {
            delivered.add(seq + ":" + sender + ":" + text);
        }

        List<String> getDelivered() {
            return delivered;
        }
    }

    private TracingBroker broker0;
    private TracingBroker broker1;
    private TracingBroker broker2;

    @Before
    public void setUp() {
        broker0 = new TracingBroker(TestConfigs.raftBrokerConfig(0, 5000));
        broker1   = new TracingBroker(TestConfigs.raftBrokerConfig(1, 5002));
        broker2   = new TracingBroker(TestConfigs.raftBrokerConfig(2, 5003));
    }

    @Test
    public void complexCausalAndConcurrentScenarioIsHandledConsistently() {
        /*
         * Scenario logico sui vector clock:
         *
         * Processi / broker:
         *  - P0 = broker0 (id 0) – qui non genera eventi suoi, solo riceve
         *  - P1 = broker1      (id 1)
         *  - P2 = broker2      (id 2)
         *
         * Eventi (con i loro vector clock):
         *
         *  E1: m1 da P1
         *      clock m1 = {1:1}
         *
         *  E2: m2 da P2, indipendente da m1 (concorrente)
         *      clock m2 = {2:1}
         *
         *  P1 riceve (o comunque "vede") m2 e poi genera:
         *
         *  E3: m3 da P1 dopo aver visto m2
         *      partendo da {1:1} (stato locale di P1 dopo m1)
         *      aggiorno con {2:1} → {1:1, 2:1}
         *      incremento P1      → {1:2, 2:1}
         *
         *  P2 riceve m1 e m3, poi genera:
         *
         *  E4: m4 da P2 dopo aver visto m1 e m3
         *      partendo da {2:1} (stato locale di P2 dopo m2)
         *      aggiorno con {1:2, 2:1} → {1:2, 2:1}
         *      incremento P2           → {1:2, 2:2}
         *
         * Sequenza globale decisa da Raft (seq):
         *   seq=1 → m1 (E1)
         *   seq=2 → m2 (E2)
         *   seq=3 → m3 (E3)
         *   seq=4 → m4 (E4)
         */

        // E1: m1 da broker1 con clock {1:1}
        VectorClock clockM1 = new VectorClock();
        clockM1.increment(1); // {1:1}
        ChatDeliverMessage m1 = new ChatDeliverMessage(
                1L,      // global sequence
                1,       // brokerId sorgente
                "alice", // sender
                "m1",    // text
                new VectorClock(clockM1)
        );

        // E2: m2 da broker2 con clock {2:1} (concorrente a m1)
        VectorClock clockM2 = new VectorClock();
        clockM2.increment(2); // {2:1}
        ChatDeliverMessage m2 = new ChatDeliverMessage(
                2L,
                2,
                "bob",
                "m2",
                new VectorClock(clockM2)
        );

        // E3: m3 da broker1 dopo aver visto m2
        // stato di P1 dopo m1: {1:1}
        // update con {2:1} → {1:1,2:1}
        // increment(1)      → {1:2,2:1}
        VectorClock clockM3 = new VectorClock(clockM1);
        clockM3.update(clockM2); // merge {1:1} e {2:1} → {1:1,2:1}
        clockM3.increment(1);    // → {1:2,2:1}
        ChatDeliverMessage m3 = new ChatDeliverMessage(
                3L,
                1,
                "alice",
                "m3",
                new VectorClock(clockM3)
        );

        // E4: m4 da broker2 dopo aver visto m1 e m3
        // stato di P2 dopo m2: {2:1}
        // update con {1:2,2:1} → {1:2,2:1}
        // increment(2)         → {1:2,2:2}
        VectorClock clockM4 = new VectorClock(clockM2);
        clockM4.update(clockM3); // → {1:2,2:1}
        clockM4.increment(2);    // → {1:2,2:2}
        ChatDeliverMessage m4 = new ChatDeliverMessage(
                4L,
                2,
                "bob",
                "m4",
                new VectorClock(clockM4)
        );

        // Applichiamo l'ordine globale [m1, m2, m3, m4]
        List<ChatDeliverMessage> ordered = List.of(m1, m2, m3, m4);

        for (ChatDeliverMessage msg : ordered) {
            broker0.handleOrderedMessage(msg);
            broker1.handleOrderedMessage(msg);
            broker2.handleOrderedMessage(msg);
        }

        // --- VERIFICA 1: ordine di consegna identico su tutti i broker ---
        List<String> expectedDelivery = List.of(
                "1:alice:m1",
                "2:bob:m2",
                "3:alice:m3",
                "4:bob:m4"
        );

        assertEquals("Broker0 should deliver all messages in global order",
                expectedDelivery, broker0.getDelivered());
        assertEquals("Broker1 should deliver all messages in global order",
                expectedDelivery, broker1.getDelivered());
        assertEquals("Broker2 should deliver all messages in global order",
                expectedDelivery, broker2.getDelivered());

        // --- VERIFICA 2: tutti i broker hanno visto almeno ID 1 e 2 ---
        assertTrue("Broker0 clock should know at least brokers 1 and 2",
                broker0.getVectorClock().getClock().size() >= 2);
        assertTrue("Broker1 clock should know at least brokers 1 and 2",
                broker1.getVectorClock().getClock().size() >= 2);
        assertTrue("Broker2 clock should know at least brokers 1 and 2",
                broker2.getVectorClock().getClock().size() >= 2);

        // --- VERIFICA 3: i timestamp per broker 1 e 2 sono coerenti tra i broker ---

        int seqTime1 = broker0.getVectorClock().getTimeStamp(1);
        int b1Time1  = broker1.getVectorClock().getTimeStamp(1);
        int b2Time1  = broker2.getVectorClock().getTimeStamp(1);

        int seqTime2 = broker0.getVectorClock().getTimeStamp(2);
        int b1Time2  = broker1.getVectorClock().getTimeStamp(2);
        int b2Time2  = broker2.getVectorClock().getTimeStamp(2);

        // Tutti devono concordare sul numero di eventi visti per broker 1
        assertEquals("All brokers should agree on broker 1 timestamp", seqTime1, b1Time1);
        assertEquals("All brokers should agree on broker 1 timestamp", seqTime1, b2Time1);

        // ...e per broker 2
        assertEquals("All brokers should agree on broker 2 timestamp", seqTime2, b1Time2);
        assertEquals("All brokers should agree on broker 2 timestamp", seqTime2, b2Time2);

        // --- VERIFICA 4: i timestamp sono almeno quelli che ci aspettiamo dalla storia (≥ 2) ---
        assertTrue("Broker 1 timestamp should be >= 2 (due eventi m1,m3)",
                seqTime1 >= 2 && b1Time1 >= 2 && b2Time1 >= 2);

        assertTrue("Broker 2 timestamp should be >= 2 (due eventi m2,m4)",
                seqTime2 >= 2 && b1Time2 >= 2 && b2Time2 >= 2);
    }
}
