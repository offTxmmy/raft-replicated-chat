package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.messages.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class SequencerState {
    private long globalSeq = 0;
    private final Broker broker;

    private final Map<Integer, Socket> brokerConnections = new HashMap<>();
    private final Map<Integer, ObjectOutputStream> brokerOutStreams = new HashMap<>();
    private final Map<Integer, ObjectInputStream> brokerInStreams = new HashMap<>();

    public SequencerState(Broker broker) {
        this.broker = broker;
    }

    public synchronized void registerBrokerConnection(int brokerId, Socket socket, ObjectInputStream brokerIn, ObjectOutputStream brokerOut) {
        brokerConnections.put(brokerId, socket);
        brokerOutStreams.put(brokerId, brokerOut);
        brokerInStreams.put(brokerId, brokerIn);

        Thread t = new Thread(() -> handleBrokerMessages(brokerId, brokerIn), "BrokerListener-" + brokerId);
        t.setDaemon(true);
        t.start();
    }

    public synchronized void handleChatFromBroker(ChatReqMessage chatReq) {
        long seq = ++globalSeq;

        // Deliver locally to clients connected to the sequencer itself
        broker.onChatDeliver(seq, chatReq.getUsername(), chatReq.getText());

        // Notify all brokers (including the sender) about the ordered message
        ChatDeliverMessage deliver = new ChatDeliverMessage(
                seq,
                chatReq.getBrokerId(),
                chatReq.getUsername(),
                chatReq.getText()
        );
        sendToAllBrokers(deliver);

        // Optionally confirm ordering to the sender broker
        sendMessageToBroker(chatReq.getBrokerId(), new ChatReqAck(chatReq.getLocalMsgId(), seq));
    }

    public void sendMessageToBroker(int brokerId, Object message) {
        ObjectOutputStream out;
        synchronized (brokerOutStreams) {
            out = brokerOutStreams.get(brokerId);
        }

        if (out == null) {
            System.err.println("No connection found for broker " + brokerId);
            return;
        }

        try {
            out.writeObject(message);
            out.flush();
        } catch (IOException e) {
            System.err.println("Failed to send message to broker " + brokerId + ": " + e.getMessage());
        }
    }

    public void sendToAllBrokers(Object message) {
        synchronized (brokerOutStreams) {
            for (Map.Entry<Integer, ObjectOutputStream> entry : brokerOutStreams.entrySet()) {
                try {
                    entry.getValue().writeObject(message);
                    entry.getValue().flush();
                } catch (IOException e) {
                    System.err.println("Failed to send message to broker " + entry.getKey() + ": " + e.getMessage());
                }
            }
        }
    }

    private void handleBrokerMessages(int brokerId, ObjectInputStream brokerIn) {
        try {
            while (true) {
                Object msg = brokerIn.readObject();
                if (msg instanceof BrokerMessage brokerMessage) {
                    processBrokerMessage(brokerId, brokerMessage);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Connection to broker " + brokerId + " closed: " + e.getMessage());
        } finally {
            cleanupBrokerConnection(brokerId);
        }
    }

    private void processBrokerMessage(int brokerId, BrokerMessage message) {
        if (message instanceof ChatReqMessage chatReq) {
            handleChatFromBroker(chatReq);
        } else {
            System.out.println("SequencerState: received unknown broker message from " + brokerId + ": " + message);
        }
    }

    private void cleanupBrokerConnection(int brokerId) {
        synchronized (brokerConnections) {
            closeQuietly(brokerConnections.remove(brokerId));
        }
        synchronized (brokerOutStreams) {
            brokerOutStreams.remove(brokerId);
        }
        synchronized (brokerInStreams) {
            brokerInStreams.remove(brokerId);
        }
    }

    private void closeQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }
}