package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.messages.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SequencerState {
    private long globalSeq = 0;
    private final Broker broker;

    private final Map<Integer, Socket> brokerConnections = new HashMap<>();
    private final Map<Integer, ObjectOutputStream> brokerOutStreams = new HashMap<>();
    private final Map<Integer, ObjectInputStream> brokerInStreams = new HashMap<>();

    private final Map<Integer, Long> lastHeartbeats = new HashMap<>();

    public SequencerState(Broker broker) {
        this.broker = broker;
        startHeartbeatReaper();
    }

    public synchronized void registerBrokerConnection(int brokerId, Socket socket, ObjectInputStream brokerIn, ObjectOutputStream brokerOut) {
        brokerConnections.put(brokerId, socket);
        brokerOutStreams.put(brokerId, brokerOut);
        brokerInStreams.put(brokerId, brokerIn);

        Thread t = new Thread(() -> handleBrokerMessages(brokerId, brokerIn), "BrokerListener-" + brokerId);
        t.setDaemon(true);
        t.start();
    }

    // Process heartbeats from brokers
    public synchronized void handleHeartbeatFromBroker(HeartbeatMessage heartbeatMessage) {
        // Update the last heartbeat timestamp for the broker
        lastHeartbeats.put(heartbeatMessage.getBrokerId(), heartbeatMessage.getTimestamp());
    }

    // Periodic reaper to check for failed brokers based on heartbeats
    private void startHeartbeatReaper() {
        // Periodically check for missed heartbeats and identify failed brokers
        new Thread(() -> {
            while (true) {
                try {
                    TimeUnit.MILLISECONDS.sleep(5000);

                    long currentTime = System.currentTimeMillis();

                    // Check each broker's last heartbeat timestamp
                    for (Map.Entry<Integer, Long> entry : lastHeartbeats.entrySet()) {
                        int brokerId = entry.getKey();
                        long lastHeartbeat = entry.getValue();

                        if (currentTime - lastHeartbeat > 5000) {
                            System.out.println(("Broker " + brokerId + " considered FAILED (no heartbeat for " + (currentTime - lastHeartbeat) + " ms). Removing broker."));
                            removeBroker(brokerId);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }).start();
    }

    public synchronized void handleChatFromBroker(ChatReqMessage chatReq) {
        long seq = ++globalSeq;

        // Deliver locally to clients connected to the sequencer itself
        // later we'll implement Raft, so the leader will wait for ACKs before
        // delivering message to its clients
        broker.handleOrderedMessage(new ChatDeliverMessage(
                seq,
                chatReq.getBrokerId(),
                chatReq.getUsername(),
                chatReq.getText(),
                chatReq.getVectorClock()
        ));

        // Notify all brokers (including the sender) about the ordered message
        ChatDeliverMessage deliver = new ChatDeliverMessage(
                seq,
                chatReq.getBrokerId(),
                chatReq.getUsername(),
                chatReq.getText(),
                chatReq.getVectorClock()
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

    private synchronized void removeBroker(int brokerId) {
        // Remove broker's connection and heartbeat tracking
        Socket socket = brokerConnections.remove(brokerId);
        ObjectInputStream out = brokerInStreams.remove(brokerId);
        ObjectOutputStream in = brokerOutStreams.remove(brokerId);

        // Close socket and clean up streams
        closeQuietly(socket);
        closeQuietly(socket);
        closeQuietly(socket);

        // Remove broker from heartbeat tracking
        lastHeartbeats.remove(brokerId);

        System.out.println("Broker " + brokerId + " removed from the system due to failure.");
    }

    private void handleBrokerMessages(int brokerId, ObjectInputStream brokerIn) {
        try {
            while (true) {
                Object msg = brokerIn.readObject();
                if (msg instanceof HeartbeatMessage hb) {
                    handleHeartbeatFromBroker(hb);
                } else if (msg instanceof BrokerMessage brokerMessage) {
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