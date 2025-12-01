package it.polimi.ds.chat.externalservices;

import it.polimi.ds.chat.broker.BrokerConfig;
import it.polimi.ds.chat.broker.HandlerState;
import it.polimi.ds.chat.messages.DirectoryRegisterMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class DirectoryService {
    private HashMap<BrokerConfig, Integer> registeredBrokers = new HashMap<>();

    public static void main(String[] args) {
        int port = 60000;

        System.out.println("Directory Service starting on port " + port + "...");
        DirectoryService service = new DirectoryService();
        service.start(port);
    }

    public void start(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Directory Service listening on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New connection from " + socket.getRemoteSocketAddress());

                new Thread(() -> handleConnection(socket)).start();
            }
        } catch (IOException e) {
            System.err.println("Directory Service error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleConnection(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            Object obj = in.readObject();
            if (obj instanceof DirectoryRegisterMessage msg) {
                registerBroker(msg);
            } else {
                System.out.println("Unknown object from " + socket.getRemoteSocketAddress() + ": " + obj);
            }

        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error handling connection: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void registerBroker(DirectoryRegisterMessage msg) {
        String host = msg.getBrokerHost();
        int port = msg.getBrokerPort();
        boolean isSequencer = msg.isSequencer();

        int brokerId = isSequencer ? 0 : -1;
        int clientPort = port;
        String sequencerHost = host;
        int sequencerPort = 0;
        int udpPort = 0;
        HandlerState handlerState = null;

        BrokerConfig config = new BrokerConfig(
                brokerId,
                isSequencer,
                host,
                port,
                clientPort,
                sequencerHost,
                sequencerPort,
                udpPort,
                handlerState
        );

        registeredBrokers.putIfAbsent(config, 0);
        System.out.println("Registered broker: " + config.getBrokerHost() + ":" + config.getBrokerPort() + " (sequencer=" + isSequencer + "), clientCount=0");
    }
}