package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.client.ClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BrokerHandler {
    private final BrokerConfig config;
    private final List<ClientHandler> clients = Collections.synchronizedList((new ArrayList<>()));

    public BrokerHandler(BrokerConfig config) {
        this.config = config;
    }

    public void start() throws IOException {
        int port = config.getClientPort();
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker started on port " + port);

        while(true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("New client connected from " + clientSocket.getRemoteSocketAddress());

            ClientHandler handler = new ClientHandler(clientSocket, this);
            clients.add(handler);

            Thread t = new Thread(handler);
            t.setDaemon(true);
            t.start();
        }
    }

    public void removeClient(ClientHandler handler) {
        clients.remove(handler);
    }

    public void broadcast(String sender, String message) {
        synchronized (clients) {
            for (ClientHandler handler : clients) {
                handler.sendMessageToClient(sender, message);
            }
        }
    }

    public void notifyLeave(String username) {
        broadcast("[system]", username + " left the chat");
    }

    public void notifyJoin(String username) {
        broadcast("[system]", username + " joined the chat");
    }
}
