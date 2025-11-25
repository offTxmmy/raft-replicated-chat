package it.polimi.ds.chat.client;

import it.polimi.ds.chat.broker.Broker;
import it.polimi.ds.chat.utilities.Protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final Broker broker;
    private PrintWriter out;
    private String username = "anonymous";

    public ClientHandler(Socket socket, Broker broker) {
        this.socket = socket;
        this.broker = broker;
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ) {
            out = new PrintWriter(socket.getOutputStream(), true);

                String line;
                while((line = in.readLine()) != null) {
                    handleCommand(line);
                }

        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        } finally {
            broker.removeClient(this);
            broker.notifyLeave(username);
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    private void handleCommand(String line) {
        if (Protocol.isJoin(line)) {
            username = Protocol.parseJoin(line);
            out.println(Protocol.welcome(username));
            broker.notifyJoin(username);
        } else if (Protocol.isMsg(line)) {
            String text = Protocol.parseMsg(line);
            broker.onClientMessage(username, text);
        } else if (Protocol.isQuit(line)) {
            try {
                socket.close();
            } catch (IOException ignored) {}
        } else {
            out.println("ERROR Unknown command " + line);
        }
    }

    public void sendMessageToClient(long seq, String sender, String text) {
        if (out != null) {
            out.println(Protocol.msgToClient(seq, sender, text));
        }
    }
}