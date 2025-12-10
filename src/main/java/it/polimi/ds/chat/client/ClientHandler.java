package it.polimi.ds.chat.client;

import it.polimi.ds.chat.broker.Broker;
import it.polimi.ds.chat.messages.ClientJoinMessage;
import it.polimi.ds.chat.messages.ClientQuitMessage;
import it.polimi.ds.chat.messages.ClientMessage;
import it.polimi.ds.chat.messages.HeartbeatMessage;
import it.polimi.ds.chat.messages.HeartbeatAckMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Handles a single client connection to the broker.
 * Responsible for reading commands from the client, parsing them,
 * and invoking the appropriate broker logic.
 */
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final Broker broker;
    private ObjectOutputStream out;
    private final Object outLock = new Object();
    private String username = "anonymous";

    /**
     * Constructs a ClientHandler for a given client socket and broker.
     *
     * @param socket the client socket
     * @param broker the broker instance
     */
    public ClientHandler(Socket socket, Broker broker) {
        this.socket = socket;
        this.broker = broker;
    }

    /**
     * Returns the username associated with this client connection.
     *
     * @return the client's username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sends an object (message or response) to the client.
     *
     * @param obj the object to send
     */
    public void sendLine(Object obj) {
        if (out != null) {
            try {
                synchronized (outLock) {
                    out.writeObject(obj);
                    out.flush();
                }
            } catch (IOException e) {
                System.err.println("Failed to send object to client " + username + ": " + e.getMessage());
            }
        }
    }

    /**
     * Main loop for handling client communication.
     * Reads objects from the client, processes commands, and handles disconnects.
     */
    @Override
    public void run() {
        try (
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            out = new ObjectOutputStream(socket.getOutputStream());
            Object obj;
            while ((obj = in.readObject()) != null) {
                handleCommand(obj);
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        } finally {
            broker.removeClient(this);
            broker.notifyLeave(username);
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * Handles a single command or message object received from the client.
     *
     * @param obj the received object (String or HeartbeatMessage)
     */
    private void handleCommand(Object obj) {
        if (obj instanceof String line) {
            Object msg = parseLineToMessage(line, username);
            if (msg instanceof ClientJoinMessage) {
                username = ClientJoinMessage.parseJoin(line);
                sendLine(ClientJoinMessage.welcome(username));
                broker.notifyJoin(username);
            } else if (msg instanceof ClientQuitMessage) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            } else if (msg instanceof ClientMessage) {
                broker.onClientMessage((ClientMessage) msg);
            } else if (msg instanceof HeartbeatMessage) {
                HeartbeatMessage hb = (HeartbeatMessage) msg;
                HeartbeatAckMessage ack = new HeartbeatAckMessage(hb.getTimestamp(), broker.getBrokerId());
                sendLine(ack);
            } else {
                sendLine("ERROR Unknown command " + line);
            }
        } else if (obj instanceof HeartbeatMessage hb) {
            HeartbeatAckMessage ack = new HeartbeatAckMessage(hb.getTimestamp(), broker.getBrokerId());
            sendLine(ack);
        } else {
            sendLine("ERROR Unknown object command " + obj);
        }
    }

    /**
     * Parses a line of text from the client into a message object.
     *
     * @param line the input line from the client
     * @param currentUsername the username currently associated with this connection
     * @return the parsed message object, or null if unrecognized
     */
    private Object parseLineToMessage(String line, String currentUsername) {
        if (line == null) return null;
        if (ClientJoinMessage.isJoin(line)) {
            String parsed = ClientJoinMessage.parseJoin(line);
            return new ClientJoinMessage(parsed, "", System.currentTimeMillis());
        }
        if (ClientQuitMessage.isQuit(line)) {
            return new ClientQuitMessage(currentUsername, "", System.currentTimeMillis());
        }
        if (ClientMessage.isMsg(line)) {
            return ClientMessage.fromClientLine(currentUsername, line);
        }
        if (HeartbeatMessage.isHeartbeatLine(line)) {
            long ts = HeartbeatMessage.parseTimestampFromWire(line);
            return new HeartbeatMessage(ts);
        }

        return null;
    }

    /**
     * Sends a chat message to the client with the given sequence number, sender, and text.
     *
     * @param seq the global sequence number of the message
     * @param sender the sender's username
     * @param text the message text
     */
    public void sendMessageToClient(long seq, String sender, String text) {
        if (out != null) {
            sendLine(ClientMessage.msgToClient(seq, sender, text));
        }
    }
}