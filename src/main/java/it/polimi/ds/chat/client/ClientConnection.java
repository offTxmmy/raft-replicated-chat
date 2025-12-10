package it.polimi.ds.chat.client;

import java.io.*;
import java.net.Socket;

public class ClientConnection {

    protected String host;
    protected int port;

    private Socket socket;
    private PrintWriter out;
    private ObjectOutputStream objectOut;
    private ObjectInputStream objectIn;

    public ClientConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void open() throws IOException {
        this.socket = new Socket(host, port);
        this.objectOut = new ObjectOutputStream(socket.getOutputStream());
        this.objectIn = new ObjectInputStream(socket.getInputStream());
        this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    public PrintWriter getWriter() {
        return out;
    }

    public ObjectOutputStream getObjectOutputStream() {
        return objectOut;
    }

    public ObjectInputStream getObjectInputStream() {
        return objectIn;
    }

    public boolean isOpen() {
        return socket != null && !socket.isClosed();
    }


    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    protected void reopenTo(String newHost, int newPort) throws IOException {
        close();
        this.host = newHost;
        this.port = newPort;
    }
}
