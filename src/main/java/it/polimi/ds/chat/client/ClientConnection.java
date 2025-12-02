package it.polimi.ds.chat.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientConnection {

    protected String host;
    protected int port;

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    public ClientConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void open() throws IOException {
        this.socket = new Socket(host, port);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    public BufferedReader getReader() {
        return in;
    }

    public PrintWriter getWriter() {
        return out;
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
