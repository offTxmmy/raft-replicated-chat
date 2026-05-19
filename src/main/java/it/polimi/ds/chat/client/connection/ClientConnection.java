package it.polimi.ds.chat.client.connection;

import java.io.*;
import java.net.Socket;

/**
 * Handles the TCP connection between the chat client and a broker.
 * Provides methods for opening, closing, and interacting with the connection.
 */
public class ClientConnection {

    protected String host;
    protected int port;

    private Socket socket;
    private PrintWriter out;
    private ObjectOutputStream objectOut;
    private ObjectInputStream objectIn;

    /**
     * Constructs a ClientConnection for the specified host and port.
     *
     * @param host the broker host to connect to
     * @param port the broker port to connect to
     */
    public ClientConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Opens the connection to the broker and initializes streams.
     *
     * @throws IOException if the connection or streams cannot be established
     */
    public void open() throws IOException {
        openSocket();
    }

    private void openSocket() throws IOException {
        this.socket = new Socket(host, port);
        this.objectOut = new ObjectOutputStream(socket.getOutputStream());
        this.objectIn = new ObjectInputStream(socket.getInputStream());
        this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    /**
     * Returns the PrintWriter for sending text lines to the broker.
     *
     * @return the PrintWriter for the connection
     */
    public PrintWriter getWriter() {
        return out;
    }

    /**
     * Returns the ObjectOutputStream for sending objects to the broker.
     *
     * @return the ObjectOutputStream for the connection
     */
    public ObjectOutputStream getObjectOutputStream() {
        return objectOut;
    }

    /**
     * Returns the ObjectInputStream for receiving objects from the broker.
     *
     * @return the ObjectInputStream for the connection
     */
    public ObjectInputStream getObjectInputStream() {
        return objectIn;
    }

    /**
     * Checks if the connection is currently open.
     *
     * @return true if the socket is open, false otherwise
     */
    public boolean isOpen() {
        return socket != null && !socket.isClosed();
    }

    /**
     * Closes the connection and underlying socket.
     */
    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * Returns the host this connection is associated with.
     *
     * @return the broker host
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the port this connection is associated with.
     *
     * @return the broker port
     */
    public int getPort() {
        return port;
    }

    /**
     * Closes the current connection and updates the host and port for reconnection.
     *
     * @param newHost the new broker host
     * @param newPort the new broker port
     * @throws IOException if an error occurs while closing the connection
     */
    protected void reopenTo(String newHost, int newPort) throws IOException {
        close();
        this.host = newHost;
        this.port = newPort;
    }

}
