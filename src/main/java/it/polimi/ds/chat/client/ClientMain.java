package it.polimi.ds.chat.client;

import it.polimi.ds.chat.utilities.Protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientMain {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 50000;

        try{
            new ClientMain().run(host, port);
        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }

    private void run(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        System.out.println("Connected to broker " + host + ": " + port);

        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); //getinputstream gestisce il decoding, arrivano byte e trasformo in caratteri
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // al contrario il encoding
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("Enter username: ");
        String username = stdin.readLine();
        out.println(Protocol.joinCommand(username));

        Thread readerThread = new Thread(() -> {
            try{
                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (IOException e) {
                System.err.println("Connection error: " + e.getMessage());
            }
        });
        readerThread.setDaemon(true);
        readerThread.start();

        System.out.println("Type messages and press ENTER to send.\nType /quit to exit");
        String input;
        while((input = stdin.readLine()) != null) {
            if(input.equalsIgnoreCase("/quit")) {
                out.println(Protocol.quitCommand());
                break;
            } else {
                out.println(Protocol.msgCommand(input));
            }
        }

        socket.close();
        System.out.println("Goodbye!");
    }
}
