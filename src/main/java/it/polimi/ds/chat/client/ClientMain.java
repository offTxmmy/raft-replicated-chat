package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientJoinMessage;
import it.polimi.ds.chat.messages.ClientQuitMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Entry point del client.
 *
 * Responsabilità:
 *  - gestire input da tastiera
 *  - inizializzare connessione, sender e receiver
 *  - inviare JOIN e QUIT
 */
public class ClientMain {

    // Timeout di attesa ACK in millisecondi
    private static final long ACK_TIMEOUT_MS = 2000L;

    public static void main(String[] args) {
        String host = "localhost";
        int port = 50000;

        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException ignored) {
            }
        }

        ClientConnection connection = new ClientConnection(host, port);

        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {

            connection.open();
            System.out.println("Connected to broker " + host + ":" + port);

            System.out.print("Enter username: ");
            String username = stdin.readLine();

            // Invia JOIN usando la tua classe esistente
            connection.getWriter().println(ClientJoinMessage.joinCommand(username));

            // Crea sender e receiver
            ClientMessageSender sender = new ClientMessageSender(connection.getWriter(), ACK_TIMEOUT_MS);
            ClientMessageReceiver receiver = new ClientMessageReceiver(connection.getReader(), sender, username);

            Thread senderThread = new Thread(sender, "MessageSender");
            Thread receiverThread = new Thread(receiver, "MessageReceiver");

            senderThread.setDaemon(true);
            receiverThread.setDaemon(true);

            senderThread.start();
            receiverThread.start();

            System.out.println("Type messages and press ENTER to send.");
            System.out.println("Type /quit to exit.");

            String input;
            while ((input = stdin.readLine()) != null) {
                if (input.equalsIgnoreCase("/quit")) {
                    // Notifica al server che il client si disconnette
                    connection.getWriter().println(ClientQuitMessage.quitCommand());
                    break;
                } else if (!input.isBlank()) {
                    // Invia messaggio utente (con timestamp & gestione ACK)
                    sender.sendUserMessage(input);
                }
            }

            // Shutdown ordinato
            sender.shutdown();
            receiver.shutdown();
            connection.close();
            System.out.println("Goodbye!");

        } catch (IOException e) {
            System.err.println("Connection error: " + e.getMessage());
        }
    }
}
