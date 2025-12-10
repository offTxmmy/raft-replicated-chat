package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientJoinMessage;
import it.polimi.ds.chat.messages.ClientQuitMessage;

import java.io.IOException;
import java.io.InputStreamReader;

public class ClientMain {

    private static final long ACK_TIMEOUT_MS = 2000L;

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: CLIENT---");

        String directoryHost = "localhost";
        int directoryPort = 60001;

        if (args.length >= 1) {
            directoryHost = args[0];
        }
        if (args.length >= 2) {
            try {
                directoryPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException ignored) {}
        }

        ClientConnection connection = new DirectoryAwareClientConnection(directoryHost, directoryPort);

        try (var stdin = new java.util.Scanner(System.in)) {
            connection.open();
            System.out.print("Enter username: ");
            String username = stdin.nextLine();
            connection.getObjectOutputStream().writeObject(ClientJoinMessage.joinCommand(username));
            connection.getObjectOutputStream().flush();
            ClientMessageSender sender =
                    new ClientMessageSender(connection.getObjectOutputStream(), ACK_TIMEOUT_MS);
            ClientHeartbeatManager heartbeatManager =
                    new ClientHeartbeatManager(
                            connection.getObjectOutputStream(),
                            () -> {
                                System.err.println("[HB] Broker non risponde a " +
                                        "troppi heartbeat consecutivi → sospetto crash.");
                            }
                    );
            ClientMessageReceiver receiver =
                    new ClientMessageReceiver(connection.getObjectInputStream(), sender, username, heartbeatManager);
            Thread senderThread = new Thread(sender, "MessageSender");
            Thread receiverThread = new Thread(receiver, "MessageReceiver");
            Thread heartbeatThread = new Thread(heartbeatManager, "HeartbeatThread");
            senderThread.setDaemon(true);
            receiverThread.setDaemon(true);
            heartbeatThread.setDaemon(true);
            senderThread.start();
            receiverThread.start();
            heartbeatThread.start();
            String input;
            while (true) {
                input = stdin.nextLine();
                if (input.equalsIgnoreCase("/quit")) {
                    connection.getObjectOutputStream().writeObject(ClientQuitMessage.quitCommand());
                    connection.getObjectOutputStream().flush();
                    break;
                } else if (!input.isBlank()) {
                    sender.sendUserMessage(input);
                }
            }
            sender.shutdown();
            receiver.shutdown();
            heartbeatManager.stop();
            connection.close();
            System.out.println("Goodbye!");
        } catch (IOException e) {
            System.err.println("[DEBUG-ERROR] Connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
