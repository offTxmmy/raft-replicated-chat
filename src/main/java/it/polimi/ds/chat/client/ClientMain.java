package it.polimi.ds.chat.client;

import it.polimi.ds.chat.client.connection.ClientConnection;
import it.polimi.ds.chat.client.connection.DirectoryAwareClientConnection;
import it.polimi.ds.chat.client.messaging.ClientMessageSender;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;

/** Entry point for the directory-aware replicated chat client. */
public class ClientMain {

    private static final long ACK_TIMEOUT_MS = 2_000L;

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTRUCTURE: CLIENT---");

        String directoryHost = args.length >= 1 ? args[0] : "localhost";
        int directoryPort = parsePort(args, 1, 60_001);

        ClientConnection connection = new DirectoryAwareClientConnection(
                directoryHost,
                directoryPort
        );
        ClientRuntime runtime = null;

        try (Scanner stdin = new Scanner(System.in)) {
            System.out.print("Enter username: ");
            if (!stdin.hasNextLine()) {
                return;
            }
            String username = stdin.nextLine();
            String clientId = UUID.randomUUID().toString();

            ClientMessageSender sender = new ClientMessageSender(
                    ACK_TIMEOUT_MS,
                    clientId
            );
            runtime = new ClientRuntime(
                    connection,
                    sender,
                    username,
                    clientId,
                    250L,
                    2_000L,
                    Thread::sleep,
                    (status, detail) -> System.out.println(
                        "[CLIENT] status=" + status + " (" + detail + ")")
            );
            runtime.start();
            System.out.println("[MAIN] Connecting to an available broker; "
                    + "messages remain queued until JOIN succeeds.");

            Thread senderThread = new Thread(sender, "MessageSender");
            senderThread.setDaemon(true);
            senderThread.start();

            while (stdin.hasNextLine()) {
                String input = stdin.nextLine();
                if (input.equalsIgnoreCase("/quit")) {
                    runtime.shutdown(true);
                    System.out.println("Goodbye!");
                    return;
                }
                if (!input.isBlank()) {
                    sender.sendUserMessage(input);
                }
            }
        } catch (IOException e) {
            System.err.println("[MAIN] Connection error: " + e.getMessage());
        } finally {
            if (runtime != null) {
                runtime.shutdown(false);
            } else {
                connection.close();
            }
        }
    }

    private static int parsePort(String[] args, int index, int defaultPort) {
        if (args.length <= index) {
            return defaultPort;
        }
        try {
            return Integer.parseInt(args[index]);
        } catch (NumberFormatException ignored) {
            return defaultPort;
        }
    }
}
