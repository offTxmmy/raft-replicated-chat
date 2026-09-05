package it.polimi.ds.chat.client;

import it.polimi.ds.chat.client.connection.ClientConnection;
import it.polimi.ds.chat.client.connection.DirectoryAwareClientConnection;
import it.polimi.ds.chat.client.messaging.ClientMessageSender;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
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
        try (Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .dumb(true)
                .build()) {
            LineReader lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();
            lineReader.option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);
            ClientRuntime runtime = null;

            try {
                String username;
                try {
                    username = lineReader.readLine("Enter username: ");
                } catch (EndOfFileException | UserInterruptException e) {
                    return;
                }
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
                        lineReader::printAbove
                );
                runtime.start();
                lineReader.printAbove(
                        "[MAIN] Connecting to an available broker; "
                                + "messages remain queued until JOIN succeeds."
                );

                Thread senderThread = new Thread(sender, "MessageSender");
                senderThread.setDaemon(true);
                senderThread.start();

                while (true) {
                    String input;
                    try {
                        input = lineReader.readLine("> ");
                    } catch (EndOfFileException | UserInterruptException e) {
                        runtime.shutdown(true);
                        lineReader.printAbove("Goodbye!");
                        return;
                    }
                    if (input.equalsIgnoreCase("/quit")) {
                        runtime.shutdown(true);
                        lineReader.printAbove("Goodbye!");
                        return;
                    }
                    if (!input.isBlank()) {
                        sender.sendUserMessage(input);
                    }
                }
            } finally {
                if (runtime != null) {
                    runtime.shutdown(false);
                }
            }
        } catch (IOException e) {
            System.err.println("[MAIN] Connection error: " + e.getMessage());
        } finally {
            connection.close();
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
