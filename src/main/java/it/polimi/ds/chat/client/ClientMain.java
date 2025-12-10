package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientJoinMessage;
import it.polimi.ds.chat.messages.ClientQuitMessage;

import java.io.BufferedReader;
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

        //System.out.println("[DEBUG] Directory host: " + directoryHost + ", port: " + directoryPort);

        ClientConnection connection =
                new DirectoryAwareClientConnection(directoryHost, directoryPort);

        //System.out.println("[DEBUG] Creata DirectoryAwareClientConnection");

        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {

            //System.out.println("[DEBUG] Chiamo connection.open() (questa contatta il DirectoryService)");
            connection.open();
            //System.out.println("[DEBUG] connection.open() completata → ora dovrei essere connesso al BROKER");

            System.out.print("Enter username: ");
            //System.out.println("[DEBUG] In attesa username da console.");
            String username = stdin.readLine();
            //System.out.println("[DEBUG] Username inserito: " + username);

            //System.out.println("[DEBUG] Invio JOIN al broker.");
            connection.getWriter().println(ClientJoinMessage.joinCommand(username));
            //System.out.println("[DEBUG] JOIN inviato.");

            //System.out.println("[DEBUG] Creo sender.");
            ClientMessageSender sender =
                    new ClientMessageSender(connection.getWriter(), ACK_TIMEOUT_MS);

            //System.out.println("[DEBUG] Creo heartbeat manager.");
            ClientHeartbeatManager heartbeatManager =
                    new ClientHeartbeatManager(
                            connection.getWriter(),
                            () -> {
                                // Callback chiamata quando si superano i MAX_MISSED_HEARTBEATS
                                System.err.println("[HB] Broker non risponde a " +
                                        "troppi heartbeat consecutivi → sospetto crash.");
                                // TODO qui implementerò la logica di riconnessione
                            }
                    );

            //System.out.println("[DEBUG] Creo receiver.");
            ClientMessageReceiver receiver =
                    new ClientMessageReceiver(connection.getReader(), sender, username, heartbeatManager);

            Thread senderThread = new Thread(sender, "MessageSender");
            Thread receiverThread = new Thread(receiver, "MessageReceiver");
            Thread heartbeatThread = new Thread(heartbeatManager, "HeartbeatThread");

            senderThread.setDaemon(true);
            receiverThread.setDaemon(true);
            heartbeatThread.setDaemon(true);

            //System.out.println("[DEBUG] Avvio senderThread.");
            senderThread.start();

            //System.out.println("[DEBUG] Avvio receiverThread.");
            receiverThread.start();

            //System.out.println("[DEBUG] Avvio heartbeatThread.");
            heartbeatThread.start();

            //System.out.println("[DEBUG] Entrato nel main loop, ora attendo input dell’utente.");

            String input;
            while ((input = stdin.readLine()) != null) {
                //System.out.println("[DEBUG] Letta riga da console: \"" + input + "\"");

                if (input.equalsIgnoreCase("/quit")) {
                    //System.out.println("[DEBUG] Rilevato comando /quit → invio QUIT al server");
                    connection.getWriter().println(ClientQuitMessage.quitCommand());
                    break;
                } else if (!input.isBlank()) {
                    //System.out.println("[DEBUG] Invio messaggio utente al sender...");
                    sender.sendUserMessage(input);
                } else {
                    //System.out.println("[DEBUG] Riga vuota → ignorata.");
                }
            }

            //System.out.println("[DEBUG] Uscito dal main loop, inizio shutdown...");

            sender.shutdown();
            //System.out.println("[DEBUG] Sender shutdown OK.");

            receiver.shutdown();
            //System.out.println("[DEBUG] Receiver shutdown OK.");

            heartbeatManager.stop();
            //System.out.println("[DEBUG] HeartbeatManager shutdown OK.");

            connection.close();
            //System.out.println("[DEBUG] Connessione chiusa.");

            System.out.println("Goodbye!");
            //System.out.println("[DEBUG] Fine ClientMain");

        } catch (IOException e) {
            System.err.println("[DEBUG-ERROR] Connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
