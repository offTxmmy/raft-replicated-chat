package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.ClientJoinMessage;
import it.polimi.ds.chat.messages.ClientQuitMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point for the chat client application.
 * <p>
 * Gestisce:
 * - connessione iniziale al broker tramite DirectoryService
 * - riconnessione automatica a un nuovo broker in caso di crash (tramite heartbeat)
 * - ritrasmissione dei messaggi pendenti verso il nuovo broker
 */
public class ClientMain {

    private static final long ACK_TIMEOUT_MS = 2000L;

    /**
     * Contiene i riferimenti alle istanze correnti di receiver e heartbeatManager,
     * così da poterle aggiornare su riconnessione e spegnere correttamente al /quit.
     */
    private static class ClientRuntimeContext {
        volatile ClientMessageReceiver receiver;
        volatile ClientHeartbeatManager heartbeatManager;
        final AtomicBoolean reconnecting = new AtomicBoolean(false);
    }

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
            } catch (NumberFormatException ignored) {
            }
        }

        ClientConnection connection = new DirectoryAwareClientConnection(directoryHost, directoryPort);
        ClientRuntimeContext ctx = new ClientRuntimeContext();

        try (var stdin = new java.util.Scanner(System.in)) {

            System.out.println("[MAIN] Apro connessione iniziale tramite DirectoryService...");
            connection.open();
            System.out.println("[MAIN] Connessione iniziale al broker " + connection.getHost() + ":" + connection.getPort() + " stabilita.");

            System.out.print("Enter username: ");
            String username = stdin.nextLine();

            ObjectOutputStream out = connection.getObjectOutputStream();
            System.out.println("[MAIN] Invio JOIN per utente '" + username + "' al broker iniziale...");
            out.writeObject(ClientJoinMessage.joinCommand(username));
            out.flush();
            System.out.println("[MAIN] JOIN inviato.");

            ClientMessageSender sender = new ClientMessageSender(connection.getObjectOutputStream(), ACK_TIMEOUT_MS);

            startReceiverAndHeartbeat(ctx, connection, sender, username);

            Thread senderThread = new Thread(sender, "MessageSender");
            senderThread.setDaemon(true);
            senderThread.start();
            System.out.println("[MAIN] Thread MessageSender avviato.");

            String input;
            while (true) {
                input = stdin.nextLine();
                if (input.equalsIgnoreCase("/quit")) {
                    System.out.println("[MAIN] Ricevuto comando /quit dall'utente.");
                    // inviamo QUIT al broker corrente
                    connection.getObjectOutputStream().writeObject(ClientQuitMessage.quitCommand());
                    connection.getObjectOutputStream().flush();
                    break;
                } else if (!input.isBlank()) {
                    sender.sendUserMessage(input);
                }
            }

            System.out.println("[MAIN] Shutdown in corso...");
            sender.shutdown();
            if (ctx.receiver != null) {
                ctx.receiver.shutdown();
            }
            if (ctx.heartbeatManager != null) {
                ctx.heartbeatManager.stop();
            }
            connection.close();
            System.out.println("Goodbye!");

        } catch (IOException e) {
            System.err.println("[MAIN-ERROR] Connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Crea e avvia:
     * - un ClientHeartbeatManager che invia heartbeat al broker corrente e,
     *   in caso di troppi heartbeat mancati, tenta la riconnessione tramite DirectoryService;
     * - un ClientMessageReceiver che legge dal broker e gestisce ACK + heartbeat ACK.
     *
     * In caso di crash del broker:
     *  - chiude la vecchia connessione,
     *  - chiama nuovamente connection.open() (nuovo broker dalla Directory),
     *  - reinvia il JOIN sul nuovo broker,
     *  - aggiorna il ClientMessageSender con il nuovo ObjectOutputStream,
     *  - crea nuovi heartbeatManager + receiver e li registra nel ctx.
     */
    private static void startReceiverAndHeartbeat(ClientRuntimeContext ctx,
                                                  ClientConnection connection,
                                                  ClientMessageSender sender,
                                                  String username) {
        System.out.println("[MAIN] startReceiverAndHeartbeat() - broker corrente: " +
                connection.getHost() + ":" + connection.getPort());

        ObjectOutputStream out = connection.getObjectOutputStream();
        ObjectInputStream in = connection.getObjectInputStream();

        ClientHeartbeatManager.HeartbeatFailureHandler failureHandler = () -> {
            if (!ctx.reconnecting.compareAndSet(false, true)) {
                System.err.println("[HB] Reconnection already in progress; ignoring duplicate failure.");
                return;
            }
            System.err.println("[HB] Broker non risponde a troppi heartbeat consecutivi → sospetto crash.");
            try {
                System.err.println("[HB] Chiudo connessione verso broker " +
                        connection.getHost() + ":" + connection.getPort());
                connection.close();

                System.err.println("[HB] Tentativo di riconnessione tramite DirectoryService...");
                connection.open();

                System.err.println("[HB] Nuova connessione stabilita verso broker " +
                        connection.getHost() + ":" + connection.getPort());

                ObjectOutputStream newOut = connection.getObjectOutputStream();

                System.err.println("[HB] Invio nuovamente JOIN per utente '" + username + "' al nuovo broker...");
                newOut.writeObject(ClientJoinMessage.joinCommand(username));
                newOut.flush();
                System.err.println("[HB] JOIN verso nuovo broker inviato.");

                System.err.println("[HB] Aggiorno sender con nuovo ObjectOutputStream...");
                sender.updateOutputStream(newOut);

                System.err.println("[HB] Avvio nuovo receiver + heartbeat per il nuovo broker...");
                startReceiverAndHeartbeat(ctx, connection, sender, username);
                ctx.reconnecting.set(false);

            } catch (IOException e) {
                System.err.println("[HB-ERROR] ERRORE durante la riconnessione: " + e.getMessage());
                e.printStackTrace();
                ctx.reconnecting.set(false);
            }
        };

        ClientHeartbeatManager heartbeatManager =
                new ClientHeartbeatManager(out, failureHandler);

        ClientMessageReceiver receiver =
                new ClientMessageReceiver(in, sender, username, heartbeatManager);

        ctx.receiver = receiver;
        ctx.heartbeatManager = heartbeatManager;

        // Thread receiver + heartbeat
        Thread receiverThread = new Thread(receiver, "MessageReceiver");
        Thread heartbeatThread = new Thread(heartbeatManager, "HeartbeatThread");

        receiverThread.setDaemon(true);
        heartbeatThread.setDaemon(true);

        //System.out.println("[MAIN] Avvio thread MessageReceiver e HeartbeatThread...");
        receiverThread.start();
        heartbeatThread.start();

    }
}
