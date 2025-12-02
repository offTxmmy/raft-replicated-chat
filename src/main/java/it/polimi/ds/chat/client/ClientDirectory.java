package it.polimi.ds.chat.client;

import it.polimi.ds.chat.messages.GetBrokerRequestMessage;
import it.polimi.ds.chat.messages.GetBrokerResponseMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ClientDirectory {

    private final String directoryHost;
    private final int directoryPort;

    public ClientDirectory(String directoryHost, int directoryPort) {
        this.directoryHost = directoryHost;
        this.directoryPort = directoryPort;
    }

    public BrokerInfo getBestBroker() throws IOException, ClassNotFoundException {
        System.out.println("[DEBUG] DirectoryClient.getBestBroker() - inizio");
        System.out.println("[DEBUG] Mi collego alla directory " + directoryHost + ":" + directoryPort);

        try (Socket socket = new Socket(directoryHost, directoryPort)) {
            System.out.println("[DEBUG] Socket verso directory aperta");

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            System.out.println("[DEBUG] ObjectOutputStream verso directory creato");

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            System.out.println("[DEBUG] ObjectInputStream verso directory creato");

            GetBrokerRequestMessage req = new GetBrokerRequestMessage();
            System.out.println("[DEBUG] Invio GetBrokerRequestMessage alla directory...");
            out.writeObject(req);
            out.flush();
            System.out.println("[DEBUG] Richiesta inviata, attendo risposta...");

            Object obj = in.readObject();
            System.out.println("[DEBUG] Oggetto risposta ricevuto dalla directory: " + obj);

            if (!(obj instanceof GetBrokerResponseMessage resp)) {
                throw new IOException("Risposta directory non riconosciuta: " + obj);
            }

            if (!resp.isAvailable()) {
                System.out.println("[DEBUG] Directory ha risposto !success → nessun broker disponibile");
                return null;
            }

            String host = resp.getBrokerHost();
            int port = resp.getBrokerPort();
            int id = resp.getBrokerId();

            System.out.println("[DEBUG] Directory ha scelto broker id=" + id + " " + host + ":" + port);
            System.out.println("[DEBUG] DirectoryClient.getBestBroker() - fine (ritorno BrokerInfo)");

            return new BrokerInfo(host, port, id);
        }
    }

    public static class BrokerInfo {
        private final String host;
        private final int port;
        private final int brokerId;

        public BrokerInfo(String host, int port, int brokerId) {
            this.host = host;
            this.port = port;
            this.brokerId = brokerId;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public int getBrokerId() {
            return brokerId;
        }
    }
}
