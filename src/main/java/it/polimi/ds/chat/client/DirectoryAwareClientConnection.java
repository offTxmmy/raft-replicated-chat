package it.polimi.ds.chat.client;

import java.io.IOException;

public class DirectoryAwareClientConnection extends ClientConnection {

    public DirectoryAwareClientConnection(String directoryHost, int directoryPort) {
        super(directoryHost, directoryPort);
    }

    @Override
    public void open() throws IOException {
        //System.out.println("[DEBUG] DirectoryAwareClientConnection.open() - inizio");
        //System.out.println("[DEBUG] Directory host/port correnti: " + getHost() + ":" + getPort());

        ClientDirectory dirClient = new ClientDirectory(getHost(), getPort());

        ClientDirectory.BrokerInfo brokerInfo;
        try {
            //System.out.println("[DEBUG] Chiamo dirClient.getBestBroker()...");
            brokerInfo = dirClient.getBestBroker();
            //System.out.println("[DEBUG] dirClient.getBestBroker() ritornato");
        } catch (ClassNotFoundException e) {
            //System.out.println("[DEBUG] Eccezione ClassNotFound in getBestBroker: " + e.getMessage());
            throw new IOException("Errore nel protocollo con il DirectoryService", e);
        }

        if (brokerInfo == null) {
            //System.out.println("[DEBUG] Nessun broker disponibile (brokerInfo == null)");
            throw new IOException("Nessun broker disponibile al momento");
        }

        String brokerHost = brokerInfo.getHost();
        int brokerPort    = brokerInfo.getPort();

        //System.out.println("[DEBUG] Broker scelto dalla directory: " + brokerHost + ":" + brokerPort);

        //System.out.println("[DEBUG] Chiamo reopenTo(" + brokerHost + ", " + brokerPort + ")...");
        // Cambia host/port e CHIUDE eventuale socket precedente
        reopenTo(brokerHost, brokerPort);

        //System.out.println("[DEBUG] Ora chiamo super.open() per connettermi al broker...");
        // Qui chiamiamo l'open() della classe base che fa realmente new Socket(...)
        super.open();
        //System.out.println("[DEBUG] Connessione al broker aperta");

        //System.out.println("[DEBUG] DirectoryAwareClientConnection.open() - fine");
    }
}
