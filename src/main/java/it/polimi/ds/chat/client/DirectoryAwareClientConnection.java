package it.polimi.ds.chat.client;

import java.io.IOException;

/**
 * A client connection that first queries the Directory Service to discover the best broker,
 * then connects to that broker for chat communication.
 */
public class DirectoryAwareClientConnection extends ClientConnection {

    // Endpoint REALI della DirectoryService (non cambiano mai)
    private final String directoryHost;
    private final int directoryPort;

    /**
     * Constructs a DirectoryAwareClientConnection using the Directory Service host and port.
     *
     * @param directoryHost the host of the Directory Service
     * @param directoryPort the port of the Directory Service
     */
    public DirectoryAwareClientConnection(String directoryHost, int directoryPort) {
        super(directoryHost, directoryPort);
        this.directoryHost = directoryHost;
        this.directoryPort = directoryPort;
    }

    /**
     * Opens the connection by querying the Directory Service for the best broker,
     * then connects to the selected broker.
     *
     * @throws IOException if no broker is available or a protocol error occurs
     */
    @Override
    public void open() throws IOException {
        System.out.println("[CONN] DirectoryAwareClientConnection.open() - INIZIO");
        System.out.println("[CONN] Directory host/port = " + directoryHost + ":" + directoryPort);

        ClientDirectory dirClient = new ClientDirectory(directoryHost, directoryPort);

        ClientDirectory.BrokerInfo brokerInfo;
        try {
            System.out.println("[CONN] Contatto DirectoryService per ottenere un broker...");
            brokerInfo = dirClient.getBestBroker();
        } catch (ClassNotFoundException e) {
            System.err.println("[CONN] Eccezione ClassNotFound durante getBestBroker: " + e.getMessage());
            throw new IOException("Errore nel protocollo con il DirectoryService", e);
        }

        if (brokerInfo == null) {
            System.err.println("[CONN] Nessun broker disponibile restituito dalla Directory.");
            throw new IOException("Nessun broker disponibile al momento");
        }

        String brokerHost = brokerInfo.getHost();
        int brokerPort    = brokerInfo.getPort();
        int brokerId      = brokerInfo.getBrokerId();

        System.out.println("[CONN] Directory ha scelto broker id=" + brokerId +
                " host=" + brokerHost + " port=" + brokerPort);

        // Cambiamo host/port della connessione verso il broker scelto
        reopenTo(brokerHost, brokerPort);
        System.out.println("[CONN] Dopo reopenTo, getHost()/getPort() = " + getHost() + ":" + getPort());

        // Apriamo la socket verso il broker
        super.open();
        System.out.println("[CONN] Connessione aperta verso broker " + getHost() + ":" + getPort());
        System.out.println("[CONN] DirectoryAwareClientConnection.open() - FINE");
    }
}
