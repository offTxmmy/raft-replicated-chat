package it.polimi.ds.chat.externalservices;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DirectoryService
 *
 * directoryservice conosce tutti i broker poichè all'entrata nella rete contattano il suddetto,
 * ha una lista di broker e client connessi ad ognuno.
 *
 * - continua a pingare tutti i brokers
 * - al ricevere di una richiesta di connessione smista i client in base al numero di
 *   connessioni ad ogni broker (il broker deve notificare quando un client si disconnette
 *   ed al corretto raggiungimento di una connessione per incrementare e decrementare)
 * - se dopo N ping il server non risponde lo rimuove dalla lista
 *
 * In caso di crash tutti i client ricontatteranno in automatico questa directory service per
 * riconnettersi ad un altro broker.
 */
public class DirectoryService {

    /**
     * Informazioni su un broker registrato.
     */
    public static class BrokerInfo {
        private final String brokerId;   // es. "broker-1"
        private final String host;
        private final int port;         // porta a cui i client si connettono

        private volatile int connectedClients;
        private volatile int failedPings;
        private volatile boolean alive;
        private volatile Instant lastHeartbeat;

        private BrokerInfo(String brokerId, String host, int port) {
            this.brokerId = brokerId;
            this.host = host;
            this.port = port;
            this.connectedClients = 0;
            this.failedPings = 0;
            this.alive = true;
            this.lastHeartbeat = Instant.now();
        }

        public String getBrokerId() {
            return brokerId;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public int getConnectedClients() {
            return connectedClients;
        }

        public int getFailedPings() {
            return failedPings;
        }

        public boolean isAlive() {
            return alive;
        }

        public Instant getLastHeartbeat() {
            return lastHeartbeat;
        }

        private void incrementClients() {
            connectedClients++;
        }

        private void decrementClients() {
            if (connectedClients > 0) {
                connectedClients--;
            }
        }

        private void resetFailedPings() {
            this.failedPings = 0;
            this.alive = true;
            this.lastHeartbeat = Instant.now();
        }

        private void incrementFailedPings() {
            this.failedPings++;
        }

        private void markDead() {
            this.alive = false;
        }

        @Override
        public String toString() {
            return "BrokerInfo{" +
                    "brokerId='" + brokerId + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", connectedClients=" + connectedClients +
                    ", failedPings=" + failedPings +
                    ", alive=" + alive +
                    '}';
        }
    }

    private final long pingIntervalMillis;   // ogni quanto pingare i broker
    private final int pingTimeoutMillis;     // timeout del singolo ping (socket connect)
    private final int maxFailedPings;        // dopo quanti ping falliti considerare il broker morto

    // STATO INTERNO

    /**
     * Mappa brokerId -> BrokerInfo
     */
    private final Map<String, BrokerInfo> brokers = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "DirectoryService-Pinger");
                t.setDaemon(true);
                return t;
            });

    private volatile boolean running = false;

    // ============================ COSTRUTTORI =================================

    /**
     * Costruttore principale.
     *
     * @param pingIntervalMillis intervallo tra un ping e l'altro verso ogni broker
     * @param pingTimeoutMillis  timeout per il tentativo di connessione di ping
     * @param maxFailedPings     numero massimo di ping falliti prima di marcare il broker come morto e rimuoverlo
     */
    public DirectoryService(long pingIntervalMillis, int pingTimeoutMillis, int maxFailedPings) {
        this.pingIntervalMillis = pingIntervalMillis;
        this.pingTimeoutMillis = pingTimeoutMillis;
        this.maxFailedPings = maxFailedPings;
    }

    /**
     * Costruttore con valori di default "ragionevoli".
     */
    public DirectoryService() {
        this(2_000L, 1_000, 3); // ping ogni 2s, timeout 1s, dopo 3 fallimenti broker rimosso
    }

    // CICLO DI VITA

    /**
     * Avvia lo scheduler che pinga tutti i broker registrati.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        scheduler.scheduleAtFixedRate(this::pingAllBrokers,
                pingIntervalMillis,
                pingIntervalMillis,
                TimeUnit.MILLISECONDS);
        System.out.println("[DirectoryService] Started ping loop.");
    }

    /**
     * Ferma il DirectoryService e lo scheduler interno.
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        scheduler.shutdownNow();
        System.out.println("[DirectoryService] Stopped.");
    }

    // =========================== REGISTRAZIONE BROKER =========================

    /**
     * Chiamato dal broker alla partenza per registrarsi nella directory.
     *
     * @param brokerId identificativo del broker
     * @param host     host su cui il broker accetta connessioni dai client
     * @param port     porta su cui il broker accetta connessioni dai client
     */
    public void registerBroker(String brokerId, String host, int port) {
        BrokerInfo info = new BrokerInfo(brokerId, host, port);
        brokers.put(brokerId, info);
        System.out.println("[DirectoryService] Registered broker: " + info);
    }

    /**
     * Chiamato dal broker o da un amministratore per deregistrare il broker.
     */
    public void unregisterBroker(String brokerId) {
        BrokerInfo removed = brokers.remove(brokerId);
        if (removed != null) {
            System.out.println("[DirectoryService] Unregistered broker: " + removed);
        }
    }

    // ====================== NOTIFICHE DA PARTE DEI BROKER =====================

    /**
     * Il broker chiama questo metodo quando ha stabilito una nuova connessione con un client.
     */
    public void notifyClientConnected(String brokerId) {
        BrokerInfo info = brokers.get(brokerId);
        if (info != null) {
            info.incrementClients();
            System.out.println("[DirectoryService] Client connected to " + brokerId +
                    ", total=" + info.getConnectedClients());
        }
    }

    /**
     * Il broker chiama questo metodo quando un client si disconnette.
     */
    public void notifyClientDisconnected(String brokerId) {
        BrokerInfo info = brokers.get(brokerId);
        if (info != null) {
            info.decrementClients();
            System.out.println("[DirectoryService] Client disconnected from " + brokerId +
                    ", total=" + info.getConnectedClients());
        }
    }

    // ASSEGNAZIONE BROKER AI CLIENT

    /**
     * Restituisce il broker "migliore" (alive, con meno client connessi).
     * Questo metodo è quello che userai quando il DirectoryService
     * riceve una richiesta di connessione da un nuovo client.
     *
     * @return Optional con BrokerInfo se presente almeno un broker valido
     */
    public Optional<BrokerInfo> chooseBrokerForNewClient() {
        return brokers.values().stream()
                .filter(BrokerInfo::isAlive)
                .min(Comparator.comparingInt(BrokerInfo::getConnectedClients));
    }

    // PING / HEARTBEAT

    /**
     * Esegue il ping di tutti i broker registrati.
     * Viene invocato periodicamente dallo scheduler interno.
     */
    private void pingAllBrokers() {
        if (!running) {
            return;
        }

        for (BrokerInfo info : brokers.values()) {
            boolean ok = pingBroker(info);

            if (ok) {
                info.resetFailedPings();
            } else {
                info.incrementFailedPings();
                System.out.println("[DirectoryService] Ping FAILED for broker "
                        + info.getBrokerId() + " (failed=" + info.getFailedPings() + ")");

                if (info.getFailedPings() >= maxFailedPings) {
                    // Consideriamo il broker morto
                    info.markDead();
                    brokers.remove(info.getBrokerId());
                    System.out.println("[DirectoryService] Broker " + info.getBrokerId()
                            + " removed after " + info.getFailedPings() + " failed pings.");
                }
            }
        }
    }

    /**
     * Prova ad aprire una connessione TCP al broker.
     * Se ci riesce entro il timeout, consideriamo il broker vivo.
     * Qui puoi eventualmente implementare un mini protocollo PING/PONG.
     */
    private boolean pingBroker(BrokerInfo info) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(info.getHost(), info.getPort()), pingTimeoutMillis);
            // Se arriviamo qui, la connessione è andata a buon fine
            return true;
        } catch (IOException e) {
            // il broker non risponde o la connessione fallisce
            return false;
        }
    }

    // METODI DI UTILITÀ

    /**
     * Restituisce una copia "safe" della mappa broker per logging/debug.
     */
    public Map<String, BrokerInfo> snapshotBrokers() {
        return Map.copyOf(brokers);
    }
}
