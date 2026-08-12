package it.polimi.ds.chat.client.connection;

import it.polimi.ds.chat.client.discovery.ClientDirectory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * A client connection that asks the Directory Service for a broker before each
 * connection attempt.
 *
 * <p>Recently failed endpoints are quarantined for a bounded interval. If the
 * Directory has not reaped a dead broker yet, the reconnect loop rejects that
 * stale answer without repeatedly opening the same dead socket. The quarantine
 * expires so a broker that restarts at the same endpoint remains reachable.</p>
 */
public class DirectoryAwareClientConnection extends ClientConnection {

    private static final long DEFAULT_EXCLUSION_NANOS =
            TimeUnit.SECONDS.toNanos(5L);

    @FunctionalInterface
    interface BrokerLookup {
        ClientDirectory.BrokerInfo getBestBroker(Set<Integer> excludedBrokerIds)
                throws IOException, ClassNotFoundException;
    }

    private record Endpoint(int brokerId, String host, int port) {
    }

    private record HostPort(String host, int port) {
    }

    private final String directoryHost;
    private final int directoryPort;
    private final BrokerLookup brokerLookup;
    private final LongSupplier nanoTime;
    private final long exclusionNanos;
    private final Map<Endpoint, Long> excludedUntil = new ConcurrentHashMap<>();
    private final Map<HostPort, Integer> knownBrokerIds = new ConcurrentHashMap<>();

    public DirectoryAwareClientConnection(String directoryHost, int directoryPort) {
        this(
                directoryHost,
                directoryPort,
                excludedBrokerIds -> new ClientDirectory(directoryHost, directoryPort)
                        .getBestBroker(excludedBrokerIds),
                System::nanoTime,
                DEFAULT_EXCLUSION_NANOS
        );
    }

    DirectoryAwareClientConnection(String directoryHost,
                                   int directoryPort,
                                   BrokerLookup brokerLookup,
                                   LongSupplier nanoTime,
                                   long exclusionNanos) {
        super(directoryHost, directoryPort);
        if (exclusionNanos < 0L) {
            throw new IllegalArgumentException("exclusionNanos must be non-negative");
        }
        this.directoryHost = directoryHost;
        this.directoryPort = directoryPort;
        this.brokerLookup = Objects.requireNonNull(brokerLookup, "brokerLookup");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.exclusionNanos = exclusionNanos;
    }

    @Override
    public void open() throws IOException {
        ClientDirectory.BrokerInfo brokerInfo;
        try {
            long now = nanoTime.getAsLong();
            discardExpiredExclusions(now);
            Set<Integer> excludedBrokerIds = excludedUntil.keySet().stream()
                    .map(Endpoint::brokerId)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            brokerInfo = brokerLookup.getBestBroker(excludedBrokerIds);
        } catch (ClassNotFoundException e) {
            throw new IOException("Invalid Directory Service response", e);
        }

        if (brokerInfo == null) {
            throw new IOException("No broker is currently available");
        }

        Endpoint endpoint = new Endpoint(
                brokerInfo.getBrokerId(),
                brokerInfo.getHost(),
                brokerInfo.getPort()
        );
        knownBrokerIds.put(
                new HostPort(endpoint.host(), endpoint.port()),
                endpoint.brokerId()
        );
        long now = nanoTime.getAsLong();
        discardExpiredExclusions(now);

        Long deadline = excludedUntil.get(endpoint);
        if (deadline != null && now < deadline) {
            throw new IOException(
                    "Directory returned recently failed broker "
                            + endpoint.host() + ":" + endpoint.port()
            );
        }

        try {
            openTo(endpoint.host(), endpoint.port());
            excludedUntil.remove(endpoint);
        } catch (IOException e) {
            exclude(endpoint, now);
            throw e;
        }
    }

    @Override
    public void markEndpointFailed(ClientConnectionGeneration failedGeneration) {
        if (failedGeneration == null) {
            return;
        }
        exclude(
                new Endpoint(
                        knownBrokerIds.getOrDefault(
                                new HostPort(
                                        failedGeneration.getHost(),
                                        failedGeneration.getPort()
                                ),
                                -1
                        ),
                        failedGeneration.getHost(),
                        failedGeneration.getPort()
                ),
                nanoTime.getAsLong()
        );
    }

    void markEndpointFailed(int brokerId, String failedHost, int failedPort) {
        exclude(new Endpoint(brokerId, failedHost, failedPort), nanoTime.getAsLong());
    }

    boolean isTemporarilyExcluded(String candidateHost, int candidatePort) {
        long now = nanoTime.getAsLong();
        discardExpiredExclusions(now);
        return excludedUntil.entrySet().stream().anyMatch(entry ->
                entry.getKey().host().equals(candidateHost)
                        && entry.getKey().port() == candidatePort
                        && now < entry.getValue());
    }

    private void exclude(Endpoint endpoint, long now) {
        if (exclusionNanos == 0L) {
            return;
        }
        excludedUntil.put(endpoint, now + exclusionNanos);
    }

    private void discardExpiredExclusions(long now) {
        excludedUntil.entrySet().removeIf(entry -> now >= entry.getValue());
        /*
         * Keep the endpoint-to-id association after a successful open. A later
         * receiver/heartbeat failure only carries the immutable connection
         * generation (host/port), and needs this id to exclude the broker on the
         * very next Directory lookup. The broker set is static and therefore
         * bounds this small cache naturally.
         */
    }

    public String getDirectoryHost() {
        return directoryHost;
    }

    public int getDirectoryPort() {
        return directoryPort;
    }
}
