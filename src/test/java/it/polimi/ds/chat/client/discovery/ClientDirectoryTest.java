package it.polimi.ds.chat.client.discovery;

import it.polimi.ds.chat.protocol.directory.GetBrokerRequestMessage;
import it.polimi.ds.chat.protocol.directory.GetBrokerResponseMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientDirectoryTest {

    @Test
    void normalLookupReturnsTheDirectorySelection() throws Exception {
        GetBrokerResponseMessage response = new GetBrokerResponseMessage(
                true, "broker.example", 5_001, 7);
        try (OneShotDirectoryServer server =
                     OneShotDirectoryServer.respondingWith(response)) {
            ClientDirectory.BrokerInfo info = new ClientDirectory(
                    "127.0.0.1", server.port(), 250, 250).getBestBroker();

            assertNotNull(info);
            assertEquals("broker.example", info.getHost());
            assertEquals(5_001, info.getPort());
            assertEquals(7, info.getBrokerId());
            assertTrue(server.requestReceived.await(1, TimeUnit.SECONDS));
            server.awaitSuccessfulCompletion();
        }
    }

    @Test
    void connectFailureProducesIOExceptionWithinTheAttemptBound() throws Exception {
        int unusedPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            unusedPort = reservation.getLocalPort();
        }

        ClientDirectory directory = new ClientDirectory(
                "127.0.0.1", unusedPort, 100, 100);

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertThrows(IOException.class, directory::getBestBroker));
    }

    @Test
    void acceptedConnectionWithoutResponseTimesOut() throws Exception {
        try (OneShotDirectoryServer server =
                     OneShotDirectoryServer.stallingAfterRequest()) {
            ClientDirectory directory = new ClientDirectory(
                    "127.0.0.1", server.port(), 250, 150);

            IOException failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertThrows(IOException.class, directory::getBestBroker)
            );

            assertInstanceOf(SocketTimeoutException.class, failure);
            assertTrue(server.requestReceived.await(1, TimeUnit.SECONDS));
        }
    }

    private static final class OneShotDirectoryServer implements AutoCloseable {
        private final ServerSocket listener;
        private final GetBrokerResponseMessage response;
        private final CountDownLatch requestReceived = new CountDownLatch(1);
        private final CountDownLatch releaseStall = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final Thread thread;

        private volatile Socket acceptedSocket;
        private volatile Throwable failure;
        private volatile boolean closing;

        private OneShotDirectoryServer(GetBrokerResponseMessage response)
                throws IOException {
            this.listener = new ServerSocket(0);
            this.response = response;
            this.thread = new Thread(this::serve, "one-shot-directory");
            thread.setDaemon(true);
            thread.start();
        }

        private static OneShotDirectoryServer respondingWith(
                GetBrokerResponseMessage response) throws IOException {
            return new OneShotDirectoryServer(response);
        }

        private static OneShotDirectoryServer stallingAfterRequest()
                throws IOException {
            return new OneShotDirectoryServer(null);
        }

        private int port() {
            return listener.getLocalPort();
        }

        private void serve() {
            try (Socket socket = listener.accept()) {
                acceptedSocket = socket;
                ObjectOutputStream output =
                        new ObjectOutputStream(socket.getOutputStream());
                output.flush();
                ObjectInputStream input =
                        new ObjectInputStream(socket.getInputStream());
                try (output; input) {
                    Object request = input.readObject();
                    if (!(request instanceof GetBrokerRequestMessage)) {
                        throw new IOException("Unexpected Directory request " + request);
                    }
                    requestReceived.countDown();

                    if (response == null) {
                        releaseStall.await();
                    } else {
                        output.writeObject(response);
                        output.flush();
                    }
                }
            } catch (SocketException e) {
                if (!closing) {
                    failure = e;
                }
            } catch (Throwable e) {
                failure = e;
            } finally {
                requestReceived.countDown();
                completed.countDown();
            }
        }

        private void awaitSuccessfulCompletion() throws Exception {
            assertTrue(completed.await(1, TimeUnit.SECONDS));
            if (failure != null) {
                throw new AssertionError("Directory test server failed", failure);
            }
        }

        @Override
        public void close() throws Exception {
            closing = true;
            releaseStall.countDown();
            listener.close();
            Socket socket = acceptedSocket;
            if (socket != null) {
                socket.close();
            }
            thread.join(1_000L);
        }
    }
}
