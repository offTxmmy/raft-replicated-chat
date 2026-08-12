package it.polimi.ds.chat.client.messaging;

import it.polimi.ds.chat.client.connection.ClientObjectWriter;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Sends client chat messages using a FIFO, single-in-flight discipline.
 *
 * <p>Only the queue head may be written or retried. A commit ACK for that head
 * removes it and enables the next message. Since the broker emits that ACK only
 * after the proposal is committed/applied, a later client sequence can never
 * overtake an earlier one during retry, failover or reconnect.</p>
 */
public class ClientMessageSender implements Runnable {

    private static final long RETRY_POLL_MS = 100L;

    private final Object stateLock = new Object();
    private final String clientId;
    private final long ackTimeoutMs;
    private final LongSupplier currentTimeMillis;
    private final Deque<ClientPendingMessage> pendingMessages = new ArrayDeque<>();
    private final AtomicLong seqCounter = new AtomicLong(0L);

    private ClientObjectWriter writer;
    private volatile boolean running = true;

    public ClientMessageSender(ObjectOutputStream out, long ackTimeoutMs) {
        this(out, ackTimeoutMs, UUID.randomUUID().toString());
    }

    public ClientMessageSender(ObjectOutputStream out,
                               long ackTimeoutMs,
                               String clientId) {
        this(
                new ClientObjectWriter(Objects.requireNonNull(out, "out")),
                ackTimeoutMs,
                clientId,
                System::currentTimeMillis
        );
    }

    /** Creates a disconnected sender; a connection generation is attached later. */
    public ClientMessageSender(long ackTimeoutMs, String clientId) {
        this(null, ackTimeoutMs, clientId, System::currentTimeMillis);
    }

    ClientMessageSender(ClientObjectWriter writer,
                        long ackTimeoutMs,
                        String clientId,
                        LongSupplier currentTimeMillis) {
        if (ackTimeoutMs <= 0L) {
            throw new IllegalArgumentException("ackTimeoutMs must be positive");
        }
        this.writer = writer;
        this.ackTimeoutMs = ackTimeoutMs;
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.currentTimeMillis = Objects.requireNonNull(
                currentTimeMillis,
                "currentTimeMillis"
        );
    }

    /**
     * Atomically switches the sender to the writer of a new connection
     * generation and immediately retransmits the current head, if any.
     */
    public void attachWriter(ClientObjectWriter newWriter) throws IOException {
        Objects.requireNonNull(newWriter, "newWriter");
        synchronized (stateLock) {
            writer = newWriter;
            sendHeadIfEligibleLocked(true);
            stateLock.notifyAll();
        }
    }

    /** Clears the writer only when the failed generation is still attached. */
    public void detachWriter(ClientObjectWriter expectedWriter) {
        synchronized (stateLock) {
            if (writer == expectedWriter) {
                writer = null;
            }
            stateLock.notifyAll();
        }
    }

    /** Assigns a sequence and queues a user message. Only the head is sent. */
    public void sendUserMessage(String text) {
        synchronized (stateLock) {
            long clientSeq = seqCounter.incrementAndGet();
            ClientPendingMessage pending = new ClientPendingMessage(
                    clientSeq,
                    buildMsgWire(clientSeq, text)
            );
            pendingMessages.addLast(pending);
            if (pendingMessages.peekFirst() == pending) {
                try {
                    sendHeadIfEligibleLocked(false);
                } catch (IOException e) {
                    System.err.println("[SEND] Failed to send message: "
                            + e.getMessage());
                }
            }
            stateLock.notifyAll();
        }
    }

    private String buildMsgWire(long clientSeq, String text) {
        return "MSG " + clientId + " " + clientSeq + " " + text;
    }

    /**
     * Accepts only an ACK for this sender's current queue head. Duplicate,
     * obsolete and out-of-order ACKs cannot skip a pending message.
     */
    public void handleAck(String acknowledgedClientId, long clientSeq) {
        if (!clientId.equals(acknowledgedClientId)) {
            return;
        }

        synchronized (stateLock) {
            ClientPendingMessage head = pendingMessages.peekFirst();
            if (head == null || head.getClientSeq() != clientSeq) {
                return;
            }

            pendingMessages.removeFirst();
            try {
                sendHeadIfEligibleLocked(false);
            } catch (IOException e) {
                System.err.println("[SEND] Failed to send next FIFO message: "
                        + e.getMessage());
            }
            stateLock.notifyAll();
        }
    }

    public void handleAck(long clientSeq) {
        handleAck(clientId, clientSeq);
    }

    public void shutdown() {
        running = false;
        synchronized (stateLock) {
            stateLock.notifyAll();
        }
    }

    @Override
    public void run() {
        while (running) {
            synchronized (stateLock) {
                if (!running) {
                    return;
                }

                try {
                    sendHeadIfEligibleLocked(false);
                } catch (IOException e) {
                    System.err.println("[RETRY] Failed to retransmit FIFO head: "
                            + e.getMessage());
                }

                try {
                    stateLock.wait(RETRY_POLL_MS);
                } catch (InterruptedException e) {
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** Deterministic retry tick used by component tests. */
    void retryExpiredHead() throws IOException {
        synchronized (stateLock) {
            sendHeadIfEligibleLocked(false);
        }
    }

    int pendingCount() {
        synchronized (stateLock) {
            return pendingMessages.size();
        }
    }

    Long pendingHeadSequence() {
        synchronized (stateLock) {
            ClientPendingMessage head = pendingMessages.peekFirst();
            return head == null ? null : head.getClientSeq();
        }
    }

    private void sendHeadIfEligibleLocked(boolean force) throws IOException {
        ClientPendingMessage head = pendingMessages.peekFirst();
        if (head == null || writer == null) {
            return;
        }

        long now = currentTimeMillis.getAsLong();
        boolean retryDue = head.hasBeenSent()
                && now - head.getLastSendTime() >= ackTimeoutMs;
        if (!force && head.hasBeenSent() && !retryDue) {
            return;
        }

        writer.send(head.getWireLine());
        head.markSent(now);
    }
}
