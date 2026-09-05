package it.polimi.ds.chat.common.net;

import java.net.Socket;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Closing a socket bounds writes as well as reads; SO_TIMEOUT alone cannot bound writes. */
public final class SocketDeadline implements AutoCloseable {
    private static final ScheduledThreadPoolExecutor TIMER = new ScheduledThreadPoolExecutor(1, task -> {
        Thread thread = new Thread(task, "socket-deadlines");
        thread.setDaemon(true);
        return thread;
    });
    static { TIMER.setRemoveOnCancelPolicy(true); }
    private final ScheduledFuture<?> timeout;

    public SocketDeadline(Socket socket, long timeoutMillis) {
        timeout = TIMER.schedule(() -> {
            try { socket.close(); } catch (IOException ignored) { }
        }, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override public void close() { timeout.cancel(false); }
}
