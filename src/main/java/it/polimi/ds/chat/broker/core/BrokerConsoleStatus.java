package it.polimi.ds.chat.broker.core;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.InfoCmp;
import org.jline.utils.Status;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Renders one best-effort broker status line without making terminal behavior
 * part of the broker's networking or consensus paths.
 */
final class BrokerConsoleStatus implements Consumer<String>, AutoCloseable {

    private final Terminal terminal;
    private final Status status;
    private final PrintStream fallbackOutput;
    private final Object renderLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private int fallbackWidth;

    private BrokerConsoleStatus(Terminal terminal, Status status, PrintStream fallbackOutput) {
        this.terminal = terminal;
        this.status = status;
        this.fallbackOutput = fallbackOutput;
    }

    static BrokerConsoleStatus create() {
        Terminal terminal = null;
        try {
            terminal = TerminalBuilder.builder()
                    .system(true)
                    .dumb(true)
                    .build();
            if (supportsPersistentStatus(terminal)) {
                return new BrokerConsoleStatus(
                        terminal,
                        Status.getStatus(terminal),
                        null);
            }
        } catch (IOException | RuntimeException ignored) {
            // A debug display must never prevent the broker from starting.
        }
        closeQuietly(terminal);
        return new BrokerConsoleStatus(null, null, System.out);
    }

    private static boolean supportsPersistentStatus(Terminal terminal) {
        return terminal.getWidth() > 0
                && terminal.getHeight() > 1
                && hasCapability(terminal, InfoCmp.Capability.change_scroll_region)
                && hasCapability(terminal, InfoCmp.Capability.save_cursor)
                && hasCapability(terminal, InfoCmp.Capability.restore_cursor)
                && hasCapability(terminal, InfoCmp.Capability.cursor_address);
    }

    private static boolean hasCapability(Terminal terminal, InfoCmp.Capability capability) {
        return terminal.getStringCapability(capability) != null;
    }

    @Override
    public void accept(String line) {
        if (closed.get()) {
            return;
        }

        String singleLine = line.replace('\r', ' ').replace('\n', ' ');
        synchronized (renderLock) {
            if (closed.get()) {
                return;
            }
            if (status != null) {
                status.update(List.of(new AttributedString(singleLine)));
                return;
            }

            int padding = Math.max(0, fallbackWidth - singleLine.length());
            fallbackOutput.print("\r" + singleLine + " ".repeat(padding));
            fallbackOutput.flush();
            fallbackWidth = singleLine.length();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        synchronized (renderLock) {
            if (status != null) {
                status.hide();
                status.close();
            } else if (fallbackWidth > 0) {
                fallbackOutput.print("\r" + " ".repeat(fallbackWidth) + "\r");
                fallbackOutput.flush();
                fallbackWidth = 0;
            }
        }

        if (terminal != null) {
            closeQuietly(terminal);
        }
    }

    private static void closeQuietly(Terminal terminal) {
        if (terminal == null) {
            return;
        }
        try {
            terminal.close();
        } catch (IOException ignored) {
        }
    }
}
