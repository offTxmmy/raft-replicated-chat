package it.polimi.ds.chat.protocol.client;

import java.io.Serializable;

public class ClientMessage implements Serializable {
    private final String username;
    private final String clientId;
    private final long clientSeq;
    private final String text;

    public ClientMessage(String username, String text, long clientSeq) {
        this(username, username, clientSeq, text);
    }

    public ClientMessage(String username, String clientId, long clientSeq, String text) {
        this.username = username;
        this.clientId = clientId;
        this.clientSeq = clientSeq;
        this.text = text;
    }

    public String getUsername() {
        return username;
    }

    public String getClientId() {
        return clientId;
    }

    public long getClientSeq() {
        return clientSeq;
    }

    /**
     * Legacy name kept for older call sites; semantically this is now clientSeq.
     */
    public long getTimestamp() {
        return clientSeq;
    }

    public String getText() {
        return text;
    }

    public static String msgCommand(String text) {
        return "MSG " + text;
    }

    public static boolean isMsg(String line) {
        return line != null && line.startsWith("MSG");
    }

    public static ClientMessage fromClientLine(String username, String line) {
        String[] parts = line.split("\\s+", 4);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed MSG line: " + line);
        }

        if (parts.length >= 4 && !isLong(parts[1])) {
            String clientId = parts[1];
            long clientSeq = Long.parseLong(parts[2]);
            String text = parts[3];
            return new ClientMessage(username, clientId, clientSeq, text);
        }

        long clientSeq = Long.parseLong(parts[1]);
        String text = line.split("\\s+", 3)[2];
        return new ClientMessage(username, text, clientSeq);
    }

    public static String msgToClient(long seq, String sender, String text) {
        return "MSG " + seq + " " + sender + ":" + text;
    }

    private static boolean isLong(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
