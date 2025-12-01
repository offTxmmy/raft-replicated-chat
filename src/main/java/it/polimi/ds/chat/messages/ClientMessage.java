package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class ClientMessage implements Serializable {
    private final String username;
    private final long timestamp;
    private final String text;

    public ClientMessage(String username, String text, long timestamp) {
        this.username = username;
        this.text = text;
        this.timestamp = timestamp;
    }

    public String getUsername() {
        return username;
    }

    public long getTimestamp() {
        return timestamp;
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
        String[] parts = line.split("\\s+", 3);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed MSG line: " + line);
        }
        long timestamp = Long.parseLong(parts[1]);
        String text = parts[2];
        return new ClientMessage(username, text, timestamp);
    }

    public static String msgToClient(long seq, String sender, String text) {
        return "MSG " + seq + " " + sender + ":" + text;
    }
}
