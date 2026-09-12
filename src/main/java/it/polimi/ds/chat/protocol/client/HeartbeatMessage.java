package it.polimi.ds.chat.protocol.client;

import java.io.Serializable;


public class HeartbeatMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long timestamp;

    public HeartbeatMessage(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "HeartbeatMessage{" +
                "timestamp=" + timestamp +
                '}';
    }

    public String toWireString() {
        return "HEARTBEAT " + timestamp;
    }


    public static boolean isHeartbeatLine(String line) {
        return line != null && line.startsWith("HEARTBEAT ");
    }

    public static long parseTimestampFromWire(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Linea nulla per heartbeat");
        }
        String[] parts = line.trim().split("\\s+");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Formato HEARTBEAT non valido: " + line);
        }
        return Long.parseLong(parts[1]);
    }
}
