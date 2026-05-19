package it.polimi.ds.chat.protocol.client;

import java.io.Serializable;


public class HeartbeatMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int brokerId;
    private final long timestamp;


    public HeartbeatMessage(int brokerId, long timestamp) {
        this.brokerId = brokerId;
        this.timestamp = timestamp;
    }


    public HeartbeatMessage(long timestamp) {
        this.brokerId = -1;
        this.timestamp = timestamp;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "HeartbeatMessage{" +
                "brokerId=" + brokerId +
                ", timestamp=" + timestamp +
                '}';
    }

    //   METODI DI SUPPORTO PER IL PROTOCOLLO TESTUALE LATO CLIENT


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
