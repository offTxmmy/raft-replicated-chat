package it.polimi.ds.chat.protocol.client;

public class ClientAckMessages {
    private static final String PREFIX = "ACK";
    public static boolean isAck(String line) {
       return line != null && line.startsWith(PREFIX);
    }

    public static long parseTimestamp(String line) throws NumberFormatException {
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed ACK: " + line);
        }
        return Long.parseLong(parts[1]);
    }

    public static String parseUsername(String line) {
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed ACK: " + line);
        }
        return parts[2];
    }

    public static String buildAck(long timestamp, String username) {
        return PREFIX + " " + timestamp + " " + username;
    }
}
