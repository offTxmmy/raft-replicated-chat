package it.polimi.ds.chat.protocol.client;

public class ClientAckMessages {
    private static final String PREFIX = "ACK";
    public static boolean isAck(String line) {
       return line != null && line.startsWith(PREFIX);
    }

    public static String parseClientId(String line) {
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed ACK: " + line);
        }
        return parts[1];
    }

    public static long parseClientSeq(String line) throws NumberFormatException {
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed ACK: " + line);
        }
        return Long.parseLong(parts[2]);
    }

    /**
     * Legacy parser kept for the old ACK timestamp name.
     */
    public static long parseTimestamp(String line) throws NumberFormatException {
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed ACK: " + line);
        }
        if (isLong(parts[1])) {
            return Long.parseLong(parts[1]);
        }
        return Long.parseLong(parts[2]);
    }

    /**
     * Legacy parser kept for the old ACK username field.
     */
    public static String parseUsername(String line) {
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Malformed ACK: " + line);
        }
        return parts[2];
    }

    public static String buildAck(String clientId, long clientSeq) {
        return PREFIX + " " + clientId + " " + clientSeq;
    }

    /**
     * Legacy builder kept for older call sites.
     */
    public static String buildAck(long timestamp, String username) {
        return PREFIX + " " + timestamp + " " + username;
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
