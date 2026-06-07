package it.polimi.ds.chat.protocol.client;

public class ClientJoinMessage extends ClientMessage {
    private static final String JOIN_PREFIX = "JOIN";
    private static final String JOIN_WITH_ID_PREFIX = "JOIN_ID";

    public ClientJoinMessage(String username, String text, long timestamp) {
        super(username, text, timestamp);
    }

    public ClientJoinMessage(String username, String clientId, String text, long timestamp) {
        super(username, clientId, timestamp, text);
    }

    public static String joinCommand(String username) {
        return JOIN_PREFIX + " " + username;
    }

    public static String joinCommand(String username, String clientId) {
        return JOIN_WITH_ID_PREFIX + " " + clientId + " " + username;
    }

    public static Boolean isJoin(String line) {
        return line != null
                && (line.startsWith(JOIN_PREFIX + " ")
                || line.startsWith(JOIN_WITH_ID_PREFIX + " "));
    }

    public static String parseJoin(String line) {
        if (line.startsWith(JOIN_WITH_ID_PREFIX + " ")) {
            String[] parts = line.split("\\s+", 3);
            if (parts.length < 3) {
                throw new IllegalArgumentException("Formato JOIN_ID non valido: " + line);
            }
            return parts[2].trim();
        }

        return line.substring(JOIN_PREFIX.length()).trim();
    }

    public static String parseClientId(String line) {
        if (!line.startsWith(JOIN_WITH_ID_PREFIX + " ")) {
            return null;
        }

        String[] parts = line.split("\\s+", 3);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Formato JOIN_ID non valido: " + line);
        }
        return parts[1];
    }

    public static String welcome(String username) {
        return "Welcome " + username;
    }
}
