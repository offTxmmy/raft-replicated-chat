package it.polimi.ds.chat.protocol.client;

public class ClientJoinMessage extends ClientMessage {
    public ClientJoinMessage(String username, String text, long timestamp) {
        super(username, text, timestamp);
    }

    public static String joinCommand(String username) {
        return "JOIN " + username;
    }

    public static Boolean isJoin(String line) {
        return line!=null && line.startsWith("JOIN");
    }

    public static String parseJoin(String line) {
        return line.substring("JOIN".length()).trim();
    }

    public static String welcome(String username) {
        return "Welcome " + username;
    }
}
