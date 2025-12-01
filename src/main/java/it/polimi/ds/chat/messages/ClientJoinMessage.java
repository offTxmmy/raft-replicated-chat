package it.polimi.ds.chat.messages;

public class ClientJoinMessage extends ClientMessage {
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
