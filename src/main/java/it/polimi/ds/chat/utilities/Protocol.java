package it.polimi.ds.chat.utilities;

public class Protocol {

    private Protocol() {

    }

    // Client --> Broker
    public static String joinCommand(String username) {
        return "JOIN " + username;
    }

    public static String msgCommand(String text) {
        return "MSG " + text;
    }

    public static String quitCommand() {
        return "QUIT";
    }

    // Broker --> Client
    public static String welcomeMessage() {
        return "Welcome";
    }

    public static String msgToClient(String sender, String text) {
        return "MSG " + sender + ":" + text;
    }

    public static Boolean isJoin(String line) {
        return line!=null && line.startsWith("JOIN");
    }

    public static boolean isMsg(String line) {
        return line != null && line.startsWith("MSG");
    }

    public static boolean isQuit(String line) {
        return line != null && line.startsWith("QUIT");
    }
    
    public static String parseJoin(String line) {
        return line.substring("JOIN".length()).trim();
    }

    public static String parseMsg(String line) {
        return line.substring("MSG".length());
    }

    public static String welcome(String username) {
        return "Welcome " + username;
    }
}