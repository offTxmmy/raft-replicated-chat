package it.polimi.ds.chat.messages;

public class ClientQuitMessage extends ClientMessage{
    public ClientQuitMessage(String username, String text, long timestamp) {
        super(username, text, timestamp);
    }

    public static String quitCommand() {
        return "QUIT";
    }

    public static boolean isQuit(String line) {
        return line != null && line.startsWith("QUIT");
    }
}
