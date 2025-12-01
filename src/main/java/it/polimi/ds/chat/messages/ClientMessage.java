package it.polimi.ds.chat.messages;

import java.io.Serializable;

public class ClientMessage implements Serializable {
    public static String msgCommand(String text) {
        return "MSG " + text;
    }

    public static boolean isMsg(String line) {
        return line != null && line.startsWith("MSG");
    }

    public static String parseMsg(String line) {
        return line.substring("MSG".length());
    }

    public static String msgToClient(long seq, String sender, String text) {
        return "MSG " + seq + " " + sender + ":" + text;
    }
}
