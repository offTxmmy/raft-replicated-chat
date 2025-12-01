package it.polimi.ds.chat.utilities;

/*public class Protocol {

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

    public static String msgToClient(long seq, String sender, String text) {
        return "MSG " + seq + " " + sender + ":" + text;
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

    // Broker --> Sequencer
    public static String chatReq(String localMsgId, int brokerId, String username, String text) {
        return "CHAT_REQ " + localMsgId + " " + brokerId + " " + username + " " + text;
    }

    public static boolean isChatReq(String line) {
        return line != null && line.startsWith("CHAT_REQ");
    }

    public static ChatReqFields parseChatReq(String line) {
        // Split only first 4 tokens; the rest is text
        String withoutPrefix = line.substring("CHAT_REQ".length());
        String[] parts = withoutPrefix.split(" ", 4);

        String localMsgId = parts[0];
        int brokerId = Integer.parseInt(parts[1]);
        String username = parts[2];
        String text = parts.length >= 4 ? parts[3] : "";

        return new ChatReqFields(localMsgId, brokerId, username, text);
    }

    // Sequencer --> Brokers
    public static String chatDeliver(long seq, String brokerId, String username, String text) {
        return "CHAT_DELIVER " + seq + " " + brokerId + " " + username + " " + text;
    }

    public static boolean isChatDeliver(String line) {
        return line != null && line.startsWith("CHAT_DELIVER");
    }

    public static ChatDeliverFields parseChatDeliver(String line) {
        String withoutPrefix = line.substring("CHAT_DELIVER".length());
        String[] parts = withoutPrefix.split(" ", 4);

        long seq = Long.parseLong(parts[0]);
        String brokerId = parts[1];
        String username = parts[2];
        String text = parts.length >= 4 ? parts[3] : "";

        return new ChatDeliverFields(seq, brokerId, username, text);
    }
}*/