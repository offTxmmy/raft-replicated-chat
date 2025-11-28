package it.polimi.ds.chat.messages;

public class BrokerJoinMessage extends BrokerMessage {

    private final String host;
    private final int port;

    public BrokerJoinMessage(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
