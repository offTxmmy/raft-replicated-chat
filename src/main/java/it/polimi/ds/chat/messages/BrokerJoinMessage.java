package it.polimi.ds.chat.messages;

public class BrokerJoinMessage extends BrokerMessage {

    private final String host;
    private final int port;
    private final int brokerId;

    public BrokerJoinMessage(String host, int port) {
        this(host, port, -1);
    }

    public BrokerJoinMessage(String host, int port, int brokerId) {
        this.host = host;
        this.port = port;
        this.brokerId = brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getBrokerId() {
        return brokerId;
    }
}
