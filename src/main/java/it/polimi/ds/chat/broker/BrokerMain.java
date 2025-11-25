package it.polimi.ds.chat.broker;

import java.io.IOException;

public class BrokerMain {

    public static void main(String[] args) {
        String brokerid = "broker1";
        boolean isSequencer = true;
        int clientPort = 50000;
        String sequencerHost = "localhost";
        int sequencerPort = 50001;
        int udpPort = 50002;

        BrokerConfig config = new BrokerConfig(brokerid, isSequencer, clientPort, sequencerHost, sequencerPort, udpPort);
        BrokerHandler broker = new BrokerHandler(config);

        try{
            broker.start();
        } catch (IOException e) {
            System.err.println("Broker failed: " + e.getMessage());
        }
    }
}
