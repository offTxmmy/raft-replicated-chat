package it.polimi.ds.chat.broker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.InputMismatchException;
import java.util.Scanner;

public class BrokerMain {

    private static final Scanner scanner = new Scanner(System.in);

    private static HandlerState handlerState;

    public static void main(String[] args) {
        System.out.println("---REPLICATED CHAT INFRASTACTURE---");

        boolean isSequencer = args.length > 0 && "first".equalsIgnoreCase(args[0]);

        String brokerIp = takeBrokerIp();
        int brokerPort = askForBrokerPort();

        int clientPort = brokerPort;
        int sequencerPort = 50001;
        int udpPort = 50002;

        String sequencerHost;
        HandlerState handlerState = null;
        int brokerId;

        if(isSequencer) {
            handlerState = new HandlerState();
            brokerId = handlerState.getNewBrokerId();
            sequencerHost = brokerIp;
            System.out.println("Starting as SEQUENCER with brokerId = " + brokerId);
        } else {
            brokerId = -1;
            sequencerHost = askForSequencerHost();
            System.out.println("Starting as FOLLOWER, waiting for ID from sequencer at " + sequencerHost + ":" + sequencerPort);
        }

        BrokerConfig config = new BrokerConfig(
                brokerId,
                isSequencer,
                brokerIp,
                brokerPort,
                clientPort,
                sequencerHost,
                sequencerPort,
                udpPort,
                handlerState          // only the leader has != null
        );

        Broker broker = new Broker(config);

        try{
            broker.start();
        } catch (IOException e) {
            System.err.println("Broker failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static String takeBrokerIp() {
        try{
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    public static Integer askForBrokerPort() {
        Integer brokerPort = null;
        while (brokerPort == null) {
            System.out.println("Insert a port for the broker to listen on for clients:");

            try {
                int port = scanner.nextInt();

                if(port < 1024 || port > 65535) {
                    System.out.println("Please enter a valid port number between 1024 and 65535.");
                } else {
                    brokerPort = port;
                }
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a numeric port number.");
            }

            scanner.nextLine();
        }
        return brokerPort;
    }

    private static String askForSequencerHost() {
        System.out.println("Insert sequencer host (press ENTER for localhost):");
        String line = scanner.nextLine().trim();
        if (line.isEmpty()) {
            return "localhost";
        } else {
            return line;
        }
    }
}
