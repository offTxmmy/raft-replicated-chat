package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.messages.ChatReqMessage;
import it.polimi.ds.chat.utilities.ChatReqFields;
import it.polimi.ds.chat.utilities.Protocol;

import java.io.*;
import java.net.Socket;

public class SequencerHandler implements Runnable {

    private final Socket socket;
    private final SequencerState sequencerState;
    private final ObjectInputStream in;
    private final ObjectOutputStream out;

    public SequencerHandler(Socket socket, SequencerState sequencerState, ObjectInputStream in, ObjectOutputStream out) {
        this.socket = socket;
        this.sequencerState = sequencerState;
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        System.out.println("SequencerHandler started for broker connection: " + socket.getRemoteSocketAddress());

        try {
            while (true) {
                Object obj = in.readObject();
                if (obj == null) {
                    break;
                }
                handleMessage(obj);
            }
        } catch (IOException e) {
            System.out.println("SequencerHandler: connection closed: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("SequencerHandler: received unknown object type: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void handleMessage(Object obj) {
        if (obj instanceof ChatReqMessage msg) {
            int brokerId = msg.getBrokerId();
            String username = msg.getUsername();
            String text = msg.getText();

            // Pass the message to the sequencer logic: it will assign global seq
            sequencerState.handleChatFromBroker(brokerId, username, text);

            // If later you add ACKs, you can send them here using `out.writeObject(...)`
        } else {
            System.out.println("SequencerHandler: unknown message from broker: " + obj);
        }
    }
}
