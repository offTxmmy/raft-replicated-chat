package it.polimi.ds.chat.broker;

import it.polimi.ds.chat.utilities.ChatReqFields;
import it.polimi.ds.chat.utilities.Protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class SequencerHandler implements Runnable {

    private final Socket socket;
    private final SequencerState sequencerState;

    public SequencerHandler(Socket socket, SequencerState sequencerState) {
        this.socket = socket;
        this.sequencerState = sequencerState;
    }

    @Override
    public void run() {
        System.out.println("SequencerHandler started for broker connection: " + socket.getRemoteSocketAddress());

        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String line;
            while ((line = in.readLine()) != null) {
                handleLine(line);
            }

        } catch (IOException e) {
            System.out.println("SequencerHandler connection closed: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void handleLine(String line) {
        if (Protocol.isChatReq(line)) {
            ChatReqFields fields = Protocol.parseChatReq(line);
            // localMsgId = fields.localMsgId (we ignore it for now)
            String brokerId = fields.brokerId;
            String username = fields.username;
            String text = fields.text;

            // Pass the message to the sequencer logic: it will assign seq
            sequencerState.handleChatFromBroker(brokerId, username, text);

        } else {
            System.out.println("SequencerHandler: unknown line from broker: " + line);
        }
    }
}
