package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.function.Consumer;

/** Maps committed chat entries to deliveries using their Raft log index. */
public final class RaftStateMachineAdapter implements Consumer<RaftLogEntry> {

    private final Consumer<ChatDeliverMessage> deliveryCallback;

    public RaftStateMachineAdapter(Consumer<ChatDeliverMessage> deliveryCallback) {
        this.deliveryCallback = deliveryCallback;
    }

    @Override
    public void accept(RaftLogEntry entry) {
        ChatCommand command = entry.getCommand();
        if (command == null) {
            // Internal Raft no-op: it establishes commitment but has no chat payload.
            return;
        }
        deliveryCallback.accept(new ChatDeliverMessage(
                entry.getIndex(),
                command.getUsername(),
                command.getClientId(),
                command.getClientSeq(),
                command.getText()));
    }
}
