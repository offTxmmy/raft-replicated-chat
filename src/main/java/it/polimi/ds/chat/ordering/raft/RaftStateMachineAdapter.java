package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.function.Consumer;

/**
 * Maps a committed {@link RaftLogEntry} into the application-level
 * {@link ChatDeliverMessage} delivered through {@link
 * it.polimi.ds.chat.ordering.api.OrderingService#onDeliver(Consumer)}.
 *
 * <p>The Raft log index is used as the global sequence number on the
 * delivery message ({@link ChatDeliverMessage#getSeq()}). This preserves
 * the total order guaranteed by Raft.
 */
public final class RaftStateMachineAdapter implements Consumer<RaftLogEntry> {

    private final Consumer<ChatDeliverMessage> deliveryCallback;

    public RaftStateMachineAdapter(Consumer<ChatDeliverMessage> deliveryCallback) {
        this.deliveryCallback = deliveryCallback;
    }

    @Override
    public void accept(RaftLogEntry entry) {
        ChatCommand cmd = entry.getCommand();
        if (cmd == null) {
            return; // configuration / no-op entry — nothing to deliver
        }
        ChatDeliverMessage deliver = new ChatDeliverMessage(
                entry.getIndex(),
                cmd.getBrokerId(),
                cmd.getUsername(),
                cmd.getText(),
                cmd.getVectorClock()
        );
        deliveryCallback.accept(deliver);
    }
}