package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.chat.ChatDeliverMessage;
import it.polimi.ds.chat.protocol.raft.ChatCommand;
import it.polimi.ds.chat.protocol.raft.RaftLogEntry;

import java.util.function.Consumer;

/**
 * Maps committed Raft log entries into application-level
 * {@link ChatDeliverMessage} instances.
 *
 * <p>
 * Raft log indexes and chat sequence numbers are intentionally kept
 * separate. Internal Raft entries, such as leader no-op entries, occupy
 * log indexes but do not represent client-visible chat messages.
 *
 * <p>
 * Only committed chat commands consume an application sequence number.
 * Since all brokers apply the same committed Raft log in the same order,
 * they deterministically assign the same dense chat sequence.
 */
public final class RaftStateMachineAdapter implements Consumer<RaftLogEntry> {

    private final Consumer<ChatDeliverMessage> deliveryCallback;

    private long applicationSequence = 0L;

    public RaftStateMachineAdapter(Consumer<ChatDeliverMessage> deliveryCallback) {
        this.deliveryCallback = deliveryCallback;
    }

    @Override
    public void accept(RaftLogEntry entry) {
        ChatCommand cmd = entry.getCommand();

        if (cmd == null) {
            // Internal Raft entry (for example a leader no-op):
            // it occupies a Raft log index but it is not a chat message,
            // therefore it must not consume an application sequence number.
            return;
        }

        long seq = ++applicationSequence;

        ChatDeliverMessage deliver = new ChatDeliverMessage(
                seq,
                cmd.getBrokerId(),
                cmd.getUsername(),
                cmd.getClientId(),
                cmd.getText(),
                cmd.getVectorClock());

        deliveryCallback.accept(deliver);
    }
}
