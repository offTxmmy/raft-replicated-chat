package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesResponseMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;
import it.polimi.ds.chat.protocol.raft.PreVoteResponseMessage;
import it.polimi.ds.chat.protocol.raft.RequestVoteResponseMessage;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Common outbound transport contract for Raft RPCs.
 *
 * <p>The Raft core depends on the narrower sender interfaces
 * ({@link RaftVoteRequestSender} and {@link RaftAppendEntriesSender}).
 * This interface is used by the integration layer to manage transport
 * lifecycle and response dispatch without committing to TCP or UDP.
 */
public interface RaftTransport extends RaftVoteRequestSender, RaftPreVoteRequestSender, RaftAppendEntriesSender {

    /** Installs preliminary election request and response handlers before startup. */
    default void attachPreVoteHandlers(
            Function<PreVoteRequestMessage, PreVoteResponseMessage> requestHandler,
            Consumer<PreVoteResponseMessage> responseHandler) {
        throw new UnsupportedOperationException("Transport does not support PreVote handlers");
    }

    @Override
    default void sendPreVote(int peerId, PreVoteRequestMessage request) {
        throw new UnsupportedOperationException("Transport does not support PreVote requests");
    }

    /**
     * Installs handlers for asynchronous RPC responses.
     *
     * <p>Implementations must call {@code voteResponseHandler} when a
     * {@link RequestVoteResponseMessage} is received, and
     * {@code appendResponseHandler} when an {@link AppendEntriesResponseMessage}
     * is received. The integer passed to {@code appendResponseHandler} is the
     * peer id associated with the response.
     */
    void attachHandlers(
            Consumer<RequestVoteResponseMessage> voteResponseHandler,
            BiConsumer<Integer, AppendEntriesResponseMessage> appendResponseHandler
    );

    /**
     * Starts the transport.
     */
    void start();

    /**
     * Stops the transport and releases its resources.
     */
    void stop();
}
