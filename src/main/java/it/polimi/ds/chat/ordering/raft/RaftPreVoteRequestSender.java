package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.PreVoteRequestMessage;

import java.util.Set;

/** Outbound preliminary election requests, separate from term-changing RequestVote RPCs. */
public interface RaftPreVoteRequestSender {

    void sendPreVote(int peerId, PreVoteRequestMessage request);

    /** TCP fans out; LAN transports override this with one broadcast request. */
    default void broadcastPreVote(PreVoteRequestMessage request, Set<Integer> peerIds) {
        for (Integer peerId : peerIds) {
            sendPreVote(peerId, request);
        }
    }
}
