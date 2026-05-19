package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;

import java.util.Set;

/**
 * Minimal outbound RPC contract needed by the election manager.
 * Transport/network details stay outside the election layer.
 * The election manager says either "send this RequestVote to peer X"
 * or "broadcast this RequestVote to the static peer set".
 */
public interface RaftVoteRequestSender {

    /**
     * Sends a vote request to a specific peer.
     *
     * @param peerId destination peer id
     * @param request vote request message
     */
    void sendRequestVote(int peerId, RequestVoteRequestMessage request);

    /**
     * Sends a vote request to all peer voters.
     *
     * <p>The default implementation preserves the original TCP/unicast behavior.
     * LAN broadcast transports should override this method to send a single
     * broadcast packet instead of one packet per peer.
     *
     * @param request vote request message
     * @param peerIds static peer voter ids, excluding the local node
     */
    default void broadcastRequestVote(RequestVoteRequestMessage request, Set<Integer> peerIds) {
        for (Integer peerId : peerIds) {
            sendRequestVote(peerId, request);
        }
    }
}
