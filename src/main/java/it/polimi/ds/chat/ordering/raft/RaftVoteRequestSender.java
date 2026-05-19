package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;

/**
 * Minimal outbound RPC contract needed by the election manager.
 * Transport/network details stay outside the election layer.
 * The election manager only says "send this RequestVote to peer X".
 */
public interface RaftVoteRequestSender {

    /**
     * Sends a vote request to a specific peer.
     *
     * @param peerId destination peer id
     * @param request vote request message
     */
    void sendRequestVote(int peerId, RequestVoteRequestMessage request);
}
