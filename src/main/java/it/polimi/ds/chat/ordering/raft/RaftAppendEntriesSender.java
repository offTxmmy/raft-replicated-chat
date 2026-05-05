package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.messages.raft.AppendEntriesRequestMessage;

/**
 * Outbound RPC contract for AppendEntries.
 * Transport details stay outside the replication layer.
 */
public interface RaftAppendEntriesSender {

    /**
     * Sends an AppendEntries request to a specific peer.
     *
     * @param peerId destination peer id
     * @param request append entries request
     */
    void sendAppendEntries(int peerId, AppendEntriesRequestMessage request);
}
