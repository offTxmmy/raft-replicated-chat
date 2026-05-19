package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.AppendEntriesRequestMessage;

import java.util.Set;

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

    /**
     * Sends the same AppendEntries request to a group of peers.
     *
     * <p>The default implementation preserves the original TCP/unicast behavior.
     * LAN broadcast transports may override this method when the request is safe
     * to send as one shared broadcast packet, such as an empty heartbeat.
     *
     * @param request append entries request
     * @param peerIds destination peer ids
     */
    default void broadcastAppendEntries(AppendEntriesRequestMessage request, Set<Integer> peerIds) {
        for (Integer peerId : peerIds) {
            sendAppendEntries(peerId, request);
        }
    }
}