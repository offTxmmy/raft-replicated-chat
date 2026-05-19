package it.polimi.ds.chat.protocol.raft;

/**
 * Identifies the Raft RPC payload carried by a UDP envelope.
 */
public enum RaftUdpMessageType {

    REQUEST_VOTE_REQUEST,

    REQUEST_VOTE_RESPONSE,

    APPEND_ENTRIES_REQUEST,

    APPEND_ENTRIES_RESPONSE
}
