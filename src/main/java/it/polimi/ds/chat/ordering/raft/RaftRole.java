package it.polimi.ds.chat.ordering.raft;

/**
 * Role of a Raft node.
 */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
