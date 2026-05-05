package it.polimi.ds.chat.ordering.raft;

/**
 * Small read-only contract needed by the consensus core
 * to evaluate RequestVote requests.
 */
public interface RaftLogMetadata {

    long lastLogIndex();

    long lastLogTerm();
}
