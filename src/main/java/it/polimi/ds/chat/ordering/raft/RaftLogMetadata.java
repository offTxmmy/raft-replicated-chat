package it.polimi.ds.chat.ordering.raft;

/**
 * Small read-only contract needed by the consensus core
 * to evaluate RequestVote requests.
 * PERSON B INTEGRATION:
 * Person B's RaftLog should implement this interface.
 * In particular, Person B's class must provide:
 * - getLastLogIndex()
 * - getLastLogTerm()
 */
public interface RaftLogMetadata {

    long lastLogIndex();

    long lastLogTerm();
}
