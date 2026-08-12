package it.polimi.ds.chat.ordering.raft;

/**
 * Callback for higher Raft term observations.
 *
 * <p>Used by components outside the election layer to report evidence of a
 * term higher than the local current term without directly managing election
 * lifecycle state.
 */
public interface RaftHigherTermObserver {

    /**
     * Invoked when a Raft message reveals a term higher than the local one.
     *
     * @param term observed higher Raft term
     */
    void onHigherTermObserved(long term);
}