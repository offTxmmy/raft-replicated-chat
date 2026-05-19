package it.polimi.ds.chat.broker.config;

/**
 * Selects which {@link it.polimi.ds.chat.ordering.api.OrderingService} implementation
 * the broker should instantiate at startup.
 *
 * <ul>
 *   <li>{@link #SEQUENCER} — legacy single-sequencer ordering (current default).
 *   <li>{@link #RAFT} — Raft-based replicated ordering with static voter set.
 * </ul>
 */
public enum OrderingMode {
    SEQUENCER,
    RAFT
}