package it.polimi.ds.chat.client;

/** Receives client connection lifecycle changes. */
@FunctionalInterface
public interface ClientStatusListener {
    void onStatusChanged(ClientStatus status, String detail);
}