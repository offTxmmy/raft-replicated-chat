package it.polimi.ds.chat.client;

/** Observable lifecycle states for the client connection. */
public enum ClientStatus {
    CONNECTING,
    RECONNECTING,
    NO_BROKER_AVAILABLE,
    CONNECTED,
    STOPPED
}