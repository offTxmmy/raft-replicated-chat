package it.polimi.ds.chat.messages;

import java.io.Serializable;

/**
 * Response sent by a Raft follower when a client tries to submit a message
 * to a broker that is not the current leader.
 *
 * The client must keep the original message pending and reconnect to the
 * suggested leader endpoint when available.
 */
public class NotLeaderResponseMessage implements Serializable {

    private final long timestamp;
    private final int leaderId;
    private final String leaderHost;
    private final int leaderPort;

    public NotLeaderResponseMessage(long timestamp, int leaderId, String leaderHost, int leaderPort) {
        this.timestamp = timestamp;
        this.leaderId = leaderId;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public String getLeaderHost() {
        return leaderHost;
    }

    public int getLeaderPort() {
        return leaderPort;
    }

    public boolean hasLeaderEndpoint() {
        return leaderId >= 0 && leaderHost != null && !leaderHost.isBlank() && leaderPort > 0;
    }

    @Override
    public String toString() {
        return "NotLeaderResponseMessage{" +
                "timestamp=" + timestamp +
                ", leaderId=" + leaderId +
                ", leaderHost='" + leaderHost + '\'' +
                ", leaderPort=" + leaderPort +
                '}';
    }
}