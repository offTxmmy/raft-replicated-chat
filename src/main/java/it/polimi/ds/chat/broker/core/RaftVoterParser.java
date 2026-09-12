package it.polimi.ds.chat.broker.core;

import it.polimi.ds.chat.ordering.raft.config.RaftPeerEndpoint;

import java.util.Map;
import java.util.TreeMap;

/** Parses the static Raft membership supplied on every broker command line. */
final class RaftVoterParser {

    private RaftVoterParser() {
    }

    static Map<Integer, RaftPeerEndpoint> parse(String csv) {
        if (csv == null || csv.isBlank()) {
            throw new IllegalArgumentException("votersCSV must not be blank");
        }

        Map<Integer, RaftPeerEndpoint> voters = new TreeMap<>();
        for (String token : csv.split(",", -1)) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                throw badToken(token);
            }
            int at = trimmed.indexOf('@');
            if (at <= 0 || at != trimmed.lastIndexOf('@')) {
                throw badToken(trimmed);
            }

            String idText = trimmed.substring(0, at);
            String[] endpoint = trimmed.substring(at + 1).split(":", -1);
            if (endpoint.length != 2 && endpoint.length != 3
                    || endpoint[0].isBlank() || endpoint[1].isBlank()
                    || (endpoint.length == 3 && endpoint[2].isBlank())) {
                throw badToken(trimmed);
            }

            try {
                int id = Integer.parseInt(idText);
                int rpcPort = Integer.parseInt(endpoint[1]);
                int clientPort = endpoint.length == 3
                        ? Integer.parseInt(endpoint[2])
                        : 50_000 + id;
                RaftPeerEndpoint peer = new RaftPeerEndpoint(id, endpoint[0], rpcPort, clientPort);
                if (voters.putIfAbsent(id, peer) != null) {
                    throw new IllegalArgumentException("Duplicate voter id: " + id);
                }
            } catch (NumberFormatException e) {
                throw badToken(trimmed, e);
            }
        }
        return Map.copyOf(voters);
    }

    private static IllegalArgumentException badToken(String token) {
        return badToken(token, null);
    }

    private static IllegalArgumentException badToken(String token, Exception cause) {
        String message = "Bad voter token: '" + token
                + "' (expected id@host:rpcPort[:clientPort])";
        return cause == null ? new IllegalArgumentException(message)
                : new IllegalArgumentException(message, cause);
    }
}
