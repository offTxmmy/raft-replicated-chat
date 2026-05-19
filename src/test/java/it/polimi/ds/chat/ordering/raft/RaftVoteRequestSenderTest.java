package it.polimi.ds.chat.ordering.raft;

import it.polimi.ds.chat.protocol.raft.RequestVoteRequestMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftVoteRequestSenderTest {

    @Test
    void defaultBroadcastRequestVoteFallsBackToUnicastForEachPeer() {
        RecordingVoteRequestSender sender = new RecordingVoteRequestSender();
        RequestVoteRequestMessage request = new RequestVoteRequestMessage(4L, 2, 10L, 3L);

        sender.broadcastRequestVote(request, setOf(1, 3, 4));

        assertEquals(3, sender.sentRequests.size());
        assertEquals(List.of(1, 3, 4), sender.destinationPeerIds());
        for (SentVoteRequest sent : sender.sentRequests) {
            assertEquals(request, sent.request());
        }
    }

    private static Set<Integer> setOf(Integer... values) {
        LinkedHashSet<Integer> set = new LinkedHashSet<>();
        for (Integer value : values) {
            set.add(value);
        }
        return set;
    }

    private record SentVoteRequest(int peerId, RequestVoteRequestMessage request) {
    }

    private static final class RecordingVoteRequestSender implements RaftVoteRequestSender {
        private final List<SentVoteRequest> sentRequests = new ArrayList<>();

        @Override
        public void sendRequestVote(int peerId, RequestVoteRequestMessage request) {
            sentRequests.add(new SentVoteRequest(peerId, request));
        }

        private List<Integer> destinationPeerIds() {
            List<Integer> ids = new ArrayList<>();
            for (SentVoteRequest request : sentRequests) {
                ids.add(request.peerId());
            }
            return ids;
        }
    }
}
