package it.polimi.ds.chat.protocol.directory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class GetBrokerRequestMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Set<Integer> excludedBrokerIds;

    public GetBrokerRequestMessage() {
        this(Collections.emptySet());
    }

    public GetBrokerRequestMessage(Set<Integer> excludedBrokerIds) {
        this.excludedBrokerIds = excludedBrokerIds == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new HashSet<>(excludedBrokerIds));
    }

    public Set<Integer> getExcludedBrokerIds() {
        // A stream produced by a pre-exclusion client has no value for this field.
        return excludedBrokerIds == null ? Collections.emptySet() : excludedBrokerIds;
    }

    @Override
    public String toString() {
        return "GetBrokerRequestMessage{excludedBrokerIds="
                + getExcludedBrokerIds() + '}';
    }
}
