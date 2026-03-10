package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStateStore implements StateStore {
    private final Map<String, RateLimitState> stateMap;
    private final Map<String, RateLimitState> hierarchicalStateMap;

    public InMemoryStateStore() {
        this.stateMap = new ConcurrentHashMap<>();
        this.hierarchicalStateMap = new ConcurrentHashMap<>();
    }

    @Override
    public void setState(RateLimitKey key, RateLimitState state) {
        this.stateMap.put(key.toKey(), state);
    }

    @Override
    public RateLimitState getState(RateLimitKey key) {
        return this.stateMap.get(key.toKey());
    }

    @Override
    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        this.hierarchicalStateMap.put(key.toKey(), state);
    }

    @Override
    public RateLimitState getHierarchicalState(RateLimitKey key) {
        return this.hierarchicalStateMap.get(key.toKey());
    }
}
