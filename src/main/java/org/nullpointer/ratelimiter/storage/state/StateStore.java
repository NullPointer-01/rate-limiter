package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public interface StateStore {
    void setState(RateLimitKey key, RateLimitState state);

    RateLimitState getState(RateLimitKey key);

    void setHierarchicalState(RateLimitKey key, RateLimitState state);

    RateLimitState getHierarchicalState(RateLimitKey key);
}
