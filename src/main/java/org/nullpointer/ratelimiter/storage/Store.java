package org.nullpointer.ratelimiter.storage;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public interface Store {
    void setDefaultConfig(RateLimitConfig config);

    RateLimitConfig getDefaultConfig();

    void setConfig(RateLimitKey key, RateLimitConfig config);

    RateLimitConfig getConfig(RateLimitKey key);

    RateLimitConfig getOrDefaultConfig(RateLimitKey key);

    void setState(RateLimitKey key, RateLimitState state);

    RateLimitState getState(RateLimitKey key);
}
