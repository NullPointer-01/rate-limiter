package org.nullpointer.ratelimiter.model.config.hierarchical;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;

public class RateLimitLevel {
    int level;
    RateLimitConfig config;
    RateLimitKey key;

    public RateLimitLevel(int level, RateLimitConfig config, RateLimitKey key) {
        this.level = level;
        this.config = config;
        this.key = key;
    }

    public int getLevel() {
        return level;
    }

    public RateLimitConfig getConfig() {
        return config;
    }

    public RateLimitKey getKey() {
        return key;
    }
}
