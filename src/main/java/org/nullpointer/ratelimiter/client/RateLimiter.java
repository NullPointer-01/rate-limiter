package org.nullpointer.ratelimiter.client;

import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.core.RateLimitEngine;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;

public class RateLimiter {
    private final RateLimitEngine engine;

    public RateLimiter(ConfigurationManager configurationManager) {
        this.engine = new RateLimitEngine(configurationManager);
    }

    public RateLimitResult process(RateLimitKey key) {
        return process(key, 1);
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        return engine.process(key, cost);
    }
}
