package org.nullpointer.ratelimiter.client.hierarchical;

import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalConfigurationManager;
import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalRateLimitEngine;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;

public class HierarchicalRateLimiter {
    private final HierarchicalRateLimitEngine engine;

    public HierarchicalRateLimiter(HierarchicalConfigurationManager configurationManager) {
        this.engine = new HierarchicalRateLimitEngine(configurationManager);
    }

    public RateLimitResult process(RateLimitKey key) {
        return process(key, 1);
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        return engine.process(key, cost);
    }
}
