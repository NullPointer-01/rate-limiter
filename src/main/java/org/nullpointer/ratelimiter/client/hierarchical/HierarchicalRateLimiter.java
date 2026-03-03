package org.nullpointer.ratelimiter.client.hierarchical;

import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalConfigurationManager;
import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalRateLimitEngine;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;

public class HierarchicalRateLimiter {
    private final HierarchicalRateLimitEngine engine;

    public HierarchicalRateLimiter(HierarchicalConfigurationManager configurationManager) {
        this.engine = new HierarchicalRateLimitEngine(configurationManager);
    }

    public RateLimitResult process(RequestContext context) {
        return process(context, 1);
    }

    public RateLimitResult process(RequestContext context, int cost) {
        return engine.process(context, cost);
    }
}
