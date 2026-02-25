package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public class HierarchicalRateLimitEngine {
    private final HierarchicalConfigurationManager configurationManager;

    public HierarchicalRateLimitEngine(HierarchicalConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        HierarchicalRateLimitConfig config = this.configurationManager.getHierarchicalConfig(key);
        RateLimitResult lastResult = null;

        for (RateLimitLevel level : config.getLevels()) {
            RateLimitConfig levelConfig = level.getConfig();
            RateLimitKey levelKey = level.getKey();

            RateLimitState state = this.configurationManager.getHierarchicalState(levelKey);

            if (state == null) {
                state = levelConfig.initialRateLimitState();
                this.configurationManager.setHierarchicalState(levelKey, state);
            }

            RateLimitingAlgorithm algorithm = levelConfig.getAlgorithm();
            RateLimitResult result = algorithm.tryConsume(levelKey, levelConfig, state, cost);

            // Phantom consumption issue - Limits from earlier levels are still consumed even if later level rejects.
            // Deny request on first reject
            if (!result.isAllowed()) {
                return result;
            }

            lastResult = result;
        }

        return lastResult;
    }
}
