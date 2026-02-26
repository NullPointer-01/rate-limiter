package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.SystemTimeSource;
import org.nullpointer.ratelimiter.utils.TimeSource;

public class HierarchicalRateLimitEngine {
    private final HierarchicalConfigurationManager configurationManager;
    private final TimeSource timeSource;

    public HierarchicalRateLimitEngine(HierarchicalConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.timeSource = new SystemTimeSource();
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        HierarchicalRateLimitConfig config = this.configurationManager.getHierarchicalConfig(key);
        RequestTime time = timeSource.capture();

        // Phase 1: Dry-run — check all levels without consuming quota
        for (RateLimitLevel level : config.getLevels()) {
            RateLimitConfig levelConfig = level.getConfig();
            RateLimitKey levelKey = level.getKey();

            RateLimitState state = this.configurationManager.getHierarchicalState(levelKey);
            if (state == null) {
                state = levelConfig.initialRateLimitState(time.nanoTime());
                this.configurationManager.setHierarchicalState(levelKey, state);
            }

            RateLimitingAlgorithm algorithm = levelConfig.getAlgorithm();
            RateLimitResult result = algorithm.checkLimit(levelKey, levelConfig, state, time, cost);

            // If any level denies, return immediately without consuming from any level
            if (!result.isAllowed()) {
                return result;
            }
        }

        // Phase 2: All levels allow — commit consumption at every level
        RateLimitResult lastResult = null;
        for (RateLimitLevel level : config.getLevels()) {
            RateLimitConfig levelConfig = level.getConfig();
            RateLimitKey levelKey = level.getKey();
            RateLimitState state = this.configurationManager.getHierarchicalState(levelKey);

            RateLimitingAlgorithm algorithm = levelConfig.getAlgorithm();
            lastResult = algorithm.tryConsume(levelKey, levelConfig, state, time, cost);
        }

        return lastResult;
    }
}
