package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.instrumentation.RateLimiterMetrics;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.RateLimitKeyGenerator;
import org.nullpointer.ratelimiter.utils.SystemTimeSource;
import org.nullpointer.ratelimiter.utils.TimeSource;

public class HierarchicalRateLimitEngine {
    private final HierarchicalConfigurationManager configurationManager;
    private final TimeSource timeSource;
    private final RateLimitKeyGenerator keyGenerator;
    private final RateLimiterMetrics metrics;

    public HierarchicalRateLimitEngine(HierarchicalConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.timeSource = new SystemTimeSource();
        this.keyGenerator = new RateLimitKeyGenerator();
        this.metrics = new RateLimiterMetrics();
    }

    public RateLimitResult process(RequestContext context, int cost) {
        RequestTime time = timeSource.capture();
        RateLimitResult lastResult = null;

        HierarchicalRateLimitConfig hierarchyPolicy = configurationManager.getHierarchyPolicy();

        // Dry-run check for all levels
        for (RateLimitLevel level : hierarchyPolicy.getLevels()) {
            RateLimitScope scope = level.getScope();

            // Retrieve config from store, else pick the default configuration from the level
            RateLimitConfig config = configurationManager.resolveConfig(scope, context);
            if (config == null) config = level.getDefaultConfig();

            RateLimitKey key = keyGenerator.generate(scope, context);
            RateLimitState state = this.configurationManager.getHierarchicalState(key);

            if (state == null) { // Create state for first time
                state = config.initialRateLimitState(time.nanoTime());
                this.configurationManager.setHierarchicalState(key, state);
            }

            RateLimitingAlgorithm algorithm = config.getAlgorithm();
            RateLimitResult result = algorithm.checkLimit(key, config, state, time, cost);

            // If any level denies, return immediately without consuming from previous levels
            if (!result.isAllowed()) {
                metrics.logRejected();
                return result;
            }
        }

        // Consume quotas for all levels
        for (RateLimitLevel level : hierarchyPolicy.getLevels()) {
            RateLimitScope scope = level.getScope();

            RateLimitConfig config = configurationManager.resolveConfig(scope, context);
            if (config == null) config = level.getDefaultConfig();

            RateLimitKey key = keyGenerator.generate(scope, context);
            RateLimitState state = this.configurationManager.getHierarchicalState(key);

            RateLimitingAlgorithm algorithm = config.getAlgorithm();
            lastResult = algorithm.tryConsume(key, config, state, time, cost);
        }
        metrics.logAllowed();

        return lastResult; // Should never be null
    }
}
