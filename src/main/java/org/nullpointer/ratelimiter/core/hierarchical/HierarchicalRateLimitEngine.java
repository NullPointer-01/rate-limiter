package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.factory.CircuitBreakerFactory;
import org.nullpointer.ratelimiter.instrumentation.RateLimiterMetrics;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.resilience.CircuitBreaker;
import org.nullpointer.ratelimiter.utils.RateLimitKeyGenerator;
import org.nullpointer.ratelimiter.utils.SystemTimeSource;
import org.nullpointer.ratelimiter.utils.TimeSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HierarchicalRateLimitEngine {
    private final HierarchicalConfigurationManager configurationManager;
    private final TimeSource timeSource;
    private final RateLimitKeyGenerator keyGenerator;
    private final RateLimiterMetrics metrics;
    private final CircuitBreaker cb;

    private final Map<String, Object> locks;

    public HierarchicalRateLimitEngine(HierarchicalConfigurationManager configurationManager) {
        this(configurationManager, CircuitBreakerFactory.defaultCircuitBreakerConfig());
    }

    public HierarchicalRateLimitEngine(HierarchicalConfigurationManager configurationManager, CircuitBreakerConfig cbConfig) {
        this.configurationManager = configurationManager;
        this.timeSource = new SystemTimeSource();
        this.keyGenerator = new RateLimitKeyGenerator();
        this.metrics = new RateLimiterMetrics();
        this.cb = new CircuitBreaker(timeSource, cbConfig);
        this.locks = new ConcurrentHashMap<>();
    }

    public RateLimitResult process(RequestContext context, int cost) {
        return process(context, cost, timeSource.capture());
    }

    public RateLimitResult process(RequestContext context, int cost, RequestTime time) {
        if (!cb.allowExecution()) {
            RateLimitResult fallbackResult = cb.getFallbackResult();
            if (fallbackResult.isAllowed()) {
                metrics.logAllowed();
            } else {
                metrics.logRejected();
            }
            return fallbackResult;
        }

        try {
            RateLimitResult lastResult = null;

            HierarchicalRateLimitPolicy hierarchyPolicy = configurationManager.getHierarchyPolicy();

            // Dry-run check for all levels
            for (RateLimitLevel level : hierarchyPolicy.getLevels()) {
                RateLimitScope scope = level.getScope();

                // Retrieve config from store, else pick the default configuration from the level
                RateLimitConfig config = configurationManager.resolveScopedConfig(scope, context);
                if (config == null) config = level.getDefaultConfig();

                RateLimitKey key = keyGenerator.generate(scope, context);
                RateLimitState state = configurationManager.getHierarchicalState(key);

                if (state == null) continue; // Skip since it is the initial request for the key

                RateLimitingAlgorithm algorithm = config.getAlgorithm();
                RateLimitResult result = algorithm.checkLimit(key, config, state, time, cost);

                // If any level denies, return immediately without consuming from previous levels
                if (!result.isAllowed()) {
                    metrics.logRejected();
                    cb.recordSuccess();
                    return result;
                }
            }

            // Consume quotas for all levels
            for (RateLimitLevel level : hierarchyPolicy.getLevels()) {
                RateLimitScope scope = level.getScope();

                RateLimitConfig config = configurationManager.resolveScopedConfig(scope, context);
                if (config == null) config = level.getDefaultConfig();

                RateLimitKey key = keyGenerator.generate(scope, context);
                String k = key.toKey();
                Object lock = locks.computeIfAbsent(k, k1 -> new Object());

                synchronized (lock) {
                    RateLimitState state = this.configurationManager.getHierarchicalState(key);
                    if (state == null) { // Create state for first time
                        state = config.initialRateLimitState(time.nanoTime());
                    }

                    RateLimitingAlgorithm algorithm = config.getAlgorithm();
                    lastResult = algorithm.tryConsume(key, config, state, time, cost);
                    this.configurationManager.setHierarchicalState(key, state);

                    // TOCTOU race condition - Between the time of dry run and consume loop, states might have changed
                    if (!lastResult.isAllowed()) {
                        metrics.logRejected();
                        cb.recordSuccess();
                        return lastResult;
                    }
                }
            }
            metrics.logAllowed();
            cb.recordSuccess();

            return lastResult; // Should never be null
        } catch (RateLimitConfigNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            metrics.logError();
            cb.recordError();
            return cb.getFallbackResult();
        }
    }
}
