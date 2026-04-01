package org.nullpointer.ratelimiter.core;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.factory.CircuitBreakerFactory;
import org.nullpointer.ratelimiter.instrumentation.RateLimiterMetrics;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.resilience.CircuitBreaker;
import org.nullpointer.ratelimiter.utils.SystemTimeSource;
import org.nullpointer.ratelimiter.utils.TimeSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimitEngine {
    private final ConfigurationManager configurationManager;
    private final TimeSource timeSource;
    private final RateLimiterMetrics metrics;
    private final CircuitBreaker cb;

    private final Map<String, Object> locks;

    public RateLimitEngine(ConfigurationManager configurationManager) {
        this(configurationManager, CircuitBreakerFactory.defaultCircuitBreakerConfig());
    }

    public RateLimitEngine(ConfigurationManager configurationManager, CircuitBreakerConfig cbConfig) {
        this.configurationManager = configurationManager;
        this.timeSource = new SystemTimeSource();
        this.metrics = new RateLimiterMetrics();
        this.cb = new CircuitBreaker(timeSource, cbConfig);
        this.locks = new ConcurrentHashMap<>();
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        return process(key, cost, timeSource.capture());
    }

    public RateLimitResult process(RateLimitKey key, int cost, RequestTime time) {
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
            RateLimitConfig config = this.configurationManager.getConfig(key);
            RateLimitResult result;

            String k = key.toKey();
            Object lock = locks.computeIfAbsent(k, k1 -> new Object());

            synchronized (lock) {
                RateLimitState state = this.configurationManager.getState(key);
                if (state == null) {
                    state = config.initialRateLimitState(time.nanoTime());
                }

                RateLimitingAlgorithm algorithm = config.getAlgorithm();
                result = algorithm.tryConsume(key, config, state, time, cost);
                this.configurationManager.setState(key, state);
            }

            if (result.isAllowed()) {
                metrics.logAllowed();
            } else {
                metrics.logRejected();
            }

            cb.recordSuccess();
            return result;
        } catch (Exception ex) {
            metrics.logError();
            cb.recordError();
            return cb.getFallbackResult();
        }
    }
}
