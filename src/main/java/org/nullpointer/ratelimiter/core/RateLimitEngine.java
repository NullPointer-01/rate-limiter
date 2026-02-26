package org.nullpointer.ratelimiter.core;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.SystemTimeSource;
import org.nullpointer.ratelimiter.utils.TimeSource;

public class RateLimitEngine {
    private final ConfigurationManager configurationManager;
    private final TimeSource timeSource;

    public RateLimitEngine(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.timeSource = new SystemTimeSource();
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        RateLimitConfig config = this.configurationManager.getConfig(key);
        RequestTime time = timeSource.capture();

        RateLimitState state = this.configurationManager.getState(key);
        if (state == null) {
            state = config.initialRateLimitState(time.nanoTime());
            this.configurationManager.setState(key, state);
        }
        RateLimitingAlgorithm algorithm = config.getAlgorithm();

        return algorithm.tryConsume(key, config, state, time, cost);
    }
}
