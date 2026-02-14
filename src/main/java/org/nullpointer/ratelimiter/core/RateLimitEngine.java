package org.nullpointer.ratelimiter.core;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public class RateLimitEngine {
    private final ConfigurationManager configurationManager;

    public RateLimitEngine(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public RateLimitResult process(RateLimitKey key, int cost) {
        RateLimitConfig config = this.configurationManager.getConfig(key);
        RateLimitState state = this.configurationManager.getState(key);
        if (state == null) {
            state = config.initialRateLimitState();
            this.configurationManager.setState(key, state);
        }
        RateLimitingAlgorithm algorithm = config.getAlgorithm();

        return algorithm.tryConsume(key, config, state, cost);
    }
}
