package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public class SlidingWindowAlgorithm implements RateLimitingAlgorithm {
    @Override
    public AlgorithmType getAlgorithmType() {
        return AlgorithmType.SLIDING_WINDOW;
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return null;
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long cost) {
        return null;
    }
}
