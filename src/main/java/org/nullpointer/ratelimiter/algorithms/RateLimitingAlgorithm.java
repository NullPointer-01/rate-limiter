package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public interface RateLimitingAlgorithm {

    RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state);

    RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long cost);
}
