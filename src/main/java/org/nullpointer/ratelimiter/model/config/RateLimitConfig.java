package org.nullpointer.ratelimiter.model.config;

import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public interface RateLimitConfig {
   RateLimitingAlgorithm getAlgorithm();

   RateLimitState initialRateLimitState();
}
