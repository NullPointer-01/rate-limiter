package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowState;

public class SlidingWindowAlgorithm implements RateLimitingAlgorithm {
    @Override
    public AlgorithmType getAlgorithmType() {
        return AlgorithmType.SLIDING_WINDOW;
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return tryConsume(key, config, state, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long cost) {
        cost = 1; // This implementation assumes all requests to be unit cost

        SlidingWindowConfig windowConfig = (SlidingWindowConfig) config;
        SlidingWindowState windowState = (SlidingWindowState) state;
        long now = System.currentTimeMillis();

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long currentWindowCost = windowState.getCurrentWindowCost(windowConfig.getWindowSizeMillis(), now);
        long maxCost = windowConfig.getMaxCost();
        long resetTime = calculateResetTime(windowConfig, windowState, now);

        if (currentWindowCost + cost <= maxCost) {
            long remainingCost = maxCost - (currentWindowCost + cost);

            windowState.appendRequest(cost, now);
            builder.allowed(true)
                    .limit(maxCost)
                    .remaining(remainingCost)
                    .resetAtMillis(resetTime);
            return builder.build();
        }

        builder.allowed(false)
                .limit(maxCost)
                .remaining(Math.max(0L, maxCost - currentWindowCost))
                .retryAfterMillis(resetTime - now)
                .resetAtMillis(resetTime);
        return builder.build();
    }


    /**
     * The Sliding window resets when the oldest request expires
     */
    private long calculateResetTime(SlidingWindowConfig config, SlidingWindowState state, long now) {
        return state.isWindowEmpty()
                ? now + config.getWindowSizeMillis()
                : state.getOldestTimestamp() + config.getWindowSizeMillis();
    }
}
