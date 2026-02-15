package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.state.FixedWindowCounterState;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowCounterState;

public class SlidingWindowCounterAlgorithm implements RateLimitingAlgorithm {
    @Override
    public AlgorithmType getAlgorithmType() {
        return AlgorithmType.SLIDING_WINDOW_COUNTER;
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return tryConsume(key, config, state, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long cost) {
        SlidingWindowCounterConfig windowConfig = (SlidingWindowCounterConfig) config;
        SlidingWindowCounterState windowState = (SlidingWindowCounterState) state;
        long now = System.currentTimeMillis();

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long windowSizeMillis = windowConfig.getWindowSizeMillis();
        long currentWindowId = now / windowSizeMillis;
        long currentWindowUsed = windowState.getWindowCost(currentWindowId);

        long currentWindowStart = currentWindowId * windowSizeMillis;
        long elapsed = now - currentWindowStart;

        long prevWindowUsed = windowState.getWindowCost(currentWindowId - 1);
        double windowProgress = elapsed / (double) windowSizeMillis;
        double weightedCost = currentWindowUsed + prevWindowUsed * (1 - windowProgress);

        long capacity = windowConfig.getCapacity();
        long resetTime = (currentWindowId + 1) * windowSizeMillis; // Start time of the next window

        if (weightedCost + cost <= capacity) {
            long remainingCost = (long) Math.max(0, capacity - (weightedCost + cost)); // Use weighted cost for remaining

            windowState.addCostToWindow(cost, currentWindowId);
            builder.allowed(true)
                    .limit(capacity)
                    .remaining(remainingCost)
                    .resetAtMillis(resetTime);
            return builder.build();
        }

        long remaining = (long) Math.max(0, capacity - weightedCost);

        builder.allowed(false)
                .limit(capacity)
                .remaining(remaining) // Use weighted cost for remaining
                .retryAfterMillis(resetTime - now)
                .resetAtMillis(resetTime);
        return builder.build();
    }
}
