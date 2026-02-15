package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.FixedWindowCounterState;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

public class FixedWindowCounterAlgorithm implements RateLimitingAlgorithm {
    @Override
    public AlgorithmType getAlgorithmType() {
        return AlgorithmType.FIXED_WINDOW_COUNTER;
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return tryConsume(key, config, state, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long cost) {
        FixedWindowCounterConfig windowConfig = (FixedWindowCounterConfig) config;
        FixedWindowCounterState windowState = (FixedWindowCounterState) state;
        long now = System.currentTimeMillis();

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long currentWindowId = now / windowConfig.getWindowSizeMillis();
        long currentWindowUsed = windowState.getWindowCost(currentWindowId);

        long capacity = windowConfig.getCapacity();
        long resetTime = (currentWindowId + 1) * windowConfig.getWindowSizeMillis(); // Start time of the next window

        if (currentWindowUsed + cost <= capacity) {
            long remainingCost = capacity - (currentWindowUsed + cost);

            windowState.addCostToWindow(cost, currentWindowId);
            builder.allowed(true)
                    .limit(capacity)
                    .remaining(remainingCost)
                    .resetAtMillis(resetTime);
            return builder.build();
        }

        builder.allowed(false)
                .limit(capacity)
                .remaining(capacity - currentWindowUsed)
                .retryAfterMillis(resetTime - now)
                .resetAtMillis(resetTime);
        return builder.build();
    }
}
