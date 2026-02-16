package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowState;

public class SlidingWindowAlgorithm implements RateLimitingAlgorithm {

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return tryConsume(key, config, state, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long cost) {
        cost = 1; // This implementation assumes all requests to be unit cost

        SlidingWindowConfig windowConfig = (SlidingWindowConfig) config;
        SlidingWindowState windowState = (SlidingWindowState) state;
        long nowNanos = System.nanoTime();
        long nowMillis = System.currentTimeMillis();

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long windowSizeNanos = windowConfig.getWindowSizeMillis() * 1_000_000L;
        long currentWindowCost = windowState.getCurrentWindowCost(windowSizeNanos, nowNanos);
        long maxCost = windowConfig.getMaxCost();
        long resetTimeMillis = calculateResetTimeMillis(windowConfig, windowState, nowNanos, nowMillis);

        if (currentWindowCost + cost <= maxCost) {
            long remainingCost = maxCost - (currentWindowCost + cost);

            windowState.appendRequest(cost, nowNanos);
            builder.allowed(true)
                    .limit(maxCost)
                    .remaining(remainingCost)
                    .resetAtMillis(resetTimeMillis)
                    .retryAfterMillis(0);
            return builder.build();
        }

        builder.allowed(false)
                .limit(maxCost)
                .remaining(Math.max(0L, maxCost - currentWindowCost))
                .retryAfterMillis(Math.max(0L, resetTimeMillis - nowMillis))
                .resetAtMillis(resetTimeMillis);
        return builder.build();
    }


    /**
     * The Sliding window resets when the oldest request expires
     */
    private long calculateResetTimeMillis(SlidingWindowConfig config, SlidingWindowState state, long nowNanos, long nowMillis) {
        long windowSizeNanos = config.getWindowSizeMillis() * 1_000_000L;
        long resetAtNanos = state.isWindowEmpty()
                ? nowNanos + windowSizeNanos
                : state.getOldestTimestampNanos() + windowSizeNanos;
        long deltaMillis = Math.max(0L, (resetAtNanos - nowNanos) / 1_000_000L);
        return nowMillis + deltaMillis;
    }
}
