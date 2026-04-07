package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowState;

import java.util.Objects;

public class SlidingWindowAlgorithm implements RateLimitingAlgorithm {

    private record Snapshot(boolean allowed, RateLimitResult result) {}

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time) {
        return tryConsume(key, config, state, time, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        Snapshot snapshot = computeLimit(key, config, state, time, cost);
        if (snapshot.allowed()) {
            SlidingWindowState windowState = (SlidingWindowState) state;
            windowState.appendRequest(cost, time.nanoTime());
        }
        return snapshot.result();
    }

    @Override
    public RateLimitResult checkLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        return computeLimit(key, config, state, time, cost).result();
    }

    private Snapshot computeLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        Objects.requireNonNull(key, "RateLimitKey cannot be null");
        Objects.requireNonNull(config, "RateLimitConfig cannot be null");

        if (cost <= 0) {
            throw new InvalidRateLimitCostException("Cost must be positive");
        }
        SlidingWindowConfig windowConfig = (SlidingWindowConfig) config;
        SlidingWindowState windowState = (SlidingWindowState) state;
        long nowNanos = time.nanoTime();
        long nowMillis = time.currentTimeMillis();

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long windowSizeNanos = windowConfig.getWindowSizeMillis() * 1_000_000L;
        long currentWindowCost = windowState.getCurrentWindowCost(windowSizeNanos, nowNanos);
        long maxCost = windowConfig.getMaxCost();

        long needed = (currentWindowCost + cost) - maxCost;
        long resetTimeMillis;

        if (needed > 0) {
            long availableAtNanos = windowState.getTimestampWhenCapacityFreed(windowSizeNanos, nowNanos, needed);
            long deltaMillis = Math.max(0L, (availableAtNanos - nowNanos) / 1_000_000L);
            resetTimeMillis = nowMillis + deltaMillis;
        } else {
            resetTimeMillis = calculateResetTimeMillis(windowConfig, windowState, nowNanos, nowMillis);
        }

        if (needed <= 0) {
            long remainingCost = maxCost - (currentWindowCost + cost);

            builder.allowed(true)
                    .limit(maxCost)
                    .remaining(remainingCost)
                    .resetAtMillis(resetTimeMillis)
                    .retryAfterMillis(0);
            return new Snapshot(true, builder.build());
        }

        builder.allowed(false)
                .limit(maxCost)
                .remaining(Math.max(0L, maxCost - currentWindowCost))
                .retryAfterMillis(Math.max(0L, resetTimeMillis - nowMillis))
                .resetAtMillis(resetTimeMillis);
        return new Snapshot(false, builder.build());
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
