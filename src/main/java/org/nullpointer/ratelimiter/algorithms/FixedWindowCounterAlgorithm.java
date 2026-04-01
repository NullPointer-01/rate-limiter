package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.FixedWindowCounterState;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.Objects;

public class FixedWindowCounterAlgorithm implements RateLimitingAlgorithm {

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time) {
        return tryConsume(key, config, state, time, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        return evaluate(key, config, state, time, cost, false);
    }

    @Override
    public RateLimitResult checkLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        return evaluate(key, config, state, time, cost,true);
    }

    private RateLimitResult evaluate(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost, boolean isReadOnly) {
        Objects.requireNonNull(key, "RateLimitKey cannot be null");
        Objects.requireNonNull(config, "RateLimitConfig cannot be null");

        if (cost <= 0) {
            throw new InvalidRateLimitCostException("Cost must be positive");
        }
        FixedWindowCounterConfig windowConfig = (FixedWindowCounterConfig) config;
        FixedWindowCounterState windowState = (FixedWindowCounterState) state;
        long nowNanos = time.nanoTime();
        long nowMillis = time.currentTimeMillis();

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long windowSizeNanos = windowConfig.getWindowSizeMillis() * 1_000_000L;
        long currentWindowId = nowNanos / windowSizeNanos;
        long currentWindowUsed = windowState.getWindowCost(currentWindowId);

        long capacity = windowConfig.getCapacity();
        long resetAtNanos = (currentWindowId + 1) * windowSizeNanos; // Start time of the next window
        long retryAfterMillis = Math.max(0L, (resetAtNanos - nowNanos) / 1_000_000L);
        long resetTimeMillis = nowMillis + retryAfterMillis;

        if (currentWindowUsed + cost <= capacity) {
            long remainingCost = capacity - (currentWindowUsed + cost);

            if (!isReadOnly) {
                windowState.addCostToWindow(cost, currentWindowId);
            }

            builder.allowed(true)
                    .limit(capacity)
                    .remaining(remainingCost)
                    .resetAtMillis(resetTimeMillis)
                    .retryAfterMillis(0);
            return builder.build();
        }

        builder.allowed(false)
                .limit(capacity)
                .remaining(Math.max(0L, capacity - currentWindowUsed))
                .retryAfterMillis(retryAfterMillis)
                .resetAtMillis(resetTimeMillis);
        return builder.build();
    }
}
