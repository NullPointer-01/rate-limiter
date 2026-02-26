package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowCounterState;

import java.util.Objects;

public class SlidingWindowCounterAlgorithm implements RateLimitingAlgorithm {

    @Override
    public synchronized RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time) {
        return tryConsume(key, config, state, time, 1);
    }

    @Override
    public synchronized RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        return evaluate(key, config, state, time, cost, false);
    }

    @Override
    public RateLimitResult checkLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost) {
        return evaluate(key, config, state, time, cost, true);
    }

    private synchronized RateLimitResult evaluate(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long cost, boolean isReadOnly) {
        Objects.requireNonNull(key, "RateLimitKey cannot be null");
        Objects.requireNonNull(config, "RateLimitConfig cannot be null");

        if (cost <= 0) {
            throw new InvalidRateLimitCostException("Cost must be positive");
        }
        SlidingWindowCounterConfig windowConfig = (SlidingWindowCounterConfig) config;
        SlidingWindowCounterState windowState = (SlidingWindowCounterState) state;

        long nowMillis = time.currentTimeMillis();
        long nowNanos = time.nanoTime();

        long origin = windowState.getOriginNanos();
        long windowSizeNanos = windowConfig.getWindowSizeMillis() * 1_000_000L;

        long elapsedSinceOrigin = nowNanos - origin;
        long currentWindowId = elapsedSinceOrigin / windowSizeNanos;

        RateLimitResult.Builder builder = RateLimitResult.builder();
        long currentWindowUsed = windowState.getWindowCost(currentWindowId);

        long currentWindowStart = origin + (currentWindowId * windowSizeNanos);
        long elapsed = nowNanos - currentWindowStart;

        long capacity = windowConfig.getCapacity();
        long prevWindowUsed = windowState.getWindowCost(currentWindowId - 1);
        double windowProgress = elapsed / (double) windowSizeNanos;
        double weightedCost = currentWindowUsed + prevWindowUsed * (1 - windowProgress);

        if (weightedCost + cost <= capacity) {
            long remainingCost = (long) Math.max(0, capacity - (weightedCost + cost));
            long resetAtNanos = currentWindowStart + windowSizeNanos;
            long retryAfterMillis = Math.max(0L, (resetAtNanos - nowNanos) / 1_000_000L);
            long resetTimeMillis = nowMillis + retryAfterMillis;

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
        long retryAfterNanos;

        // Current window cannot accommodate the request
        if (currentWindowUsed + cost > capacity) {
            double maxAllowedFromOldCurrent = capacity - cost;
            double requiredProgress = 1.0 - (maxAllowedFromOldCurrent / (double) currentWindowUsed);
            requiredProgress = Math.max(0.0, Math.min(1.0, requiredProgress));

            long timeToNextWindow = (currentWindowStart + windowSizeNanos) - nowNanos;
            long timeInNextWindow = (long) Math.ceil(requiredProgress * windowSizeNanos);
            retryAfterNanos = timeToNextWindow + timeInNextWindow;
        } else {
            // Previous window is the bottleneck
            double requiredProgress = 1.0 - (capacity - currentWindowUsed - cost / (double) prevWindowUsed);
            requiredProgress = Math.max(windowProgress, Math.min(1.0, requiredProgress));
            retryAfterNanos = (long) Math.ceil((requiredProgress - windowProgress) * windowSizeNanos);
        }

        long retryAfterMillis = Math.max(0L, retryAfterNanos / 1_000_000L);
        long resetTimeMillis = nowMillis + retryAfterMillis;

        builder.allowed(false)
                .limit(capacity)
                .remaining((long) Math.max(0, capacity - weightedCost))
                .retryAfterMillis(retryAfterMillis)
                .resetAtMillis(resetTimeMillis);

        return builder.build();
    }
}
