package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;

import java.util.Objects;

public class TokenBucketAlgorithm implements RateLimitingAlgorithm {

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time) {
        return tryConsume(key, config, state, time, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long tokens) {
        return evaluate(key, config, state, time, tokens, false);
    }

    @Override
    public RateLimitResult checkLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long tokens) {
        return evaluate(key, config, state, time, tokens, true);
    }

    private RateLimitResult evaluate(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long tokens, boolean isReadOnly) {
        Objects.requireNonNull(key, "RateLimitKey cannot be null");
        Objects.requireNonNull(config, "RateLimitConfig cannot be null");

        if (tokens <= 0) {
            throw new InvalidRateLimitCostException("Cost must be positive");
        }
        TokenBucketConfig bucketConfig = (TokenBucketConfig) config;
        TokenBucketState bucketState = (TokenBucketState) state;

        long nowNanos = time.nanoTime();
        long nowMillis = time.currentTimeMillis();

        double availableTokens;
        if (isReadOnly) {
            availableTokens = computeRefilledTokens(bucketConfig, bucketState, nowNanos);
        } else {
            refill(bucketConfig, bucketState, nowNanos);
            availableTokens = bucketState.getAvailableTokens();
        }

        RateLimitResult.Builder builder = RateLimitResult.builder();

        if (availableTokens >= tokens) {
            double remainingTokens = availableTokens - tokens;

            if (!isReadOnly) {
                bucketState.setAvailableTokens(remainingTokens);
            }
            builder.allowed(true)
                    .limit((long) Math.floor(bucketConfig.getCapacity()))
                    .remaining((long) Math.floor(remainingTokens))
                    .resetAtMillis(calculateResetTimeFromTokens(bucketConfig, remainingTokens, nowMillis))
                    .retryAfterMillis(0);
            return builder.build();
        }

        double tokensNeeded = tokens - availableTokens;
        long retryAfterMillis = (long) Math.ceil(tokensNeeded * bucketConfig.getRefillIntervalMillis() / bucketConfig.getRefillTokens());

        builder.allowed(false)
                .limit((long) Math.floor(bucketConfig.getCapacity()))
                .remaining((long) Math.floor(availableTokens))
                .retryAfterMillis(retryAfterMillis)
                .resetAtMillis(nowMillis + retryAfterMillis);
        return builder.build();
    }

    /**
     * Computes what the available tokens would be after refill, without mutating state.
     */
    private double computeRefilledTokens(TokenBucketConfig config, TokenBucketState state, long nowNanos) {
        double capacity = config.getCapacity();
        double refillTokens = config.getRefillTokens();
        double refillIntervalNanos = config.getRefillIntervalMillis() * 1_000_000;

        if (refillTokens <= 0 || refillIntervalNanos <= 0) {
            return state.getAvailableTokens();
        }

        double availableTokens = state.getAvailableTokens();
        long lastRefillNanos = state.getLastRefillNanos();

        long elapsedNanos = nowNanos - lastRefillNanos;
        if (elapsedNanos > 0) {
            double tokensToAdd = (refillTokens * elapsedNanos) / refillIntervalNanos;
            if (tokensToAdd > 0) {
                availableTokens = Math.min(capacity, availableTokens + tokensToAdd);
            }
        }
        return availableTokens;
    }

    private long calculateResetTimeFromTokens(TokenBucketConfig config, double remainingTokens, long nowMillis) {
        double tokensToFill = config.getCapacity() - remainingTokens;
        if (tokensToFill <= 0) return nowMillis;
        long retryAfterMillis = (long) Math.ceil(tokensToFill * config.getRefillIntervalMillis() / config.getRefillTokens());
        return nowMillis + retryAfterMillis;
    }

    private void refill(TokenBucketConfig config, TokenBucketState state, long nowNanos) {
        double capacity = config.getCapacity();
        double refillTokens = config.getRefillTokens();
        double refillIntervalNanos = config.getRefillIntervalMillis() * 1_000_000;

        if (refillTokens <= 0 || refillIntervalNanos <= 0) {
            return;
        }

        double availableTokens = state.getAvailableTokens();
        long lastRefillNanos = state.getLastRefillNanos();

        long elapsedNanos = nowNanos - lastRefillNanos;
        if (elapsedNanos > 0) {
            double tokensToAdd = (refillTokens * elapsedNanos) / refillIntervalNanos;
            if (tokensToAdd > 0) {
                availableTokens = Math.min(capacity, availableTokens + tokensToAdd);

                // Advance time only for the portion that produced tokens
                double elapsedUsedNanos = (tokensToAdd * refillIntervalNanos) / refillTokens;
                lastRefillNanos = Math.min(nowNanos, (long) (lastRefillNanos + elapsedUsedNanos));

                state.setLastRefillNanos(lastRefillNanos);
                state.setAvailableTokens(availableTokens);
            }
        }
    }
}
