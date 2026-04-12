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

    private record Snapshot(boolean allowed, double newAvailableTokens, long newLastRefillNanos, RateLimitResult result) {}
    private record RefillResult(double availableTokens, long lastRefillNanos) {}

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time) {
        return tryConsume(key, config, state, time, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long tokens) {
        Snapshot snapshot = computeLimit(key, config, state, time, tokens);
        if (snapshot.allowed()) {
            TokenBucketState bucketState = (TokenBucketState) state;
            bucketState.setAvailableTokens(snapshot.newAvailableTokens());
            bucketState.setLastRefillNanos(snapshot.newLastRefillNanos());
        }
        return snapshot.result();
    }

    @Override
    public RateLimitResult checkLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long tokens) {
        return computeLimit(key, config, state, time, tokens).result();
    }

    private Snapshot computeLimit(RateLimitKey key, RateLimitConfig config, RateLimitState state, RequestTime time, long tokens) {
        Objects.requireNonNull(key, "RateLimitKey cannot be null");
        Objects.requireNonNull(config, "RateLimitConfig cannot be null");

        if (tokens <= 0) {
            throw new InvalidRateLimitCostException("Cost must be positive");
        }
        TokenBucketConfig bucketConfig = (TokenBucketConfig) config;
        TokenBucketState bucketState = (TokenBucketState) state;

        long nowNanos = time.nanoTime();
        long nowMillis = time.currentTimeMillis();

        RefillResult refillResult = computeRefill(bucketConfig, bucketState, nowNanos);
        double availableTokens = refillResult.availableTokens();

        RateLimitResult.Builder builder = RateLimitResult.builder();

        if (availableTokens >= tokens) {
            double remainingTokens = availableTokens - tokens;

            builder.allowed(true)
                    .limit((long) Math.floor(bucketConfig.getBucketCapacity()))
                    .remaining((long) Math.floor(remainingTokens))
                    .resetAtMillis(calculateResetTimeFromTokens(bucketConfig, remainingTokens, nowMillis))
                    .retryAfterMillis(0);
            return new Snapshot(true, remainingTokens, refillResult.lastRefillNanos(), builder.build());
        }

        double tokensNeeded = tokens - availableTokens;
        long retryAfterMillis = (long) Math.ceil(tokensNeeded * bucketConfig.getRefillIntervalMillis() / bucketConfig.getRefillTokens());

        builder.allowed(false)
                .limit((long) Math.floor(bucketConfig.getBucketCapacity()))
                .remaining((long) Math.floor(availableTokens))
                .retryAfterMillis(retryAfterMillis)
                .resetAtMillis(nowMillis + retryAfterMillis);
        return new Snapshot(false, availableTokens, bucketState.getLastRefillNanos(), builder.build());
    }

    private RefillResult computeRefill(TokenBucketConfig config, TokenBucketState state, long nowNanos) {
        double capacity = config.getBucketCapacity();
        double refillTokens = config.getRefillTokens();
        double refillIntervalNanos = config.getRefillIntervalMillis() * 1_000_000;

        double availableTokens = state.getAvailableTokens();
        long lastRefillNanos = state.getLastRefillNanos();

        if (refillTokens <= 0 || refillIntervalNanos <= 0) {
            return new RefillResult(availableTokens, lastRefillNanos);
        }

        long elapsedNanos = nowNanos - lastRefillNanos;
        if (elapsedNanos > 0) {
            double tokensToAdd = (refillTokens * elapsedNanos) / refillIntervalNanos;
            if (tokensToAdd > 0) {
                availableTokens = Math.min(capacity, availableTokens + tokensToAdd);

                // Advance time only for the portion that produced tokens
                double elapsedUsedNanos = (tokensToAdd * refillIntervalNanos) / refillTokens;
                lastRefillNanos = Math.min(nowNanos, (long) (lastRefillNanos + elapsedUsedNanos));
            }
        }
        return new RefillResult(availableTokens, lastRefillNanos);
    }

    private long calculateResetTimeFromTokens(TokenBucketConfig config, double remainingTokens, long nowMillis) {
        double tokensToFill = config.getBucketCapacity() - remainingTokens;
        if (tokensToFill <= 0) return nowMillis;
        long retryAfterMillis = (long) Math.ceil(tokensToFill * config.getRefillIntervalMillis() / config.getRefillTokens());
        return nowMillis + retryAfterMillis;
    }
}
