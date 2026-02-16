package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;

public class TokenBucketAlgorithm implements RateLimitingAlgorithm {

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return tryConsume(key, config, state, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long tokens) {
        TokenBucketConfig bucketConfig = (TokenBucketConfig) config;
        TokenBucketState bucketState = (TokenBucketState) state;
        long nowNanos = System.nanoTime();
        long nowMillis = System.currentTimeMillis();

        refill(bucketConfig, bucketState, nowNanos);

        RateLimitResult.Builder builder = RateLimitResult.builder();
        double availableTokens = bucketState.getAvailableTokens();

        if (availableTokens >= tokens) {
            double remainingTokens = availableTokens - tokens;

            bucketState.setAvailableTokens(remainingTokens);
            builder.allowed(true)
                    .limit((long) Math.floor(bucketConfig.getCapacity()))
                    .remaining((long) Math.floor(remainingTokens))
                    .resetAtMillis(calculateResetTime(bucketConfig, bucketState, nowMillis))
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

    private long calculateResetTime(TokenBucketConfig config, TokenBucketState state, long now) {
        double tokensToFill = config.getCapacity() - state.getAvailableTokens();
        if (tokensToFill <= 0) return now;

        long retryAfterMillis = (long) Math.ceil(tokensToFill * config.getRefillIntervalMillis() / config.getRefillTokens());
        return now + retryAfterMillis;
    }
}
