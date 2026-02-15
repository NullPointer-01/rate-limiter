package org.nullpointer.ratelimiter.algorithms;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;

public class TokenBucketAlgorithm implements RateLimitingAlgorithm {
    @Override
    public AlgorithmType getAlgorithmType() {
        return AlgorithmType.TOKEN_BUCKET;
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state) {
        return tryConsume(key, config, state, 1);
    }

    @Override
    public RateLimitResult tryConsume(RateLimitKey key, RateLimitConfig config, RateLimitState state, long tokens) {
        TokenBucketConfig bucketConfig = (TokenBucketConfig) config;
        TokenBucketState bucketState = (TokenBucketState) state;
        long now = System.currentTimeMillis();

        refill(bucketConfig, bucketState, now);

        RateLimitResult.Builder builder = RateLimitResult.builder();
        double availableTokens = bucketState.getAvailableTokens();

        if (availableTokens >= tokens) {
            double remainingTokens = availableTokens - tokens;

            bucketState.setAvailableTokens(remainingTokens);
            builder.allowed(true)
                    .limit((long) Math.floor(bucketConfig.getCapacity()))
                    .remaining((long) Math.floor(remainingTokens))
                    .resetAtMillis(calculateResetTime(bucketConfig, bucketState, now));
            return builder.build();
        }

        double tokensNeeded = tokens - availableTokens;
        long retryAfterMillis = (long) Math.ceil(tokensNeeded * bucketConfig.getRefillIntervalMillis() / bucketConfig.getRefillTokens());

        builder.allowed(false)
                .limit((long) Math.floor(bucketConfig.getCapacity()))
                .remaining((long) Math.floor(availableTokens))
                .retryAfterMillis(retryAfterMillis)
                .resetAtMillis(now + retryAfterMillis);
        return builder.build();
    }

    private void refill(TokenBucketConfig config, TokenBucketState state, long now) {
        double capacity = config.getCapacity();
        double refillTokens = config.getRefillTokens();
        double refillIntervalMillis = config.getRefillIntervalMillis();

        if (refillTokens <= 0 || refillIntervalMillis <= 0) {
            return;
        }

        double availableTokens = state.getAvailableTokens();
        long lastRefillTimestamp = state.getLastRefillTimestamp();

        long elapsed = now - lastRefillTimestamp;
        if (elapsed > 0) {
            double tokensToAdd = (refillTokens * elapsed) / refillIntervalMillis;
            if (tokensToAdd > 0) {
                availableTokens = Math.min(capacity, availableTokens + tokensToAdd);

                // Advance time only for the portion that produced tokens
                double elapsedUsed = (tokensToAdd * refillIntervalMillis) / refillTokens;
                lastRefillTimestamp = Math.min(now, (long) (lastRefillTimestamp + elapsedUsed));

                state.setLastRefillTimestamp(lastRefillTimestamp);
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
