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
        long availableTokens = bucketState.getAvailableTokens();

        if (availableTokens >= tokens) {
            long remainingTokens = availableTokens - tokens;

            bucketState.setAvailableTokens(remainingTokens);
            builder.allowed(true)
                    .limit(bucketConfig.getCapacity())
                    .remaining(remainingTokens)
                    .resetAtMillis(calculateResetTime(bucketConfig, bucketState, now));
            return builder.build();
        }

        long tokensNeeded = tokens - availableTokens;
        long retryAfterMillis = (long) Math.ceil((double) tokensNeeded * bucketConfig.getRefillIntervalMillis() / bucketConfig.getRefillTokens());

        builder.allowed(false)
                .limit(bucketConfig.getCapacity())
                .remaining(availableTokens)
                .retryAfterMillis(retryAfterMillis)
                .resetAtMillis(now + retryAfterMillis);
        return builder.build();
    }

    private void refill(TokenBucketConfig config, TokenBucketState state, long now) {
        long capacity = config.getCapacity();
        long refillTokens = config.getRefillTokens();
        long refillIntervalMillis = config.getRefillIntervalMillis();

        long availableTokens = state.getAvailableTokens();
        long lastRefillTimestamp = state.getLastRefillTimestamp();

        long elapsed = now - lastRefillTimestamp;
        if (elapsed > 0) {
            long tokensToAdd = (refillTokens * elapsed) / refillIntervalMillis;
            availableTokens = Math.min(capacity, availableTokens + tokensToAdd);

            state.setAvailableTokens(availableTokens);
            state.setLastRefillTimestamp(now);
        }
    }

    private long calculateResetTime(TokenBucketConfig config, TokenBucketState state, long now) {
        long tokensToFill = config.getCapacity() - state.getAvailableTokens();
        if (tokensToFill <= 0) return now;

        long retryAfterMillis = (long) Math.ceil((double) tokensToFill * config.getRefillIntervalMillis() / config.getRefillTokens());
        return now + retryAfterMillis;
    }
}
