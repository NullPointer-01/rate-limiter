package org.nullpointer.ratelimiter.model.config;

import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.algorithms.factory.AlgorithmFactory;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;

import java.util.concurrent.TimeUnit;

public class TokenBucketConfig implements RateLimitConfig {
    private final long capacity;
    private final long refillTokens;
    private final long refillIntervalMillis;

    public TokenBucketConfig(long capacity, long refillTokens, long refillInterval, TimeUnit timeUnit) {
        this.capacity = capacity;
        this.refillTokens = refillTokens;
        this.refillIntervalMillis = timeUnit.toMillis(refillInterval);
    }

    public long getCapacity() {
        return capacity;
    }

    public long getRefillTokens() {
        return refillTokens;
    }

    public long getRefillIntervalMillis() {
        return refillIntervalMillis;
    }

    @Override
    public RateLimitingAlgorithm getAlgorithm() {
        return AlgorithmFactory.getAlgorithmByType(AlgorithmType.TOKEN_BUCKET);
    }

    @Override
    public RateLimitState initialRateLimitState() {
        return new TokenBucketState(capacity, System.currentTimeMillis());
    }
}
