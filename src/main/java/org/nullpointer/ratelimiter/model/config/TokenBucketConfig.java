package org.nullpointer.ratelimiter.model.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.factory.AlgorithmFactory;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;

import java.util.concurrent.TimeUnit;

public class TokenBucketConfig implements RateLimitConfig {
    private final double capacity;
    private final double refillTokens;
    private final double refillIntervalMillis;

    public TokenBucketConfig(long capacity, long refillTokens, long refillInterval, TimeUnit timeUnit) {
        this.capacity = capacity;
        this.refillTokens = refillTokens;
        this.refillIntervalMillis = timeUnit.toMillis(refillInterval);
    }

    @JsonCreator
    protected TokenBucketConfig(
            @JsonProperty("capacity") double capacity,
            @JsonProperty("refillTokens") double refillTokens,
            @JsonProperty("refillIntervalMillis") double refillIntervalMillis) {
        this.capacity = capacity;
        this.refillTokens = refillTokens;
        this.refillIntervalMillis = refillIntervalMillis;
    }

    @JsonProperty("capacity")
    public double getBucketCapacity() {
        return capacity;
    }

    public double getRefillTokens() {
        return refillTokens;
    }

    public double getRefillIntervalMillis() {
        return refillIntervalMillis;
    }

    @Override
    @JsonIgnore
    public RateLimitingAlgorithm getAlgorithm() {
        return AlgorithmFactory.getAlgorithmByType(AlgorithmType.TOKEN_BUCKET);
    }

    @Override
    @JsonIgnore
    public RateLimitState initialRateLimitState(long nanoTime) {
        return new TokenBucketState(capacity, nanoTime);
    }

    @Override
    @JsonIgnore
    public long getCapacity() {
        return (long) capacity;
    }

    @Override
    @JsonIgnore
    public long getWindowSizeMillis() {
        return (long) ((capacity / refillTokens) * refillIntervalMillis);
    }
}
