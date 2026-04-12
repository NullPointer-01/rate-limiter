package org.nullpointer.ratelimiter.model.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.factory.AlgorithmFactory;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowCounterState;

import java.util.concurrent.TimeUnit;

public class SlidingWindowCounterConfig implements RateLimitConfig {
    private final long capacity; // requests per window
    private final long windowSizeMillis;

    public SlidingWindowCounterConfig(long capacity, long windowSize, TimeUnit timeUnit) {
        this.capacity = capacity;
        this.windowSizeMillis = timeUnit.toMillis(windowSize);
    }

    @JsonCreator
    protected SlidingWindowCounterConfig(
            @JsonProperty("capacity") long capacity,
            @JsonProperty("windowSizeMillis") long windowSizeMillis) {
        this.capacity = capacity;
        this.windowSizeMillis = windowSizeMillis;
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    @Override
    @JsonIgnore
    public RateLimitingAlgorithm getAlgorithm() {
        return AlgorithmFactory.getAlgorithmByType(AlgorithmType.SLIDING_WINDOW_COUNTER);
    }

    @Override
    @JsonIgnore
    public RateLimitState initialRateLimitState(long nanoTime) {
        return new SlidingWindowCounterState(nanoTime);
    }

}
