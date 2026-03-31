package org.nullpointer.ratelimiter.model.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.factory.AlgorithmFactory;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.SlidingWindowState;

import java.util.concurrent.TimeUnit;

public class SlidingWindowConfig implements RateLimitConfig {
    private final long maxCost;
    private final long windowSizeMillis;

    public SlidingWindowConfig(long maxCost, long windowSize, TimeUnit timeUnit) {
        this.maxCost = maxCost;
        this.windowSizeMillis = timeUnit.toMillis(windowSize);
    }

    @JsonCreator
    protected SlidingWindowConfig(
            @JsonProperty("maxCost") long maxCost,
            @JsonProperty("windowSizeMillis") long windowSizeMillis) {
        this.maxCost = maxCost;
        this.windowSizeMillis = windowSizeMillis;
    }

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public long getMaxCost() {
        return maxCost;
    }

    @Override
    @JsonIgnore
    public RateLimitingAlgorithm getAlgorithm() {
        return AlgorithmFactory.getAlgorithmByType(AlgorithmType.SLIDING_WINDOW);
    }

    @Override
    @JsonIgnore
    public RateLimitState initialRateLimitState(long nanoTime) {
        return new SlidingWindowState();
    }
}
