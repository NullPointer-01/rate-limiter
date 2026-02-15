package org.nullpointer.ratelimiter.model.config;

import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.algorithms.factory.AlgorithmFactory;
import org.nullpointer.ratelimiter.model.state.FixedWindowCounterState;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.concurrent.TimeUnit;

public class FixedWindowCounterConfig implements RateLimitConfig {
    private final long capacity; // requests per window
    private final long windowSizeMillis;

    public FixedWindowCounterConfig(long capacity, long windowSize, TimeUnit timeUnit) {
        this.capacity = capacity;
        this.windowSizeMillis = timeUnit.toMillis(windowSize);
    }

    public long getCapacity() {
        return capacity;
    }

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    @Override
    public RateLimitingAlgorithm getAlgorithm() {
        return AlgorithmFactory.getAlgorithmByType(AlgorithmType.FIXED_WINDOW_COUNTER);
    }

    @Override
    public RateLimitState initialRateLimitState() {
        return new FixedWindowCounterState();
    }
}
