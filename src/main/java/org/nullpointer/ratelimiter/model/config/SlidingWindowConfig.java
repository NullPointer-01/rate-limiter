package org.nullpointer.ratelimiter.model.config;

import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.algorithms.SlidingWindowAlgorithm;
import org.nullpointer.ratelimiter.algorithms.factory.AlgorithmFactory;
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

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public long getMaxCost() {
        return maxCost;
    }

    @Override
    public RateLimitingAlgorithm getAlgorithm() {
        return AlgorithmFactory.getAlgorithmByType(AlgorithmType.SLIDING_WINDOW);
    }

    @Override
    public RateLimitState initialRateLimitState() {
        return new SlidingWindowState();
    }
}
