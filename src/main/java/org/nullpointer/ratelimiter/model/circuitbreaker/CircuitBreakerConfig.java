package org.nullpointer.ratelimiter.model.circuitbreaker;

import java.util.concurrent.TimeUnit;

public class CircuitBreakerConfig {
    private final CircuitBreakerMode mode;
    private final int windowSize;
    private final long waitTimeNanos;
    private final double failureRate;
    private final double trialFailureRate;
    private final int permittedHalfOpenCalls;
    private final int minimumCalls;

    public CircuitBreakerConfig(CircuitBreakerMode mode, int windowSize, long waitTime, TimeUnit timeUnit, double failureRate, double trialFailureRate, int permittedHalfOpenCalls, int minimumHalfOpenCalls) {
        this.mode = mode;
        this.windowSize = windowSize;
        this.waitTimeNanos = timeUnit.toNanos(waitTime);
        this.failureRate = failureRate;
        this.trialFailureRate = trialFailureRate;
        this.permittedHalfOpenCalls = permittedHalfOpenCalls;
        this.minimumCalls = minimumHalfOpenCalls;
    }

    public boolean isFailOpenMode() {
        return CircuitBreakerMode.FAIL_OPEN.equals(mode);
    }

    public boolean isFailClosedMode() {
        return CircuitBreakerMode.FAIL_CLOSED.equals(mode);
    }

    public int getWindowSize() {
        return windowSize;
    }

    public long getWaitTimeNanos() {
        return waitTimeNanos;
    }

    public double getFailureRate() {
        return failureRate;
    }

    public double getTrialFailureRate() {
        return trialFailureRate;
    }

    public int getPermittedHalfOpenCalls() {
        return permittedHalfOpenCalls;
    }

    public int getMinimumCalls() {
        return minimumCalls;
    }

    public double getSuccessThreshold() {
        return 100.0;
    }
}