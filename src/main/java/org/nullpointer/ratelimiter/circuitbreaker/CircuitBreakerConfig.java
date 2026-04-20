package org.nullpointer.ratelimiter.circuitbreaker;

import java.util.concurrent.TimeUnit;

public class CircuitBreakerConfig {
    private final CircuitBreakerMode mode;
    private final int windowSize;
    private final long waitTimeNanos;
    private final double failureRate;
    private final double trialFailureRate;
    private final int permittedHalfOpenCalls;
    private final int minimumCalls;

    private CircuitBreakerConfig(Builder builder) {
        this.mode = builder.mode;
        this.windowSize = builder.windowSize;
        this.waitTimeNanos = builder.timeUnit.toNanos(builder.waitTime);
        this.failureRate = builder.failureRate;
        this.trialFailureRate = builder.trialFailureRate;
        this.permittedHalfOpenCalls = builder.permittedHalfOpenCalls;
        this.minimumCalls = builder.minimumCalls;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isFailOpenMode() {
        return CircuitBreakerMode.FAIL_OPEN.equals(mode);
    }

    public boolean isFailClosedMode() {
        return CircuitBreakerMode.FAIL_CLOSED.equals(mode);
    }

    public CircuitBreakerMode getMode() {
        return mode;
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

    public static final class Builder {
        private CircuitBreakerMode mode;
        private int windowSize;
        private long waitTime;
        private TimeUnit timeUnit;
        private double failureRate;
        private double trialFailureRate;
        private int permittedHalfOpenCalls;
        private int minimumCalls;

        private Builder() {
            this.mode = CircuitBreakerMode.FAIL_OPEN;
            this.windowSize = 10;
            this.waitTime = 30;
            this.timeUnit = TimeUnit.SECONDS;
            this.failureRate = 50.0;
            this.trialFailureRate = 50.0;
            this.permittedHalfOpenCalls = 10;
            this.minimumCalls = 5;
        }

        public Builder mode(CircuitBreakerMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder windowSize(int windowSize) {
            this.windowSize = windowSize;
            return this;
        }

        public Builder waitTime(long waitTime, TimeUnit timeUnit) {
            this.waitTime = waitTime;
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder failureRate(double failureRate) {
            this.failureRate = failureRate;
            return this;
        }

        public Builder trialFailureRate(double trialFailureRate) {
            this.trialFailureRate = trialFailureRate;
            return this;
        }

        public Builder permittedHalfOpenCalls(int permittedHalfOpenCalls) {
            this.permittedHalfOpenCalls = permittedHalfOpenCalls;
            return this;
        }

        public Builder minimumCalls(int minimumCalls) {
            this.minimumCalls = minimumCalls;
            return this;
        }

        public CircuitBreakerConfig build() {
            return new CircuitBreakerConfig(this);
        }
    }
}
