package org.nullpointer.ratelimiter.circuitbreaker;

import java.util.Arrays;
import java.util.function.Supplier;

public class CircuitBreaker<T> {
    private final Clock clock;
    private final CircuitBreakerConfig config;
    private final Supplier<T> allowedFallback;
    private final Supplier<T> deniedFallback;

    private long lastOpenedTimeNanos;
    private CircuitBreakerState state;

    private final boolean[] window;
    private int idx;

    // CLOSED metrics
    private long success;
    private long errors;

    // HALF_OPEN metrics
    private int trialCalls;
    private int trialSuccess;
    private int trialFailures;

    public CircuitBreaker(Clock clock, CircuitBreakerConfig config,
                          Supplier<T> allowedFallback, Supplier<T> deniedFallback) {
        this.config = config;
        this.state = CircuitBreakerState.CLOSED;
        this.clock = clock;
        this.allowedFallback = allowedFallback;
        this.deniedFallback = deniedFallback;

        this.success = 0;
        this.errors = 0;
        this.trialCalls = 0;

        this.idx = 0;
        this.window = new boolean[config.getWindowSize()];
    }

    /**
     * Determines whether the next call should be permitted.
     */
    public synchronized boolean allowExecution() {
        // Allow calls in CLOSED state
        if (CircuitBreakerState.CLOSED.equals(state)) {
            return true;
        }

        if (CircuitBreakerState.OPEN.equals(state)) {
            // Move to HALF_OPEN after wait time
            if (clock.nanoTime() - lastOpenedTimeNanos >= config.getWaitTimeNanos()) {
                transitionToHalfOpen();
            } else {
                return false;
            }
        }

        if (CircuitBreakerState.HALF_OPEN.equals(state)) {
            if (trialCalls >= config.getPermittedHalfOpenCalls()) return false;

            trialCalls++;
            return true;
        }

        return true;
    }

    /**
     * Records a successful call. Updates the sliding window or half-open trial metrics.
     * Triggers a transition from HALF_OPEN to CLOSED if trial success rate meets the threshold.
     */
    public synchronized void recordSuccess() {
        if (CircuitBreakerState.OPEN.equals(state)) return;

        if (CircuitBreakerState.HALF_OPEN.equals(state)) {
            trialSuccess++;
            if (trialCalls == config.getPermittedHalfOpenCalls() && trialSuccessRate() >= config.getSuccessThreshold()) {
                transitionToClosed();
            }
            return;
        }

        if (success + errors >= window.length) {
            if (window[idx]) success--;
            else errors--;
        }

        window[idx] = true;
        idx = (idx + 1) % window.length;

        success++;
        evaluateFailureRate();
    }

    /**
     * Records a failed call. Updates the sliding window or half-open trial metrics.
     * Triggers a transition from CLOSED to OPEN, or from HALF_OPEN back to OPEN.
     */
    public synchronized void recordError() {
        if (CircuitBreakerState.OPEN.equals(state)) return;

        if (CircuitBreakerState.HALF_OPEN.equals(state)) {
            trialFailures++;
            if (trialFailureRate() >= config.getTrialFailureRate()) {
                transitionToOpen();
            }
            return;
        }

        if (success + errors >= window.length) {
            if (window[idx]) success--;
            else errors--;
        }

        window[idx] = false;
        idx = (idx + 1) % window.length;

        errors++;
        evaluateFailureRate();
    }

    public synchronized boolean isOpen() {
        return CircuitBreakerState.OPEN.equals(state);
    }

    public synchronized boolean isClosed() {
        return CircuitBreakerState.CLOSED.equals(state);
    }

    public synchronized boolean isHalfOpen() {
        return CircuitBreakerState.HALF_OPEN.equals(state);
    }

    public T getFallbackResult() {
        if (config.isFailOpenMode()) {
            return allowedFallback.get();
        }

        return deniedFallback.get();
    }

    private void evaluateFailureRate() {
        long total = success + errors;
        if (total >= config.getMinimumCalls() && failureRate() >= config.getFailureRate()) {
            transitionToOpen();
        }
    }

    private void transitionToOpen() {
        state = CircuitBreakerState.OPEN;
        lastOpenedTimeNanos = clock.nanoTime();
    }

    private void transitionToHalfOpen() {
        state = CircuitBreakerState.HALF_OPEN;
        trialCalls = 0;
        trialSuccess = 0;
        trialFailures = 0;
    }

    private void transitionToClosed() {
        state = CircuitBreakerState.CLOSED;
        resetMetrics();
    }

    private void resetMetrics() {
        success = 0;
        errors = 0;
        idx = 0;
        Arrays.fill(window, false);
    }

    private double failureRate() {
        long total = success + errors;
        if (total <= 0) {
            return 0;
        }

        return 100.0 * errors / total;
    }

    private double trialFailureRate() {
        long total = trialSuccess + trialFailures;
        if (total <= 0) {
            return 0;
        }

        return 100.0 * trialFailures / total;
    }

    private double trialSuccessRate() {
        long total = trialSuccess + trialFailures;
        if (total <= 0) {
            return 0;
        }

        return 100.0 * trialSuccess / total;
    }
}
