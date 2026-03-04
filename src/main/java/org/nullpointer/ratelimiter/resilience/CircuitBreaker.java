package org.nullpointer.ratelimiter.resilience;

import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerConfig;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerState;
import org.nullpointer.ratelimiter.utils.TimeSource;

import java.util.Arrays;

public class CircuitBreaker {
    private final TimeSource time;
    private final CircuitBreakerConfig config;

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

    public CircuitBreaker(TimeSource time, CircuitBreakerConfig config) {
        this.config = config;
        this.state = CircuitBreakerState.CLOSED;
        this.time = time;

        this.success = 0;
        this.errors = 0;
        this.trialCalls = 0;

        this.idx = 0;
        this.window = new boolean[config.getWindowSize()];
    }

    public synchronized boolean allowExecution() {
        // Allow calls in CLOSED state
        if (CircuitBreakerState.CLOSED.equals(state)) {
            return true;
        }

        if (CircuitBreakerState.OPEN.equals(state)) {
            // Move to HALF_OPEN after wait time
            if (time.nanoTime() - lastOpenedTimeNanos >= config.getWaitTimeNanos()) {
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

    public synchronized void recordSuccess() {
        if (CircuitBreakerState.OPEN.equals(state)) return; // Method should not be called in OPEN state

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
        evaluateFailureRate(); // Evaluate on success too
    }

    public synchronized void recordError() {
        if (CircuitBreakerState.OPEN.equals(state)) return; // Method should not be called in OPEN state

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

    private void evaluateFailureRate() {
        long total = success + errors;
        if (total >= config.getMinimumCalls() && failureRate() >= config.getFailureRate()) {
            transitionToOpen();
        }
    }

    public RateLimitResult getFallbackResult() {
        if (config.isFailOpenMode()) {
            return RateLimitResult.builder().allowed(true).build();
        }

        return RateLimitResult.builder().allowed(false).build();
    }

    private void transitionToOpen() {
        state = CircuitBreakerState.OPEN;
        lastOpenedTimeNanos = time.nanoTime();
        // resetMetrics(); Reset is optional
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