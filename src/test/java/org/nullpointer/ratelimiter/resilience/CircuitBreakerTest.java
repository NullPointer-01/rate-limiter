package org.nullpointer.ratelimiter.resilience;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerConfig;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerMode;
import org.nullpointer.ratelimiter.utils.TimeSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitBreakerTest {

    @Test
    void closedStateAllowsExecution() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreaker breaker = new CircuitBreaker(time, CircuitBreakerConfig.builder().build());

        assertTrue(breaker.isClosed());
        assertFalse(breaker.isOpen());
        assertFalse(breaker.isHalfOpen());
        assertTrue(breaker.allowExecution());
    }

    @Test
    void failClosedModeReturnsDeniedFallback() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .mode(CircuitBreakerMode.FAIL_CLOSED)
            .windowSize(1)
            .waitTime(2, TimeUnit.SECONDS)
            .failureRate(0.0)
            .trialFailureRate(50.0)
            .permittedHalfOpenCalls(20)
            .minimumCalls(1)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();

        assertTrue(breaker.isOpen());
        assertFalse(breaker.isClosed());
        assertFalse(breaker.allowExecution());
        assertFalse(breaker.getFallbackResult().isAllowed());
    }

    @Test
    void failOpenModeReturnsAllowedFallback() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .mode(CircuitBreakerMode.FAIL_OPEN)
            .windowSize(1)
            .waitTime(2, TimeUnit.SECONDS)
            .failureRate(0.0)
            .trialFailureRate(50.0)
            .permittedHalfOpenCalls(20)
            .minimumCalls(1)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();

        assertTrue(breaker.isOpen());
        assertFalse(breaker.isClosed());
        assertFalse(breaker.allowExecution());
        assertTrue(breaker.getFallbackResult().isAllowed());
    }

    @Test
    void openTransitionsToHalfOpenAfterWaitAndClosesOnSuccess() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .mode(CircuitBreakerMode.FAIL_CLOSED)
            .windowSize(1)
            .waitTime(2, TimeUnit.SECONDS)
            .failureRate(0.0)
            .trialFailureRate(10)
            .permittedHalfOpenCalls(5)
            .minimumCalls(1)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());

        time.advanceNanos(TimeUnit.SECONDS.toNanos(3));
        assertTrue(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
        assertFalse(breaker.isOpen());

        breaker.recordSuccess();
        assertTrue(breaker.isHalfOpen());
        assertTrue(breaker.allowExecution());
    }

    @Test
    void opensOnlyAfterMinimumCallsReached() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .windowSize(10)
            .failureRate(50.0)
            .minimumCalls(3)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        breaker.recordError();

        assertTrue(breaker.isClosed());
        assertTrue(breaker.allowExecution());

        breaker.recordSuccess();
        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());
    }

    @Test
    void opensWhenFailureRateThresholdIsBreached() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .windowSize(4)
            .failureRate(50.0)
            .minimumCalls(4)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        breaker.recordError();
        breaker.recordSuccess();
        breaker.recordSuccess();

        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());
    }

    @Test
    void slidingWindowOverwriteUsesLatestWindowOnly() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .windowSize(3)
            .failureRate(50.0)
            .minimumCalls(3)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        breaker.recordSuccess();
        breaker.recordSuccess();
        assertTrue(breaker.isClosed());
        assertTrue(breaker.allowExecution());

        breaker.recordSuccess();
        assertTrue(breaker.isClosed());
        assertTrue(breaker.allowExecution());

        breaker.recordError();
        assertTrue(breaker.isClosed());
        assertTrue(breaker.allowExecution());
    }

    @Test
    void halfOpenPermittedCallsLimitIsEnforced() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .windowSize(1)
            .waitTime(2, TimeUnit.SECONDS)
            .failureRate(0.0)
            .permittedHalfOpenCalls(2)
            .minimumCalls(1)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());

        time.advanceNanos(TimeUnit.SECONDS.toNanos(3));
        assertTrue(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
        assertTrue(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
        assertFalse(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
    }

    @Test
    void halfOpenFailureRateThresholdReopensBreaker() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .windowSize(1)
            .waitTime(2, TimeUnit.SECONDS)
            .failureRate(0.0)
            .trialFailureRate(50.0)
            .permittedHalfOpenCalls(5)
            .minimumCalls(1)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());

        time.advanceNanos(TimeUnit.SECONDS.toNanos(3));
        assertTrue(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
        breaker.recordError();

        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());
    }

    @Test
    void halfOpenClosesOnlyAfterRequiredSuccessfulTrialCalls() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = CircuitBreakerConfig.builder()
            .mode(CircuitBreakerMode.FAIL_CLOSED)
            .windowSize(1)
            .waitTime(2, TimeUnit.SECONDS)
            .failureRate(0.0)
            .trialFailureRate(100.0)
            .permittedHalfOpenCalls(2)
            .minimumCalls(1)
            .build();
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        assertTrue(breaker.isOpen());
        assertFalse(breaker.allowExecution());

        time.advanceNanos(TimeUnit.SECONDS.toNanos(3));
        assertTrue(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
        breaker.recordSuccess();

        assertTrue(breaker.allowExecution());
        assertTrue(breaker.isHalfOpen());
        breaker.recordSuccess();

        assertTrue(breaker.isClosed());
        assertFalse(breaker.isHalfOpen());
        assertTrue(breaker.allowExecution());
    }

    private static final class MutableTimeSource implements TimeSource {
        private long millis;
        private long nanos;

        @Override
        public long currentTimeMillis() {
            return millis;
        }

        @Override
        public long nanoTime() {
            return nanos;
        }

        void advanceNanos(long deltaNanos) {
            nanos += deltaNanos;
            millis += TimeUnit.NANOSECONDS.toMillis(deltaNanos);
        }
    }
}
