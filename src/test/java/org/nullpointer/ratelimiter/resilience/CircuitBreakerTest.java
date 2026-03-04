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
    void failClosedModeReturnsDeniedFallback() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = new CircuitBreakerConfig(CircuitBreakerMode.FAIL_CLOSED, 1, 2, TimeUnit.SECONDS, 0.0, 50.0, 20, 1);
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();

        assertFalse(breaker.allowExecution());
        assertFalse(breaker.getFallbackResult().isAllowed());
    }

    @Test
    void failOpenModeReturnsAllowedFallback() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = new CircuitBreakerConfig(CircuitBreakerMode.FAIL_OPEN, 1, 2, TimeUnit.SECONDS, 0.0, 50.0, 20, 1);
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();

        assertFalse(breaker.allowExecution());
        assertTrue(breaker.getFallbackResult().isAllowed());
    }

    @Test
    void openTransitionsToHalfOpenAfterWaitAndClosesOnSuccess() {
        MutableTimeSource time = new MutableTimeSource();
        CircuitBreakerConfig config = new CircuitBreakerConfig(CircuitBreakerMode.FAIL_CLOSED, 1, 2, TimeUnit.SECONDS, 0.0, 10, 5, 1);
        CircuitBreaker breaker = new CircuitBreaker(time, config);

        breaker.recordError();
        assertFalse(breaker.allowExecution());

        time.advanceNanos(TimeUnit.SECONDS.toNanos(3));
        assertTrue(breaker.allowExecution());

        breaker.recordSuccess();
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
