package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.state.FixedWindowCounterState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FixedWindowCounterAlgorithmTest {

    @Test
    void enforcesFixedWindowCapacity() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(3, 1, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult r4 = algorithm.tryConsume(key, config, state, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertTrue(r3.isAllowed());
        assertFalse(r4.isAllowed());
        assertEquals(0, r3.getRemaining());
        assertTrue(r4.getRetryAfterMillis() >= 0);
    }

    @Test
    void newWindowAllowsAfterWait() throws InterruptedException {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(1, 5, TimeUnit.MILLISECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult first = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult second = algorithm.tryConsume(key, config, state, 1);
        long waitMillis = Math.max(1L, second.getRetryAfterMillis() + 1);
        Thread.sleep(waitMillis);
        RateLimitResult third = algorithm.tryConsume(key, config, state, 1);

        assertTrue(first.isAllowed());
        assertFalse(second.isAllowed());
        assertTrue(third.isAllowed());
    }

    @Test
    void remainingIsNeverNegativeOnDeny() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(2, 1, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        algorithm.tryConsume(key, config, state, 2);
        RateLimitResult denied = algorithm.tryConsume(key, config, state, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRemaining() >= 0);
    }
}
