package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.state.SlidingWindowState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowAlgorithmTest {

    @Test
    void enforcesSlidingWindowCapacity() {
        SlidingWindowConfig config = new SlidingWindowConfig(3, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult r4 = algorithm.tryConsume(key, config, state, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertTrue(r3.isAllowed());
        assertFalse(r4.isAllowed());
        assertTrue(r4.getRetryAfterMillis() >= 0);
    }

    @Test
    void allowsAfterWindowSlides() throws InterruptedException {
        SlidingWindowConfig config = new SlidingWindowConfig(5, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult first = algorithm.tryConsume(key, config, state, 1);
        Thread.sleep(200);
        RateLimitResult second = algorithm.tryConsume(key, config, state, 4);
        RateLimitResult third = algorithm.tryConsume(key, config, state, 4);

        long waitMillis = Math.max(1L, third.getRetryAfterMillis() + 1);
        System.out.println(waitMillis);
        Thread.sleep(waitMillis);
        RateLimitResult fourth = algorithm.tryConsume(key, config, state, 4);

        assertTrue(first.isAllowed());
        assertTrue(second.isAllowed());
        assertFalse(third.isAllowed());
        assertTrue(fourth.isAllowed());
    }

    @Test
    void remainingIsNeverNegativeOnDeny() {
        SlidingWindowConfig config = new SlidingWindowConfig(2, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        algorithm.tryConsume(key, config, state, 1);
        algorithm.tryConsume(key, config, state, 1);
        RateLimitResult denied = algorithm.tryConsume(key, config, state, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRemaining() >= 0);
    }

    @Test
    void multipleCost() {
        SlidingWindowConfig config = new SlidingWindowConfig(5, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, 2);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, 3);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertFalse(r3.isAllowed());
    }

    @Test
    void differentKeysAreIndependent() {
        SlidingWindowConfig config = new SlidingWindowConfig(1, 1, TimeUnit.SECONDS);
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();

        RateLimitResult r1 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-a").build(), config, new SlidingWindowState(), 1);
        RateLimitResult r2 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-b").build(), config, new SlidingWindowState(), 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
    }
}
