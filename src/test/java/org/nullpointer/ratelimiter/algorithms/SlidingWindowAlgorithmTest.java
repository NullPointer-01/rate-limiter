package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.state.SlidingWindowState;
import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowAlgorithmTest {

    @Test
    void enforcesSlidingWindowCapacity() {
        SlidingWindowConfig config = new SlidingWindowConfig(3, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now, 1);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now, 1);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, now, 1);
        RateLimitResult r4 = algorithm.tryConsume(key, config, state, now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertTrue(r3.isAllowed());
        assertFalse(r4.isAllowed());
        assertTrue(r4.getRetryAfterMillis() >= 0);
    }

    @Test
    void allowsAfterWindowSlides() {
        SlidingWindowConfig config = new SlidingWindowConfig(5, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();
        long startMillis = 10_000;
        RequestTime t1 = new RequestTime(startMillis, startMillis * 1_000_000);

        RateLimitResult first = algorithm.tryConsume(key, config, state, t1, 1);

        long nextMillis = startMillis + 200;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000);

        RateLimitResult second = algorithm.tryConsume(key, config, state, t2, 4);
        RateLimitResult third = algorithm.tryConsume(key, config, state, t2, 4);

        assertTrue(first.isAllowed());
        assertTrue(second.isAllowed());
        assertFalse(third.isAllowed());

        long waitMillis = Math.max(1L, third.getRetryAfterMillis() + 1);
        long finalMillis = nextMillis + waitMillis;
        RequestTime t3 = new RequestTime(finalMillis, finalMillis * 1_000_000);

        RateLimitResult fourth = algorithm.tryConsume(key, config, state, t3, 4);

        assertTrue(fourth.isAllowed());
    }

    @Test
    void remainingIsNeverNegativeOnDeny() {
        SlidingWindowConfig config = new SlidingWindowConfig(2, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        algorithm.tryConsume(key, config, state, now, 1);
        algorithm.tryConsume(key, config, state, now, 1);
        RateLimitResult denied = algorithm.tryConsume(key, config, state, now, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRemaining() >= 0);
    }

    @Test
    void multipleCost() {
        SlidingWindowConfig config = new SlidingWindowConfig(5, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now, 2);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now, 3);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertFalse(r3.isAllowed());
    }

    @Test
    void differentKeysAreIndependent() {
        SlidingWindowConfig config = new SlidingWindowConfig(1, 1, TimeUnit.SECONDS);
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-a").build(), config, new SlidingWindowState(), now, 1);
        RateLimitResult r2 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-b").build(), config, new SlidingWindowState(), now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
    }

    @Test
    void testConcurrency() throws InterruptedException {
        int threadCount = 20;
        int requestsPerThread = 50;
        int capacity = 100; // Total allowed requests

        SlidingWindowConfig config = new SlidingWindowConfig(capacity, 10, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.atomic.AtomicInteger allowedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    if (algorithm.tryConsume(key, config, state, now, 1).isAllowed()) {
                        allowedCount.incrementAndGet();
                    }
                }
            });
        }

        executor.shutdown();
        boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(finished);

        // We expect allowedCount to be exactly capacity, because the total requests (20*50=1000) > capacity (100)
        assertEquals(capacity, allowedCount.get());
    }

    @Test
    void invalidCost() {
        SlidingWindowConfig config = new SlidingWindowConfig(10, 1, TimeUnit.SECONDS);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost-test").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        assertThrows(InvalidRateLimitCostException.class, () ->
                algorithm.tryConsume(key, config, state, now, 0)
        );
        assertThrows(InvalidRateLimitCostException.class, () ->
                algorithm.tryConsume(key, config, state, now, -1)
        );
    }

    @Test
    void canConsumeDoesNotMutateState() {
        SlidingWindowConfig config = new SlidingWindowConfig(5, 1, TimeUnit.MINUTES);
        SlidingWindowState state = new SlidingWindowState();
        SlidingWindowAlgorithm algorithm = new SlidingWindowAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-peek").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult peek1 = algorithm.checkLimit(key, config, state, now, 3);
        assertTrue(peek1.isAllowed());
        assertEquals(2, peek1.getRemaining());

        // State unchanged — canConsume again should return the same result
        RateLimitResult peek2 = algorithm.checkLimit(key, config, state, now, 3);
        assertTrue(peek2.isAllowed());
        assertEquals(2, peek2.getRemaining());

        // Actually consume
        RateLimitResult consume = algorithm.tryConsume(key, config, state, now, 3);
        assertTrue(consume.isAllowed());

        // After real consumption, checking for 3 more should fail
        RateLimitResult peek3 = algorithm.checkLimit(key, config, state, now, 3);
        assertFalse(peek3.isAllowed());
    }
}
