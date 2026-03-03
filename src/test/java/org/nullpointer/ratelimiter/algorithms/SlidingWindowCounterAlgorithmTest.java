package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.state.SlidingWindowCounterState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowCounterAlgorithmTest {

    @Test
    void enforcesSlidingWindowCounterCapacity() {
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(3, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(now.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

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
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(1, 1, TimeUnit.SECONDS);
        long startMillis = 10_000;
        RequestTime t1 = new RequestTime(startMillis, startMillis * 1_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(t1.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult first = algorithm.tryConsume(key, config, state, t1, 1);
        RateLimitResult second = algorithm.tryConsume(key, config, state, t1, 1);

        long waitMillis = Math.max(1L, second.getRetryAfterMillis() + 1);
        long nextMillis = startMillis + waitMillis;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000);

        RateLimitResult third = algorithm.tryConsume(key, config, state, t2, 1);

        assertTrue(first.isAllowed());
        assertFalse(second.isAllowed());
        assertTrue(third.isAllowed());
    }

    @Test
    void remainingIsNeverNegativeOnDeny() {
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(2, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(now.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        algorithm.tryConsume(key, config, state, now, 2);
        RateLimitResult denied = algorithm.tryConsume(key, config, state, now, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRemaining() >= 0);
    }

    @Test
    void multipleCost() {
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(5, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(now.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now, 2);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now, 3);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertFalse(r3.isAllowed());
    }

    @Test
    void differentKeysAreIndependent() {
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(1, 1, TimeUnit.SECONDS);
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-a").build(), config, new SlidingWindowCounterState(now.nanoTime()), now, 1);
        RateLimitResult r2 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-b").build(), config, new SlidingWindowCounterState(now.nanoTime()), now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
    }

    @Test
    void testConcurrency() throws InterruptedException {
        int threadCount = 20;
        int requestsPerThread = 50;
        int capacity = 100; // Total allowed requests

        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(capacity, 10, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(now.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();

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
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(10, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(now.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost-test").build();

        assertThrows(InvalidRateLimitCostException.class, () ->
            algorithm.tryConsume(key, config, state, now, 0)
        );
        assertThrows(InvalidRateLimitCostException.class, () ->
            algorithm.tryConsume(key, config, state, now, -1)
        );
    }

    @Test
    void canConsumeDoesNotMutateState() {
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(5, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        SlidingWindowCounterState state = new SlidingWindowCounterState(now.nanoTime());
        SlidingWindowCounterAlgorithm algorithm = new SlidingWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-peek").build();

        RateLimitResult peek1 = algorithm.checkLimit(key, config, state, now, 3);
        assertTrue(peek1.isAllowed());
        assertEquals(2, peek1.getRemaining());

        // State unchanged — Should return the same result
        RateLimitResult peek2 = algorithm.checkLimit(key, config, state, now, 3);
        assertTrue(peek2.isAllowed());
        assertEquals(2, peek2.getRemaining());

        // Actually consume
        RateLimitResult consume = algorithm.tryConsume(key, config, state, now, 3);
        assertTrue(consume.isAllowed());

        // After real consumption, check for 3 more should fail
        RateLimitResult peek3 = algorithm.checkLimit(key, config, state, now, 3);
        assertFalse(peek3.isAllowed());
    }
}
